use axum::{
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    response::Response,
};
use bytes::{Buf, Bytes};
use bytes::BytesMut;
use byteorder::BigEndian;
use futures::StreamExt;
use streamhub::define::{
    DataSender, FrameData, Information, InformationSender, NotifyInfo, ProtocolId, PublishDesc, PublisherInfo,
    StreamHubEvent, StreamHubEventSender, StreamHubError, StreamOp, TStreamHandler,
};
use streamhub::stream::StreamIdentifier;
use tokio::sync::oneshot;
use xflv::define::tag_type;
use xflv::demuxer::{FlvAudioTagDemuxer, FlvVideoTagDemuxer};

use bytesio::bytes_reader::BytesReader;
use commonlib::auth::{Auth, SecretCarrier};
use std::net::SocketAddr;

use async_trait::async_trait;

use streamhub::utils::{RandomDigitCount, Uuid};

#[derive(Default)]
struct SimpleHandler;

#[async_trait]
impl TStreamHandler for SimpleHandler {
    async fn send_prior_data(
        &self,
        _sender: DataSender,
        _desc: PublishDesc,
    ) -> Result<(), StreamHubError> {
        Ok(())
    }

    async fn get_statistic_data(&self) -> Option<streamhub::statistics::StatisticsStream> {
        None
    }

    async fn send_information(&self, _sender: InformationSender) {
        // No SDP or codec priming needed for raw FLV ingest
    }
}

/// Handle an incoming HTTP-FLV publish (POST/PUT) request.
pub async fn handle_publish(
    State((event_producer, auth)): State<(StreamHubEventSender, Option<Auth>)>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    // Parse path like /app/stream.flv
    let path = req.uri().path();
    let query_string: Option<String> = req.uri().query().map(|s| s.to_string());

    match path.find(".flv") {
        Some(index) if index > 0 => {
            let (left, _) = path.split_at(index);
            let rv: Vec<_> = left.split('/').collect();
            if rv.len() < 3 {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("invalid path"))
                    .unwrap();
            }

            let app_name = String::from(rv[1]);
            let stream_name = String::from(rv[2]);

            // Auth as push
            if let Some(auth_val) = auth.clone() {
                if auth_val
                    .authenticate(&stream_name, &query_string.map(SecretCarrier::Query), false)
                    .is_err()
                {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::from("Unauthorized"))
                        .unwrap();
                }
            }

            // Publish to stream hub
            let (event_result_sender, event_result_receiver) = oneshot::channel();
            let remote_addr_str = remote_addr.to_string();
            let session_id = Uuid::new(RandomDigitCount::Four);
            let publish_event = StreamHubEvent::Publish {
                identifier: StreamIdentifier::HttpFlv {
                    app_name: app_name.clone(),
                    stream_name: stream_name.clone(),
                },
                info: PublisherInfo {
                    id: session_id,
                    desc: PublishDesc {
                        op: StreamOp::Push,
                        from: ProtocolId::HttpFlv,
                        to: None,
                    },
                    pub_data_type: streamhub::define::PubDataType::Frame,
                    notify_info: NotifyInfo {
                        request_url: req.uri().to_string(),
                        remote_addr: remote_addr_str,
                    },
                },
                stream_handler: std::sync::Arc::new(SimpleHandler::default()),
                result_sender: event_result_sender,
            };

            if event_producer.send(publish_event).is_err() {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("failed to publish"))
                    .unwrap();
            }

            let result = match event_result_receiver.await {
                Ok(v) => v,
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from("publish canceled"))
                        .unwrap();
                }
            };

            let (frame_sender_opt, _packet_sender_opt, _stat_opt) = match result {
                Ok(v) => v,
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from("publish failed"))
                        .unwrap();
                }
            };

            let Some(frame_sender) = frame_sender_opt else {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("no frame sender"))
                    .unwrap();
            };

            // Start reading FLV stream from body
            let mut body_stream = req.into_body().into_data_stream();
            let mut reader = BytesReader::new(BytesMut::new());
            let mut video_demux = FlvVideoTagDemuxer::new();
            let mut audio_demux = FlvAudioTagDemuxer::new();
            let mut header_read = false;

            // Helper to pull next chunk
            let mut next_chunk = || async {
                match body_stream.next().await {
                    Some(Ok(chunk)) => Some(chunk),
                    _ => None,
                }
            };

            // Consume header: 9 bytes header then continue
            while !header_read {
                if reader.len() < 9 {
                    if let Some(chunk) = next_chunk().await {
                        reader.extend_from_slice(chunk.chunk());
                        continue;
                    } else {
                        // no data
                        break;
                    }
                }
                let _ = reader.read_bytes(9); // ignore result; format validation not strict here
                header_read = true;
            }

            // Main loop: read [prev_size(4), tag header(11), payload]
            'outer: loop {
                // Need at least 15 bytes for header
                while reader.len() < 15 {
                    if let Some(chunk) = next_chunk().await {
                        reader.extend_from_slice(chunk.chunk());
                    } else {
                        break 'outer;
                    }
                }

                // Peek to get data_size
                // previous tag size (peek 4 bytes)
                if reader.advance_bytes_cursor(4).is_err() {
                    break;
                }
                // tag_type
                let tt = match reader.advance_u8() {
                    Ok(v) => v,
                    Err(_) => break,
                };
                // data_size
                let ds = match reader.advance_u24::<BigEndian>() {
                    Ok(v) => v,
                    Err(_) => break,
                } as usize;
                // timestamp (ignore here) and extended and stream id
                if reader.advance_u24::<BigEndian>().is_err()
                    || reader.advance_u8().is_err()
                    || reader.advance_u24::<BigEndian>().is_err()
                {
                    break;
                }

                // Ensure full payload available
                let need = 15 + ds;
                while reader.len() < need {
                    if let Some(chunk) = next_chunk().await {
                        reader.extend_from_slice(chunk.chunk());
                    } else {
                        break 'outer;
                    }
                }

                // Now actually consume
                let _ = reader.read_u32::<BigEndian>();
                let tag_type_val = reader.read_u8().unwrap_or(0);
                let data_size = reader.read_u24::<BigEndian>().unwrap_or(0) as usize;
                let timestamp = reader.read_u24::<BigEndian>().unwrap_or(0);
                let timestamp_ext = reader.read_u8().unwrap_or(0);
                let _ = reader.read_u24::<BigEndian>(); // stream id
                let dts: u32 = (timestamp & 0x00FF_FFFF) | ((timestamp_ext as u32) << 24);
                let payload = match reader.read_bytes(data_size) {
                    Ok(b) => b,
                    Err(_) => break,
                };

                match tag_type_val {
                    tag_type::VIDEO => {
                        if let Ok(Some(v)) = video_demux.demux(dts, payload) {
                            let _ = frame_sender.send(FrameData::Video {
                                timestamp: v.dts as u32,
                                data: v.data,
                            });
                        }
                    }
                    tag_type::AUDIO => {
                        if let Ok(a) = audio_demux.demux(dts, payload) {
                            if a.has_data {
                                let _ = frame_sender.send(FrameData::Audio {
                                    timestamp: a.dts as u32,
                                    data: a.data,
                                });
                            }
                        }
                    }
                    tag_type::SCRIPT_DATA_AMF => {
                        let _ = frame_sender.send(FrameData::MetaData {
                            timestamp: dts,
                            data: payload,
                        });
                    }
                    _ => {}
                }
            }

            // Unpublish when done
            let unpublish_event = StreamHubEvent::UnPublish {
                identifier: StreamIdentifier::HttpFlv { app_name, stream_name },
                info: PublisherInfo {
                    id: session_id,
                    desc: PublishDesc {
                        op: StreamOp::Push,
                        from: ProtocolId::HttpFlv,
                        to: None,
                    },
                    pub_data_type: streamhub::define::PubDataType::Frame,
                    notify_info: NotifyInfo {
                        request_url: String::new(),
                        remote_addr: String::new(),
                    },
                },
            };
            let _ = event_producer.send(unpublish_event);

            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .unwrap()
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not Found"))
            .unwrap(),
    }
}
