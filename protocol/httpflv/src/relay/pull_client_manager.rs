use {
    bytes::BytesMut,
    futures::StreamExt,
    std::{collections::HashMap, sync::{Arc, atomic::{AtomicBool, Ordering}}},
    streamhub::{
        define::{BroadcastEvent, BroadcastEventReceiver, StreamHubEvent, StreamHubEventSender, StreamOp, PublishDesc, PublisherInfo, ProtocolId, FrameData},
        errors::{StreamHubError, StreamHubErrorValue},
        stream::StreamIdentifier,
        utils::{RandomDigitCount, Uuid},
    },
    tokio::sync::Mutex,
    xflv::{define::tag_type, demuxer::{FlvAudioTagDemuxer, FlvVideoTagDemuxer}},
    bytesio::bytes_reader::BytesReader,
    async_trait::async_trait,
};

use reqwest::Client;
use streamhub::define::{DataSender, InformationSender, TStreamHandler};

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

    async fn send_information(&self, _sender: InformationSender) {}
}

pub struct HttpFlvPullClientManager {
    clients: HashMap<String, Arc<AtomicBool>>,
    client_event_consumer: BroadcastEventReceiver,
    channel_event_producer: StreamHubEventSender,
}

impl HttpFlvPullClientManager {
    pub fn new(
        consumer: BroadcastEventReceiver,
        producer: StreamHubEventSender,
    ) -> Self {
        Self {
            clients: HashMap::new(),
            client_event_consumer: consumer,
            channel_event_producer: producer,
        }
    }

    pub async fn run(&mut self) -> Result<(), StreamHubError> {
        loop {
            let val = self.client_event_consumer.recv().await.map_err(|e| StreamHubError{ value: StreamHubErrorValue::OtherError(e.to_string()) })?;

            match val {
                BroadcastEvent::Subscribe { id, identifier, server_address, result_sender } => {
                    let sender = result_sender;
                    let Some(url) = server_address else {
                        if let Some(s) = sender { let _ = s.send(Err(StreamHubError{ value: StreamHubErrorValue::OtherError("missing server_address".into())})).await; }
                        continue;
                    };

                    let Some(key) = identifier.to_key() else {
                        if let Some(s) = sender { let _ = s.send(Err(StreamHubError{ value: StreamHubErrorValue::NoAppOrStreamName})).await; }
                        continue;
                    };

                    if self.clients.get(&id).is_some() {
                        if let Some(s) = sender { let _ = s.send(Err(StreamHubError{ value: StreamHubErrorValue::Exists})).await; }
                        continue;
                    }

                    let running = Arc::new(AtomicBool::new(true));
                    self.clients.insert(id.clone(), running.clone());

                    let producer = self.channel_event_producer.clone();
                    tokio::spawn(async move {
                        let client = Client::new();
                        let res = client.get(&url).send().await;
                        if res.is_err() { return; }
                        let mut stream = res.unwrap().bytes_stream();

                        // Publish to hub
                        let session_id = Uuid::new(RandomDigitCount::Four);
                        let (event_result_sender, event_result_receiver) = tokio::sync::oneshot::channel();
                        let publish_event = StreamHubEvent::Publish {
                            identifier: StreamIdentifier::Rtmp { app_name: key.app_name.clone(), stream_name: key.stream_name.clone() },
                            info: PublisherInfo {
                                id: session_id,
                                desc: PublishDesc { op: StreamOp::Relay, from: ProtocolId::HttpFlv, to: Some(ProtocolId::Rtmp) },
                                pub_data_type: streamhub::define::PubDataType::Frame,
                                notify_info: streamhub::define::NotifyInfo { request_url: url.clone(), remote_addr: String::new() },
                            },
                            stream_handler: std::sync::Arc::new(SimpleHandler::default()),
                            result_sender: event_result_sender,
                        };
                        if producer.send(publish_event).is_err() { return; }
                        let Ok(result) = event_result_receiver.await else { return; };
                        let Ok((frame_sender_opt, _, _)) = result else { return; };
                        let Some(frame_sender) = frame_sender_opt else { return; };

                        let mut reader = BytesReader::new(BytesMut::new());
                        let mut header_read = false;
                        let mut video_demux = FlvVideoTagDemuxer::new();
                        let mut audio_demux = FlvAudioTagDemuxer::new();

                        // read loop
                        while running.load(Ordering::Acquire) {
                            if let Some(Ok(chunk)) = stream.next().await {
                                reader.extend_from_slice(chunk.as_ref());
                            } else {
                                break;
                            }

                            if !header_read && reader.len() >= 9 {
                                let _ = reader.read_bytes(9); // FLV header
                                header_read = true;
                            }

                            loop {
                                // need at least 15 bytes for header
                                if reader.len() < 15 { break; }
                                if reader.advance_bytes_cursor(4).is_err() { break; }
                                let Ok(tag_type_val) = reader.advance_u8() else { break; };
                                let Ok(ds3) = reader.advance_u24::<byteorder::BigEndian>() else { break; };
                                let Ok(ts3) = reader.advance_u24::<byteorder::BigEndian>() else { break; };
                                let Ok(ts_ext) = reader.advance_u8() else { break; };
                                if reader.advance_u24::<byteorder::BigEndian>().is_err() { break; }
                                let need = 15 + ds3 as usize;
                                if reader.len() < need { break; }

                                let _ = reader.read_u32::<byteorder::BigEndian>();
                                let _ = reader.read_u8();
                                let data_size = reader.read_u24::<byteorder::BigEndian>().unwrap_or(0) as usize;
                                let timestamp = reader.read_u24::<byteorder::BigEndian>().unwrap_or(0);
                                let timestamp_ext = reader.read_u8().unwrap_or(0);
                                let _ = reader.read_u24::<byteorder::BigEndian>();
                                let dts: u32 = (timestamp & 0x00FF_FFFF) | ((timestamp_ext as u32) << 24);
                                let payload = match reader.read_bytes(data_size) { Ok(b) => b, Err(_) => break };

                                match tag_type_val {
                                    tag_type::VIDEO => {
                                        if let Ok(Some(v)) = video_demux.demux(dts, payload) {
                                            let _ = frame_sender.send(FrameData::Video { timestamp: v.dts as u32, data: v.data });
                                        }
                                    }
                                    tag_type::AUDIO => {
                                        if let Ok(a) = audio_demux.demux(dts, payload) {
                                            if a.has_data { let _ = frame_sender.send(FrameData::Audio { timestamp: a.dts as u32, data: a.data }); }
                                        }
                                    }
                                    tag_type::SCRIPT_DATA_AMF => {
                                        let _ = frame_sender.send(FrameData::MetaData { timestamp: dts, data: payload });
                                    }
                                    _ => {}
                                }
                            }
                        }

                        // unpublish
                        let _ = producer.send(StreamHubEvent::UnPublish {
                            identifier: StreamIdentifier::Rtmp { app_name: key.app_name, stream_name: key.stream_name },
                            info: PublisherInfo { id: session_id, desc: PublishDesc { op: StreamOp::Relay, from: ProtocolId::HttpFlv, to: Some(ProtocolId::Rtmp) }, pub_data_type: streamhub::define::PubDataType::Frame, notify_info: streamhub::define::NotifyInfo { request_url: url, remote_addr: String::new() } },
                        });
                    });

                    if let Some(s) = sender { let _ = s.send(Ok(())).await; }
                }
                BroadcastEvent::UnSubscribe { id, result_sender } => {
                    if let Some(flag) = self.clients.get(&id) {
                        flag.store(false, Ordering::Release);
                        self.clients.remove(&id);
                        if let Some(s) = result_sender { let _ = s.send(Ok(())).await; }
                    } else if let Some(s) = result_sender {
                        let _ = s.send(Err(StreamHubError{ value: StreamHubErrorValue::NoAppName })).await;
                    }
                }
                _ => {}
            }
        }
    }
}

