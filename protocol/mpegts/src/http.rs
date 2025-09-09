use bytes::BytesMut;
use futures::channel::mpsc::UnboundedSender;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;

use streamhub::define::{
    NotifyInfo, PacketData, PacketDataReceiver, ProtocolId, StatisticData, StatisticDataSender,
    StreamHubEvent, StreamHubEventSender, StreamOp, SubDataType, SubscribeDesc, SubscriberInfo,
};
use streamhub::{
    stream::StreamIdentifier,
    utils::{RandomDigitCount, Uuid},
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use xmpegts::define::{epsi_stream_type, MPEG_FLAG_IDR_FRAME};
use xmpegts::ts::TsMuxer;

pub type HttpResponseDataProducer = UnboundedSender<io::Result<BytesMut>>;

#[derive(Error, Debug)]
pub enum HttpTsError {
    #[error("channel send error")] 
    ChannelSend,
    #[error("stream hub error: {0}")] 
    StreamHub(String),
    #[error("mpegts error: {0}")] 
    MpegTs(String),
}

impl From<streamhub::errors::StreamHubError> for HttpTsError {
    fn from(err: streamhub::errors::StreamHubError) -> Self {
        HttpTsError::StreamHub(err.to_string())
    }
}

impl From<xmpegts::errors::MpegTsError> for HttpTsError {
    fn from(err: xmpegts::errors::MpegTsError) -> Self {
        HttpTsError::MpegTs(err.to_string())
    }
}

pub struct HttpTs {
    app_name: String,
    stream_name: String,

    ts_muxer: TsMuxer,
    video_pid: u16,
    audio_pid: u16,

    event_producer: StreamHubEventSender,
    data_receiver: PacketDataReceiver,
    statistic_data_sender: Option<StatisticDataSender>,
    http_response_data_producer: HttpResponseDataProducer,
    subscriber_id: Uuid,
    request_url: String,
    remote_addr: SocketAddr,
    max_no_data_retries: usize,
}

impl HttpTs {
    pub fn new(
        app_name: String,
        stream_name: String,
        event_producer: StreamHubEventSender,
        http_response_data_producer: HttpResponseDataProducer,
        request_url: String,
        remote_addr: SocketAddr,
        max_no_data_retries: usize,
    ) -> Self {
        let (_, data_receiver) = mpsc::unbounded_channel();
        let subscriber_id = Uuid::new(RandomDigitCount::Four);

        let mut ts_muxer = TsMuxer::new();
        let audio_pid = ts_muxer
            .add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new())
            .unwrap();
        let video_pid = ts_muxer
            .add_stream(epsi_stream_type::PSI_STREAM_H264, BytesMut::new())
            .unwrap();

        Self {
            app_name,
            stream_name,
            ts_muxer,
            video_pid,
            audio_pid,
            data_receiver,
            statistic_data_sender: None,
            event_producer,
            http_response_data_producer,
            subscriber_id,
            request_url,
            remote_addr,
            max_no_data_retries,
        }
    }

    pub async fn run(&mut self) -> Result<(), HttpTsError> {
        self.subscribe_from_stream_hub().await?;
        self.send_media_stream().await?;
        Ok(())
    }

    pub async fn send_media_stream(&mut self) -> Result<(), HttpTsError> {
        let mut retry_count = 0;

        loop {
            if let Some(data) = self.data_receiver.recv().await {
                match data {
                    PacketData::Audio { timestamp, data } => {
                        if let Some(sender) = &self.statistic_data_sender {
                            let _ = sender.send(StatisticData::Audio {
                                uuid: Some(self.subscriber_id),
                                aac_packet_type: 1,
                                data_size: data.len(),
                                duration: 0,
                            });
                        }
                        self.ts_muxer.write(
                            self.audio_pid,
                            (timestamp as i64) * 90,
                            (timestamp as i64) * 90,
                            0,
                            data,
                        )?;
                    }
                    PacketData::Video {
                        timestamp,
                        data,
                        is_keyframe,
                    } => {
                        if let Some(sender) = &self.statistic_data_sender {
                            let _ = sender.send(StatisticData::Video {
                                uuid: Some(self.subscriber_id),
                                frame_count: 1,
                                is_key_frame: Some(is_keyframe),
                                data_size: data.len(),
                                duration: 0,
                            });
                        }
                        let flags = if is_keyframe { MPEG_FLAG_IDR_FRAME } else { 0 };
                        self.ts_muxer.write(
                            self.video_pid,
                            (timestamp as i64) * 90,
                            (timestamp as i64) * 90,
                            flags,
                            data,
                        )?;
                    }
                }

                let out = self.ts_muxer.get_data();
                if !out.is_empty() {
                    self.http_response_data_producer
                        .start_send(Ok(out))
                        .map_err(|_| HttpTsError::ChannelSend)?;
                }
                retry_count = 0;
            } else {
                retry_count += 1;
            }

            if retry_count > self.max_no_data_retries {
                break;
            }
        }

        self.unsubscribe_from_stream_hub().await
    }

    pub async fn unsubscribe_from_stream_hub(&mut self) -> Result<(), HttpTsError> {
        let sub_info = SubscriberInfo {
            id: self.subscriber_id,
            desc: SubscribeDesc {
                op: StreamOp::Remux,
                from: ProtocolId::Rtmp,
                to: Some(ProtocolId::HttpTs),
            },
            sub_data_type: SubDataType::Packet,
            notify_info: NotifyInfo {
                request_url: self.request_url.clone(),
                remote_addr: self.remote_addr.to_string(),
            },
        };

        let identifier = StreamIdentifier::Rtmp {
            app_name: self.app_name.clone(),
            stream_name: self.stream_name.clone(),
        };

        let subscribe_event = StreamHubEvent::UnSubscribe {
            identifier,
            info: sub_info,
        };
        let _ = self.event_producer.send(subscribe_event);
        Ok(())
    }

    pub async fn subscribe_from_stream_hub(&mut self) -> Result<(), HttpTsError> {
        let sub_info = SubscriberInfo {
            id: self.subscriber_id,
            desc: SubscribeDesc {
                op: StreamOp::Remux,
                from: ProtocolId::Rtmp,
                to: Some(ProtocolId::HttpTs),
            },
            sub_data_type: SubDataType::Packet,
            notify_info: NotifyInfo {
                request_url: self.request_url.clone(),
                remote_addr: self.remote_addr.to_string(),
            },
        };

        let identifier = StreamIdentifier::Rtmp {
            app_name: self.app_name.clone(),
            stream_name: self.stream_name.clone(),
        };

        let (event_result_sender, event_result_receiver) = oneshot::channel();

        let subscribe_event = StreamHubEvent::Subscribe {
            identifier,
            info: sub_info,
            result_sender: event_result_sender,
        };

        self.event_producer
            .send(subscribe_event)
            .map_err(|_| HttpTsError::ChannelSend)?;

        let result_receiver = event_result_receiver
            .await
            .map_err(|e| HttpTsError::StreamHub(e.to_string()))??;
        let receiver = result_receiver.0.packet_receiver.unwrap();
        self.data_receiver = receiver;
        self.statistic_data_sender = result_receiver.1;

        if let Some(sender) = &self.statistic_data_sender {
            let _ = sender.send(StatisticData::Subscriber {
                id: self.subscriber_id,
                remote_addr: self.remote_addr.to_string(),
                start_time: chrono::Local::now(),
                desc: SubscribeDesc {
                    op: StreamOp::Remux,
                    from: ProtocolId::Rtmp,
                    to: Some(ProtocolId::HttpTs),
                },
            });
        }

        Ok(())
    }
}
