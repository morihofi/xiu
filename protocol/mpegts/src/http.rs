use bytes::BytesMut;
use futures::channel::mpsc::UnboundedSender;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;

use streamhub::define::{
    Information, InformationSender, NotifyInfo, PacketData, PacketDataReceiver, ProtocolId,
    StatisticData, StatisticDataSender, StreamHubEvent, StreamHubEventSender, StreamOp,
    SubDataType, SubscribeDesc, SubscriberInfo,
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

    // AAC codec parameters from AudioSpecificConfig
    aac_profile: Option<u8>,
    aac_sf_index: Option<u8>,
    aac_channel_config: Option<u8>,
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
            aac_profile: None,
            aac_sf_index: None,
            aac_channel_config: None,
        }
    }

    pub async fn run(&mut self) -> Result<(), HttpTsError> {
        self.subscribe_from_stream_hub().await?;
        self.fetch_codec_config().await.ok();
        self.send_media_stream().await?;
        Ok(())
    }

    pub async fn send_media_stream(&mut self) -> Result<(), HttpTsError> {
        let mut retry_count = 0;

        loop {
            if let Some(data) = self.data_receiver.recv().await {
                match data {
                    PacketData::Audio { timestamp, data } => {
                        let payload = if let (Some(profile), Some(sf_idx), Some(chan_cfg)) =
                            (self.aac_profile, self.aac_sf_index, self.aac_channel_config)
                        {
                            let mut adts = build_adts_header(profile, sf_idx, chan_cfg, data.len());
                            adts.extend_from_slice(&data);
                            adts
                        } else {
                            // Fallback: pass-through raw (may decode poorly)
                            data
                        };
                        if let Some(sender) = &self.statistic_data_sender {
                            let _ = sender.send(StatisticData::Audio {
                                uuid: Some(self.subscriber_id),
                                aac_packet_type: 1,
                                data_size: payload.len(),
                                duration: 0,
                            });
                        }
                        self.ts_muxer.write(
                            self.audio_pid,
                            (timestamp as i64) * 90,
                            (timestamp as i64) * 90,
                            0,
                            payload,
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
                    if let Err(e) = self.http_response_data_producer.start_send(Ok(out)) {
                        if e.is_disconnected() {
                            log::info!("TS client disconnected; stopping and unsubscribing");
                            break;
                        } else {
                            log::error!("send TS chunk error: {}", e);
                            retry_count += 1;
                            continue;
                        }
                    }
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

    async fn fetch_codec_config(&mut self) -> Result<(), HttpTsError> {
        // Ask publisher for codec info (ASC for AAC)
        let (info_tx, mut info_rx) = tokio::sync::mpsc::unbounded_channel::<Information>();
        let identifier = StreamIdentifier::Rtmp {
            app_name: self.app_name.clone(),
            stream_name: self.stream_name.clone(),
        };
        self.event_producer
            .send(StreamHubEvent::Request {
                identifier,
                sender: info_tx,
            })
            .map_err(|_| HttpTsError::ChannelSend)?;

        // Best-effort single receive
        if let Some(info) = info_rx.recv().await {
            if let Information::CodecConfig { video: _, audio } = info {
                if let Some(a) = audio {
                    if a.codec as u8 == 10 /* AAC */ {
                        if let Some(asc) = a.asc {
                            if let Some((profile, sf_idx, chan_cfg)) = parse_aac_asc(&asc[..]) {
                                self.aac_profile = Some(profile);
                                self.aac_sf_index = Some(sf_idx);
                                self.aac_channel_config = Some(chan_cfg);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// Parse AudioSpecificConfig (MPEG-4 AAC) to get profile, sample rate index, channel config
fn parse_aac_asc(asc: &[u8]) -> Option<(u8, u8, u8)> {
    if asc.len() < 2 {
        return None;
    }
    let b0 = asc[0];
    let b1 = asc[1];
    let audio_object_type = (b0 >> 3) & 0x1F; // 5 bits
    let sampling_frequency_index = ((b0 & 0x07) << 1) | ((b1 >> 7) & 0x01); // 4 bits
    let channel_configuration = (b1 >> 3) & 0x0F; // 4 bits
    // ADTS uses profile = audioObjectType - 1
    let profile_adts = if audio_object_type >= 1 {
        (audio_object_type - 1) & 0x03
    } else {
        0
    };
    Some((profile_adts, sampling_frequency_index, channel_configuration as u8))
}

// Build a 7-byte ADTS header for an AAC raw frame
fn build_adts_header(profile: u8, sf_idx: u8, chan_cfg: u8, payload_len: usize) -> BytesMut {
    let adts_len = 7 + payload_len;
    let mut hdr = BytesMut::with_capacity(7);
    hdr.extend_from_slice(&[
        0xFF, // syncword 0xFFF
        0xF1, // 1111 0001: sync continue, MPEG-4, layer 00, protection_absent=1
        // profile(2), sampling_freq_idx(4), private_bit(1), channel_conf (high 1 bit)
        ((profile & 0x03) << 6) | ((sf_idx & 0x0F) << 2) | ((chan_cfg >> 2) & 0x01),
        // channel_conf (low 2 bits), originality, home, copyright bits, frame length high 2 bits
        ((chan_cfg & 0x03) << 6) | (((adts_len >> 11) & 0x03) as u8),
        // frame length middle 8 bits
        ((adts_len >> 3) & 0xFF) as u8,
        // frame length low 3 bits | fullness high 5 bits
        (((adts_len & 0x07) as u8) << 5) | 0x1F,
        // fullness low 6 bits | number_of_raw_data_blocks_in_frame(2) (0)
        0xFC,
    ]);
    hdr
}
