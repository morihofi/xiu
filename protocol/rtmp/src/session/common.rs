use streamhub::define::{DataSender, StatisticData, StatisticDataSender};
use tokio::sync::oneshot;

use {
    super::{
        define::SessionType,
        errors::{SessionError, SessionErrorValue},
    },
    crate::{
        cache::errors::CacheError,
        cache::Cache,
        chunk::{
            define::{chunk_type, csid_type},
            packetizer::ChunkPacketizer,
            ChunkInfo,
        },
        messages::define::msg_type_id,
    },
    async_trait::async_trait,
    base64::{engine::general_purpose, Engine as _},
    byteorder::BigEndian,
    bytes::BytesMut,
    bytesio::bytes_reader::BytesReader,
    std::fmt,
    std::{net::SocketAddr, sync::Arc},
    streamhub::{
        define::{
            FrameData, FrameDataReceiver, FrameDataSender, Information, InformationSender,
            NotifyInfo, PacketData, PacketDataSender, PublishType, PublisherInfo,
            StreamHubEvent, StreamHubEventSender, SubscribeType, SubscriberInfo, TStreamHandler,
        },
        errors::{StreamHubError, StreamHubErrorValue},
        statistics::StatisticsStream,
        stream::StreamIdentifier,
        utils::Uuid,
    },
    tokio::sync::{mpsc, Mutex},
    xflv::{
        define,
        flv_tag_header::{AudioTagHeader, VideoTagHeader},
        mpeg4_aac::Mpeg4AacProcessor,
        mpeg4_avc::Mpeg4AvcProcessor,
        Unmarshal,
    },
};

pub struct Common {
    /* Used to mark the subscriber's the data producer
    in channels and delete it from map when unsubscribe
    is called. */
    session_id: Uuid,
    //only Server Subscriber or Client Publisher needs to send out trunck data.
    packetizer: Option<ChunkPacketizer>,

    data_receiver: FrameDataReceiver,
    data_sender: FrameDataSender,
    packet_sender: Option<PacketDataSender>,

    event_producer: StreamHubEventSender,
    pub session_type: SessionType,

    /*save the client side socket connected to the SeverSession */
    remote_addr: Option<SocketAddr>,
    /*request URL from client*/
    pub request_url: String,
    pub stream_handler: Arc<RtmpStreamHandler>,
    /* now used for subscriber session */
    statistic_data_sender: Option<StatisticDataSender>,
}

impl Common {
    pub fn new(
        packetizer: Option<ChunkPacketizer>,
        event_producer: StreamHubEventSender,
        session_type: SessionType,
        remote_addr: Option<SocketAddr>,
    ) -> Self {
        //only used for init,since I don't found a better way to deal with this.
        let (init_producer, init_consumer) = mpsc::unbounded_channel();

        Self {
            session_id: Uuid::new(streamhub::utils::RandomDigitCount::Four),
            packetizer,

            data_sender: init_producer,
            packet_sender: None,
            data_receiver: init_consumer,

            event_producer,
            session_type,
            remote_addr,
            request_url: String::default(),
            stream_handler: Arc::new(RtmpStreamHandler::new()),
            statistic_data_sender: None,
            //cache: None,
        }
    }
    pub async fn send_channel_data(&mut self) -> Result<(), SessionError> {
        let mut retry_times = 0;
        loop {
            if let Some(data) = self.data_receiver.recv().await {
                match data {
                    FrameData::Audio { timestamp, data } => {
                        let data_size = data.len();
                        self.send_audio(data, timestamp).await?;

                        if let Some(sender) = &self.statistic_data_sender {
                            let statistic_audio_data = StatisticData::Audio {
                                uuid: Some(self.session_id),
                                aac_packet_type: 1,
                                data_size,
                                duration: 0,
                            };
                            if let Err(err) = sender.send(statistic_audio_data) {
                                log::error!("send statistic_data err: {}", err);
                            }
                        }
                    }
                    FrameData::Video { timestamp, data } => {
                        let data_size = data.len();
                        self.send_video(data, timestamp).await?;

                        if let Some(sender) = &self.statistic_data_sender {
                            let statistic_video_data = StatisticData::Video {
                                uuid: Some(self.session_id),
                                frame_count: 1,
                                data_size,
                                is_key_frame: None,
                                duration: 0,
                            };
                            if let Err(err) = sender.send(statistic_video_data) {
                                log::error!("send statistic_data err: {}", err);
                            }
                        }
                    }
                    FrameData::MetaData { timestamp, data } => {
                        self.send_metadata(data, timestamp).await?;
                    }
                    _ => {}
                }
            } else {
                retry_times += 1;
                log::debug!(
                    "send_channel_data: no data receives ,retry {} times!",
                    retry_times
                );

                if retry_times > 10 {
                    return Err(SessionError {
                        value: SessionErrorValue::NoMediaDataReceived,
                    });
                }
            }
        }
    }

    pub async fn send_audio(&mut self, data: BytesMut, timestamp: u32) -> Result<(), SessionError> {
        let mut chunk_info = ChunkInfo::new(
            csid_type::AUDIO,
            chunk_type::TYPE_0,
            timestamp,
            data.len() as u32,
            msg_type_id::AUDIO,
            0,
            data,
        );

        if let Some(packetizer) = &mut self.packetizer {
            packetizer.write_chunk(&mut chunk_info).await?;
        }

        Ok(())
    }

    pub async fn send_video(&mut self, data: BytesMut, timestamp: u32) -> Result<(), SessionError> {
        let mut chunk_info = ChunkInfo::new(
            csid_type::VIDEO,
            chunk_type::TYPE_0,
            timestamp,
            data.len() as u32,
            msg_type_id::VIDEO,
            0,
            data,
        );

        if let Some(packetizer) = &mut self.packetizer {
            packetizer.write_chunk(&mut chunk_info).await?;
        }

        Ok(())
    }

    pub async fn send_metadata(
        &mut self,
        data: BytesMut,
        timestamp: u32,
    ) -> Result<(), SessionError> {
        let mut chunk_info = ChunkInfo::new(
            csid_type::DATA_AMF0_AMF3,
            chunk_type::TYPE_0,
            timestamp,
            data.len() as u32,
            msg_type_id::DATA_AMF0,
            0,
            data,
        );

        if let Some(packetizer) = &mut self.packetizer {
            packetizer.write_chunk(&mut chunk_info).await?;
        }

        Ok(())
    }

    pub async fn on_video_data(
        &mut self,
        data: &mut BytesMut,
        timestamp: &u32,
    ) -> Result<(), SessionError> {
        let channel_data = FrameData::Video {
            timestamp: *timestamp,
            data: data.clone(),
        };

        match self.data_sender.send(channel_data) {
            Ok(_) => {}
            Err(err) => {
                log::error!("send video err: {}", err);
                return Err(SessionError {
                    value: SessionErrorValue::SendFrameDataErr,
                });
            }
        }

        if let Some(sender) = &self.packet_sender {
            if let Some(payload) = crate::remuxer::remux_h264(data) {
                let _ = sender.send(PacketData::Video {
                    timestamp: *timestamp,
                    data: payload,
                });
            }
        }

        self.stream_handler.save_video_data(data, *timestamp).await?;

        Ok(())
    }

    pub async fn on_audio_data(
        &mut self,
        data: &mut BytesMut,
        timestamp: &u32,
    ) -> Result<(), SessionError> {
        let channel_data = FrameData::Audio {
            timestamp: *timestamp,
            data: data.clone(),
        };

        match self.data_sender.send(channel_data) {
            Ok(_) => {}
            Err(err) => {
                log::error!("receive audio err {}", err);
                return Err(SessionError {
                    value: SessionErrorValue::SendFrameDataErr,
                });
            }
        }

        if let Some(sender) = &self.packet_sender {
            if let Some(payload) = crate::remuxer::remux_aac(data) {
                let _ = sender.send(PacketData::Audio {
                    timestamp: *timestamp,
                    data: payload,
                });
            }
        }

        self.stream_handler.save_audio_data(data, *timestamp).await?;

        Ok(())
    }

    pub async fn on_meta_data(
        &mut self,
        data: &mut BytesMut,
        timestamp: &u32,
    ) -> Result<(), SessionError> {
        let channel_data = FrameData::MetaData {
            timestamp: *timestamp,
            data: data.clone(),
        };

        match self.data_sender.send(channel_data) {
            Ok(_) => {}
            Err(_) => {
                return Err(SessionError {
                    value: SessionErrorValue::SendFrameDataErr,
                })
            }
        }

        self.stream_handler.save_metadata(data, *timestamp).await;

        Ok(())
    }

    fn get_subscriber_info(&mut self) -> SubscriberInfo {
        let remote_addr = if let Some(addr) = self.remote_addr {
            addr.to_string()
        } else {
            String::from("unknown")
        };

        let sub_type = match self.session_type {
            SessionType::Client => SubscribeType::RtmpRelay,
            SessionType::Server => SubscribeType::RtmpPull,
        };

        SubscriberInfo {
            id: self.session_id,
            /*rtmp local client subscribe from local rtmp session
            and publish(relay) the rtmp steam to remote RTMP server*/
            sub_type,
            sub_data_type: streamhub::define::SubDataType::Frame,
            notify_info: NotifyInfo {
                request_url: self.request_url.clone(),
                remote_addr,
            },
        }
    }

    fn get_publisher_info(&mut self) -> PublisherInfo {
        let remote_addr = if let Some(addr) = self.remote_addr {
            addr.to_string()
        } else {
            String::from("unknown")
        };

        let pub_type = match self.session_type {
            SessionType::Client => PublishType::RtmpRelay,
            SessionType::Server => PublishType::RtmpPush,
        };

        PublisherInfo {
            id: self.session_id,
            pub_type,
            pub_data_type: streamhub::define::PubDataType::Both,
            notify_info: NotifyInfo {
                request_url: self.request_url.clone(),
                remote_addr,
            },
        }
    }

    /* Subscribe from stream hub and push stream data to players or other rtmp nodes */
    pub async fn subscribe_from_stream_hub(
        &mut self,
        app_name: String,
        stream_name: String,
    ) -> Result<(), SessionError> {
        log::info!(
            "subscribe_from_stream_hub, app_name: {} stream_name: {} subscribe_id: {}",
            app_name,
            stream_name,
            self.session_id
        );

        let identifier = StreamIdentifier::Rtmp {
            app_name,
            stream_name,
        };

        let (event_result_sender, event_result_receiver) = oneshot::channel();

        let subscribe_event = StreamHubEvent::Subscribe {
            identifier,
            info: self.get_subscriber_info(),
            result_sender: event_result_sender,
        };
        let rv = self.event_producer.send(subscribe_event);

        if rv.is_err() {
            return Err(SessionError {
                value: SessionErrorValue::StreamHubEventSendErr,
            });
        }

        let result = event_result_receiver.await??;
        self.data_receiver = result.0.frame_receiver.unwrap();

        let statistic_data_sender: Option<StatisticDataSender> = result.1;

        if let Some(sender) = &statistic_data_sender {
            let statistic_subscriber = StatisticData::Subscriber {
                id: self.session_id,
                remote_addr: self.remote_addr.unwrap().to_string(),
                start_time: chrono::Local::now(),
                sub_type: SubscribeType::RtmpPull,
            };
            if let Err(err) = sender.send(statistic_subscriber) {
                log::error!("send statistic_subscriber err: {}", err);
            }
        }

        self.statistic_data_sender = statistic_data_sender;

        Ok(())
    }

    pub async fn unsubscribe_from_stream_hub(
        &mut self,
        app_name: String,
        stream_name: String,
    ) -> Result<(), SessionError> {
        let identifier = StreamIdentifier::Rtmp {
            app_name,
            stream_name,
        };

        let subscribe_event = StreamHubEvent::UnSubscribe {
            identifier,
            info: self.get_subscriber_info(),
        };
        if let Err(err) = self.event_producer.send(subscribe_event) {
            log::error!("unsubscribe_from_stream_hub err {}", err);
        }

        Ok(())
    }

    /* Publish RTMP streams to stream hub, the streams can be pushed from remote or pulled from remote to local */
    pub async fn publish_to_stream_hub(
        &mut self,
        app_name: String,
        stream_name: String,
        gop_num: usize,
    ) -> Result<(), SessionError> {
        let (event_result_sender, event_result_receiver) = oneshot::channel();
        let info = self.get_publisher_info();
        let remote_addr = info.notify_info.remote_addr.clone();

        let publish_event = StreamHubEvent::Publish {
            identifier: StreamIdentifier::Rtmp {
                app_name: app_name.clone(),
                stream_name: stream_name.clone(),
            },
            info,
            stream_handler: self.stream_handler.clone(),
            result_sender: event_result_sender,
        };

        if self.event_producer.send(publish_event).is_err() {
            return Err(SessionError {
                value: SessionErrorValue::StreamHubEventSendErr,
            });
        }

        let result = event_result_receiver.await??;
        self.data_sender = result.0.unwrap();
        self.packet_sender = result.1;

        let statistic_data_sender: Option<StatisticDataSender> = result.2;

        if let Some(sender) = &statistic_data_sender {
            let statistic_publisher = StatisticData::Publisher {
                id: self.session_id,
                remote_addr,
                start_time: chrono::Local::now(),
            };
            if let Err(err) = sender.send(statistic_publisher) {
                log::error!("send statistic_publisher err: {}", err);
            }
        }

        self.stream_handler
            .set_cache(Cache::new(gop_num, statistic_data_sender))
            .await;
        Ok(())
    }

    pub async fn unpublish_to_stream_hub(
        &mut self,
        app_name: String,
        stream_name: String,
    ) -> Result<(), SessionError> {
        log::info!(
            "unpublish_to_stream_hub, app_name:{}, stream_name:{}",
            app_name,
            stream_name
        );
        let unpublish_event = StreamHubEvent::UnPublish {
            identifier: StreamIdentifier::Rtmp {
                app_name: app_name.clone(),
                stream_name: stream_name.clone(),
            },
            info: self.get_publisher_info(),
        };

        match self.event_producer.send(unpublish_event) {
            Err(_) => {
                log::error!(
                    "unpublish_to_stream_hub error.app_name: {}, stream_name: {}",
                    app_name,
                    stream_name
                );
                return Err(SessionError {
                    value: SessionErrorValue::StreamHubEventSendErr,
                });
            }
            _ => {
                log::info!(
                    "unpublish_to_stream_hub successfully.app_name: {}, stream_name: {}",
                    app_name,
                    stream_name
                );
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct RtmpStreamHandler {
    /*cache is used to save RTMP sequence/gops/meta data
    which needs to be send to client(player) */
    /*The cache will be used in different threads(save
    cache in one thread and send cache data to different clients
    in other threads) */
    pub cache: Mutex<Option<Cache>>,
}

impl RtmpStreamHandler {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(None),
        }
    }

    pub async fn set_cache(&self, cache: Cache) {
        *self.cache.lock().await = Some(cache);
    }

    pub async fn save_video_data(
        &self,
        chunk_body: &BytesMut,
        timestamp: u32,
    ) -> Result<(), CacheError> {
        if let Some(cache) = &mut *self.cache.lock().await {
            cache.save_video_data(chunk_body, timestamp).await?;
        }
        Ok(())
    }

    pub async fn save_audio_data(
        &self,
        chunk_body: &BytesMut,
        timestamp: u32,
    ) -> Result<(), CacheError> {
        if let Some(cache) = &mut *self.cache.lock().await {
            cache.save_audio_data(chunk_body, timestamp).await?;
        }
        Ok(())
    }

    pub async fn save_metadata(&self, chunk_body: &BytesMut, timestamp: u32) {
        if let Some(cache) = &mut *self.cache.lock().await {
            cache.save_metadata(chunk_body, timestamp);
        }
    }
}

#[async_trait]
impl TStreamHandler for RtmpStreamHandler {
    async fn send_prior_data(
        &self,
        data_sender: DataSender,
        sub_type: SubscribeType,
    ) -> Result<(), StreamHubError> {
        let cache_lock = self.cache.lock().await;
        let Some(cache) = cache_lock.as_ref() else {
            return Ok(());
        };

        match data_sender {
            DataSender::Frame { sender } => {
                if let Some(meta_body_data) = cache.get_metadata() {
                    log::info!("send_prior_data: meta_body_data: ");
                    sender
                        .send(meta_body_data)
                        .map_err(|_| StreamHubError { value: StreamHubErrorValue::SendError })?;
                }
                if let Some(audio_seq_data) = cache.get_audio_seq() {
                    log::info!("send_prior_data: audio_seq_data: ",);
                    sender
                        .send(audio_seq_data)
                        .map_err(|_| StreamHubError { value: StreamHubErrorValue::SendError })?;
                }
                if let Some(video_seq_data) = cache.get_video_seq() {
                    log::info!("send_prior_data: video_seq_data:");
                    sender
                        .send(video_seq_data)
                        .map_err(|_| StreamHubError { value: StreamHubErrorValue::SendError })?;
                }
                match sub_type {
                    SubscribeType::RtmpPull
                    | SubscribeType::RtspPull
                    | SubscribeType::RtmpRemux2HttpFlv
                    | SubscribeType::RtmpRemux2Hls => {
                        if let Some(gops_data) = cache.get_gops_data() {
                            for gop in gops_data {
                                for channel_data in gop.get_frame_data() {
                                    sender
                                        .send(channel_data)
                                        .map_err(|_| StreamHubError {
                                            value: StreamHubErrorValue::SendError,
                                        })?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            DataSender::Packet { sender } => {
                if let Some(gops_data) = cache.get_gops_data() {
                    for gop in gops_data {
                        for frame in gop.get_frame_data() {
                            match frame {
                                FrameData::Audio { timestamp, data } => {
                                    if let Some(payload) = crate::remuxer::remux_aac(&data) {
                                        let _ = sender.send(PacketData::Audio {
                                            timestamp,
                                            data: payload,
                                        });
                                    }
                                }
                                FrameData::Video { timestamp, data } => {
                                    if let Some(payload) = crate::remuxer::remux_h264(&data) {
                                        let _ = sender.send(PacketData::Video {
                                            timestamp,
                                            data: payload,
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
    async fn get_statistic_data(&self) -> Option<StatisticsStream> {
        //if let Some(cache) = &mut *self.cache.lock().await {
        //    return Some(cache.av_statistics.get_avstatistic_data().await);
        //}

        None
    }

    async fn send_information(&self, sender: InformationSender) {
        let cache_lock = self.cache.lock().await;
        let Some(cache) = cache_lock.as_ref() else {
            return;
        };

        let mut sdp = String::from("v=0\r\n");
        sdp.push_str("o=- 0 0 IN IP4 0.0.0.0\r\n");
        sdp.push_str("s=Stream\r\n");
        sdp.push_str("c=IN IP4 0.0.0.0\r\n");
        sdp.push_str("t=0 0\r\n");

        let mut has_media = false;

        if let Some(FrameData::Video { data, .. }) = cache.get_video_seq() {
            let mut reader = BytesReader::new(data.clone());
            if let Ok(tag_header) = VideoTagHeader::unmarshal(&mut reader) {
                let remain_bytes = reader.extract_remaining_bytes();
                match define::u8_2_avc_codec_id(tag_header.codec_id) {
                    define::AvcCodecId::H264 => {
                        let mut avc_processor = Mpeg4AvcProcessor::default();
                        if avc_processor
                            .decoder_configuration_record_load(&mut BytesReader::new(remain_bytes.clone()))
                            .is_ok()
                        {
                            if let (Some(sps), Some(pps)) = (
                                avc_processor.mpeg4_avc.sps.get(0),
                                avc_processor.mpeg4_avc.pps.get(0),
                            ) {
                                let sps_b64 = general_purpose::STANDARD.encode(&sps.data[..]);
                                let pps_b64 = general_purpose::STANDARD.encode(&pps.data[..]);
                                sdp.push_str("m=video 0 RTP/AVP 96\r\n");
                                sdp.push_str("a=rtpmap:96 H264/90000\r\n");
                                sdp.push_str(&format!(
                                    "a=fmtp:96 packetization-mode=1; sprop-parameter-sets={},{}\r\n",
                                    sps_b64, pps_b64
                                ));
                                sdp.push_str("a=control:streamid=0\r\n");
                                has_media = true;
                            }
                        }
                    }
                    define::AvcCodecId::HEVC => {
                        let mut reader = BytesReader::new(remain_bytes.clone());
                        // hvcc header
                        let _ = reader.read_u8(); // configurationVersion
                        let _ = reader.read_u8();
                        let _ = reader.read_u32::<BigEndian>();
                        let _ = reader.read_u48::<BigEndian>();
                        let _ = reader.read_u8();
                        let _ = reader.read_u16::<BigEndian>();
                        let _ = reader.read_u8();
                        let _ = reader.read_u8();
                        let _ = reader.read_u8();
                        let _ = reader.read_u8();
                        let _ = reader.read_u16::<BigEndian>();
                        let _ = reader.read_u8();
                        let num_arrays = reader.read_u8().unwrap_or(0);

                        let mut vps_b64 = String::new();
                        let mut sps_b64 = String::new();
                        let mut pps_b64 = String::new();

                        for _ in 0..num_arrays {
                            let header = match reader.read_u8() {
                                Ok(h) => h,
                                Err(_) => break,
                            };
                            let nal_type = header & 0x3F;
                            let num_nalus = match reader.read_u16::<BigEndian>() {
                                Ok(n) => n,
                                Err(_) => break,
                            };
                            for _ in 0..num_nalus {
                                let nal_size = match reader.read_u16::<BigEndian>() {
                                    Ok(s) => s as usize,
                                    Err(_) => break,
                                };
                                let nal_unit = match reader.read_bytes(nal_size) {
                                    Ok(n) => n,
                                    Err(_) => break,
                                };
                                let b64 = general_purpose::STANDARD.encode(&nal_unit[..]);
                                match nal_type {
                                    32 => if vps_b64.is_empty() { vps_b64 = b64; },
                                    33 => if sps_b64.is_empty() { sps_b64 = b64; },
                                    34 => if pps_b64.is_empty() { pps_b64 = b64; },
                                    _ => {}
                                }
                            }
                        }

                        if !sps_b64.is_empty() && !pps_b64.is_empty() {
                            sdp.push_str("m=video 0 RTP/AVP 96\r\n");
                            sdp.push_str("a=rtpmap:96 H265/90000\r\n");
                            sdp.push_str(&format!(
                                "a=fmtp:96 sprop-vps={}; sprop-sps={}; sprop-pps={}\r\n",
                                vps_b64, sps_b64, pps_b64
                            ));
                            sdp.push_str("a=control:streamid=0\r\n");
                            has_media = true;
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(FrameData::Audio { data, .. }) = cache.get_audio_seq() {
            let mut reader = BytesReader::new(data.clone());
            if let Ok(tag_header) = AudioTagHeader::unmarshal(&mut reader) {
                let remain_bytes = reader.extract_remaining_bytes();
                if tag_header.sound_format == define::SoundFormat::AAC as u8
                    && tag_header.aac_packet_type == define::aac_packet_type::AAC_SEQHDR
                {
                    let mut aac_processor = Mpeg4AacProcessor::default();
                    if aac_processor
                        .extend_data(remain_bytes.clone())
                        .audio_specific_config_load()
                        .is_ok()
                    {
                        let sample_rate = aac_processor.mpeg4_aac.sampling_frequency;
                        let channels = aac_processor.mpeg4_aac.channels;
                        let asc_hex = hex::encode(&remain_bytes[..]);
                        sdp.push_str("m=audio 0 RTP/AVP 97\r\n");
                        sdp.push_str(&format!(
                            "a=rtpmap:97 MPEG4-GENERIC/{}/{}\r\n",
                            sample_rate, channels
                        ));
                        sdp.push_str(&format!(
                            "a=fmtp:97 profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3; config={}\r\n",
                            asc_hex
                        ));
                        sdp.push_str("a=control:streamid=1\r\n");
                        has_media = true;
                    }
                }
            }
        }

        if has_media {
            if let Err(err) = sender.send(Information::Sdp { data: sdp }) {
                log::error!("send_information of rtmp error: {}", err);
            }
        }
    }
}

impl fmt::Debug for Common {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "S2 {{ member: {:?} }}", self.request_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_send_prior_packet_data() {
        let handler = RtmpStreamHandler::new();
        handler.set_cache(Cache::new(1, None)).await;
        {
            let mut cache_lock = handler.cache.lock().await;
            let cache = cache_lock.as_mut().unwrap();
            let audio = BytesMut::from(&[0xAF, 0x01, 0x11, 0x22][..]);
            cache.save_audio_data(&audio, 0).await.unwrap();
            let mut video = BytesMut::new();
            video.extend_from_slice(&[0x17, 0x01, 0x00, 0x00, 0x00]);
            video.extend_from_slice(&[0x00, 0x00, 0x00, 0x04]);
            video.extend_from_slice(&[0x65, 0x88, 0x84, 0x21]);
            cache.save_video_data(&video, 0).await.unwrap();
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        handler
            .send_prior_data(DataSender::Packet { sender: tx }, SubscribeType::RtspPull)
            .await
            .unwrap();

        let mut received = 0;
        while let Ok(pkt) = rx.try_recv() {
            match pkt {
                PacketData::Audio { .. } | PacketData::Video { .. } => received += 1,
            }
        }

        assert!(received > 0);
    }
}
