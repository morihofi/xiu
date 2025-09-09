use crate::rtp::errors::PackerError;
use crate::rtp::errors::UnPackerError;
use crate::rtp::rtcp::rtcp_header::RtcpHeader;
use crate::rtp::rtcp::RTCP_RR;
use crate::rtp::rtcp::RTCP_SR;
use crate::rtp::utils::OnFrameFn;
use crate::rtp::utils::OnRtpPacketFn;
use crate::rtp::utils::OnRtpPacketFn2;
use crate::rtp::RtpPacket;

use super::rtp::rtp_aac::RtpAacPacker;
use super::rtp::rtp_h264::RtpH264Packer;
use super::rtp::rtp_h265::RtpH265Packer;

use super::rtp::rtp_aac::RtpAacUnPacker;
use super::rtp::rtp_h264::RtpH264UnPacker;
use super::rtp::rtp_h265::RtpH265UnPacker;

use super::rtp::rtcp::rtcp_context::RtcpContext;
use super::rtp::rtcp::rtcp_sr::RtcpSenderReport;
use super::rtp::utils::TPacker;
use super::rtp::utils::TUnPacker;
use super::rtsp_codec::RtspCodecId;
use super::rtsp_codec::RtspCodecInfo;
use crate::rtp::utils::Marshal;
use crate::rtp::utils::Unmarshal;
use byteorder::BigEndian;
use bytes::BytesMut;
use bytesio::bytes_errors::BytesWriteError;
use bytesio::bytes_reader::BytesReader;
use bytesio::bytes_writer::AsyncBytesWriter;
use bytesio::bytesio::TNetIO;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait TRtpFunc {
    fn create_packer(&mut self, writer: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>);
    fn create_unpacker(&mut self);
}

pub struct RtpChannel {
    codec_info: RtspCodecInfo,
    pub rtp_packer: Option<Box<dyn TPacker>>,
    pub rtp_unpacker: Option<Box<dyn TUnPacker>>,
    ssrc: u32,
    init_sequence: u16,
    timestamp: u32,
    mtu: usize,
}

#[derive(Default)]
pub struct RtcpChannel {
    recv_ctx: RtcpContext,
    pub send_ctx: RtcpContext,
    channel_identifier: u8,
}

impl RtpChannel {
    pub fn new(codec_info: RtspCodecInfo) -> Self {
        let ssrc: u32 = rand::thread_rng().gen();
        let mut rtp_channel = RtpChannel {
            codec_info,
            ssrc,
            rtp_packer: None,
            rtp_unpacker: None,
            init_sequence: 0,
            timestamp: 0,
            // Default MTU used for RTP payload sizing when fragmenting
            mtu: 1400,
        };
        rtp_channel.create_unpacker();
        rtp_channel
    }

    //Receive av frame from network -> pack AV frame to RTP packet -> send to stream hub
    pub async fn on_packet(&mut self, reader: &mut BytesReader) -> Result<(), UnPackerError> {
        if let Some(unpacker) = &mut self.rtp_unpacker {
            unpacker.unpack(reader).await?;
        }
        Ok(())
    }

    //Receive av frame from stream hub -> pack -> send out
    pub async fn on_frame(
        &mut self,
        nalus: &mut BytesMut,
        timestamp: u32,
    ) -> Result<(), PackerError> {
        self.timestamp = timestamp;
        if let Some(packer) = &mut self.rtp_packer {
            return packer.pack(nalus, timestamp).await;
        }
        Ok(())
    }

    //Set handler for processing AV frame when unpack a whole AV frame
    //from rtp packets received from network.
    pub fn on_frame_handler(&mut self, f: OnFrameFn) {
        if let Some(unpacker) = &mut self.rtp_unpacker {
            unpacker.on_frame_handler(f);
        }
    }

    //Set handler for processing rtp packet when packed a rtp packet
    pub fn on_packet_handler(&mut self, f: OnRtpPacketFn) {
        if let Some(packer) = &mut self.rtp_packer {
            packer.on_packet_handler(f);
        }
    }

    //Set handler for processing received AV rtp packet from network
    pub fn on_packet_for_rtcp_handler(&mut self, f: OnRtpPacketFn2) {
        if let Some(packer) = &mut self.rtp_packer {
            packer.on_packet_for_rtcp_handler(f);
        }
    }

    pub fn get_ssrc(&self) -> u32 {
        self.ssrc
    }

    pub fn get_sequence_number(&self) -> u16 {
        self.init_sequence
    }

    pub fn get_timestamp(&self) -> u32 {
        self.timestamp
    }

    pub fn set_mtu(&mut self, mtu: usize) {
        self.mtu = mtu;
    }
}

impl TRtpFunc for RtpChannel {
    fn create_unpacker(&mut self) {
        match self.codec_info.codec_id {
            RtspCodecId::H264 => {
                self.rtp_unpacker = Some(Box::new(RtpH264UnPacker::new()));
            }
            RtspCodecId::H265 => {
                self.rtp_unpacker = Some(Box::new(RtpH265UnPacker::new()));
            }
            RtspCodecId::AAC => {
                self.rtp_unpacker = Some(Box::new(RtpAacUnPacker::new()));
            }
            RtspCodecId::G711A => {}
        }
    }
    fn create_packer(&mut self, io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>) {
        match self.codec_info.codec_id {
            RtspCodecId::H264 => {
                self.rtp_packer = Some(Box::new(RtpH264Packer::new(
                    self.codec_info.payload_type,
                    self.ssrc,
                    self.init_sequence,
                    self.mtu,
                    io,
                )));
            }
            RtspCodecId::H265 => {
                self.rtp_packer = Some(Box::new(RtpH265Packer::new(
                    self.codec_info.payload_type,
                    self.ssrc,
                    self.init_sequence,
                    self.mtu,
                    io,
                )));
            }
            RtspCodecId::AAC => {
                self.rtp_packer = Some(Box::new(RtpAacPacker::new(
                    self.codec_info.payload_type,
                    self.ssrc,
                    self.init_sequence,
                    io,
                )));
            }
            RtspCodecId::G711A => {}
        }
    }
}

impl RtcpChannel {
    pub fn set_channel_identifier(&mut self, channel_idendifier: u8) {
        self.channel_identifier = channel_idendifier;
    }

    pub async fn on_rtcp(
        &mut self,
        reader: &mut BytesReader,
        rtcp_io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    ) {
        let mut reader_clone = BytesReader::new(reader.get_remaining_bytes());
        if let Ok(rtcp_header) = RtcpHeader::unmarshal(&mut reader_clone) {
            match rtcp_header.payload_type {
                RTCP_SR => {
                    if let Ok(sr) = RtcpSenderReport::unmarshal(reader) {
                        self.recv_ctx.received_sr(&sr);
                        if let Err(err) = self.send_rr(rtcp_io).await {
                            log::error!("send rr error: {}", err);
                        }
                    }
                }
                RTCP_RR => {}
                _ => {}
            }
        }
    }

    pub fn on_packet(&mut self, packet: RtpPacket) {
        self.recv_ctx.received_rtp(packet);
    }

    pub async fn send_rr(
        &mut self,
        rtcp_io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    ) -> Result<(), BytesWriteError> {
        let rr = self.recv_ctx.generate_rr();

        let net_type = rtcp_io.lock().await.get_net_type();
        if let Ok(msg) = rr.marshal() {
            let mut bytes_writer = AsyncBytesWriter::new(rtcp_io);
            match net_type {
                bytesio::bytesio::NetType::TCP => {
                    bytes_writer.write_u8(crate::rtsp_utils::INTERLEAVED_MAGIC)?;
                    bytes_writer.write_u8(self.channel_identifier)?;
                    bytes_writer.write_u16::<BigEndian>(msg.len() as u16)?;
                }
                bytesio::bytesio::NetType::UDP => {}
            }
            bytes_writer.write(&msg)?;
            bytes_writer.flush().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtp::define::{ANNEXB_NALU_START_CODE, FU_A, FU_END, FU_START};
    use bytes::{Bytes, BytesMut};
    use bytesio::bytesio::{NetType, TNetIO};
    use bytesio::bytesio_errors::BytesIOError;
    use std::sync::Arc;
    use std::time::Duration;

    struct DummyIO;

    #[async_trait::async_trait]
    impl TNetIO for DummyIO {
        async fn write(&mut self, _bytes: Bytes) -> Result<(), BytesIOError> {
            Ok(())
        }

        async fn read(&mut self) -> Result<BytesMut, BytesIOError> {
            Ok(BytesMut::new())
        }

        async fn read_timeout(&mut self, _duration: Duration) -> Result<BytesMut, BytesIOError> {
            Ok(BytesMut::new())
        }

        fn get_net_type(&self) -> NetType {
            NetType::TCP
        }
    }

    #[tokio::test]
    async fn rtp_packer_increments_sequence_and_sets_version() {
        let codec_info = RtspCodecInfo {
            codec_id: RtspCodecId::H264,
            payload_type: 96,
            ..Default::default()
        };
        let mut channel = RtpChannel::new(codec_info);
        let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> = Arc::new(Mutex::new(Box::new(DummyIO)));
        channel.create_packer(io);

        let packets = Arc::new(std::sync::Mutex::new(Vec::new()));
        let packets_clone = packets.clone();

        channel.on_packet_handler(Box::new(move |_io, packet| {
            let packets_inner = packets_clone.clone();
            Box::pin(async move {
                packets_inner.lock().unwrap().push(packet);
                Ok(())
            })
        }));

        let mut frame1 = BytesMut::new();
        frame1.extend_from_slice(&ANNEXB_NALU_START_CODE);
        frame1.extend_from_slice(&[0x65, 0x88, 0x84]);
        channel.on_frame(&mut frame1, 0).await.unwrap();

        let mut frame2 = BytesMut::new();
        frame2.extend_from_slice(&ANNEXB_NALU_START_CODE);
        frame2.extend_from_slice(&[0x65, 0x88, 0x85]);
        channel.on_frame(&mut frame2, 0).await.unwrap();

        let locked = packets.lock().unwrap();
        assert_eq!(locked.len(), 2);
        assert_eq!(locked[0].header.version, 2);
        assert_eq!(locked[1].header.version, 2);
        assert_eq!(locked[0].header.seq_number + 1, locked[1].header.seq_number);
    }

    #[tokio::test]
    async fn multi_nalu_frame_results_in_multiple_packets() {
        let codec_info = RtspCodecInfo {
            codec_id: RtspCodecId::H264,
            payload_type: 96,
            ..Default::default()
        };
        let mut channel = RtpChannel::new(codec_info);
        let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> = Arc::new(Mutex::new(Box::new(DummyIO)));
        channel.create_packer(io);

        let packets = Arc::new(std::sync::Mutex::new(Vec::new()));
        let packets_clone = packets.clone();

        channel.on_packet_handler(Box::new(move |_io, packet| {
            let packets_inner = packets_clone.clone();
            Box::pin(async move {
                packets_inner.lock().unwrap().push(packet);
                Ok(())
            })
        }));

        let mut frame = BytesMut::new();
        frame.extend_from_slice(&ANNEXB_NALU_START_CODE);
        frame.extend_from_slice(&[0x65, 0x88, 0x84]);
        frame.extend_from_slice(&ANNEXB_NALU_START_CODE);
        frame.extend_from_slice(&[0x41, 0x9A, 0x22]);

        channel.on_frame(&mut frame, 0).await.unwrap();

        let locked = packets.lock().unwrap();
        assert_eq!(locked.len(), 2);
    }

    #[tokio::test]
    async fn large_keyframe_is_fragmented() {
        let codec_info = RtspCodecInfo {
            codec_id: RtspCodecId::H264,
            payload_type: 96,
            ..Default::default()
        };
        let mut channel = RtpChannel::new(codec_info);
        let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> = Arc::new(Mutex::new(Box::new(DummyIO)));
        channel.create_packer(io);

        let packets = Arc::new(std::sync::Mutex::new(Vec::new()));
        let packets_clone = packets.clone();

        channel.on_packet_handler(Box::new(move |_io, packet| {
            let packets_inner = packets_clone.clone();
            Box::pin(async move {
                packets_inner.lock().unwrap().push(packet);
                Ok(())
            })
        }));

        let mut frame = BytesMut::new();
        frame.extend_from_slice(&ANNEXB_NALU_START_CODE);
        frame.extend_from_slice(&[0x65]);
        frame.extend_from_slice(&[0x88; 2000]);

        channel.on_frame(&mut frame, 0).await.unwrap();

        let locked = packets.lock().unwrap();
        assert!(locked.len() > 1);
        assert_eq!(locked[0].payload[0] & 0x1F, FU_A);
        assert!(locked[0].payload[1] & FU_START > 0);
        assert!(locked.last().unwrap().payload[1] & FU_END > 0);
    }
}
