pub mod define;
pub mod errors;
pub mod rtcp;
pub mod rtp_aac;
pub mod rtp_h264;
pub mod rtp_h265;
pub mod utils;

pub use xrtp::{RtpHeader, RtpPacket};

use bytes::BytesMut;
use bytesio::bytes_errors::{BytesReadError, BytesWriteError};
use bytesio::bytes_reader::BytesReader;

use self::utils::{Marshal, Unmarshal};

// Bridge existing Marshal/Unmarshal traits to the shared xrtp types
impl Unmarshal<&mut BytesReader, Result<RtpHeader, BytesReadError>> for RtpHeader {
    fn unmarshal(reader: &mut BytesReader) -> Result<RtpHeader, BytesReadError> {
        xrtp::RtpHeader::read_from(reader)
    }
}

impl Marshal<Result<BytesMut, BytesWriteError>> for RtpHeader {
    fn marshal(&self) -> Result<BytesMut, BytesWriteError> {
        xrtp::RtpHeader::to_bytes(self)
    }
}

impl Unmarshal<&mut BytesReader, Result<RtpPacket, BytesReadError>> for RtpPacket {
    fn unmarshal(reader: &mut BytesReader) -> Result<RtpPacket, BytesReadError> {
        xrtp::RtpPacket::read_from(reader)
    }
}

impl Marshal<Result<BytesMut, BytesWriteError>> for RtpPacket {
    fn marshal(&self) -> Result<BytesMut, BytesWriteError> {
        xrtp::RtpPacket::to_bytes(self)
    }
}

