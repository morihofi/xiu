use byteorder::BigEndian;
use bytes::{BufMut, BytesMut};
use bytesio::bytes_errors::{BytesReadError, BytesWriteError};
use bytesio::bytes_reader::BytesReader;
use bytesio::bytes_writer::BytesWriter;

use crate::header::RtpHeader;

#[derive(Debug, Clone, Default)]
pub struct RtpPacket {
    pub header: RtpHeader,
    pub header_extension_profile: u16,
    pub header_extension_length: u16,
    pub header_extension_payload: BytesMut,
    pub payload: BytesMut,
    pub padding: BytesMut,
}

impl RtpPacket {
    pub fn read_from(reader: &mut BytesReader) -> Result<Self, BytesReadError> {
        //https://blog.jianchihu.net/webrtc-research-rtp-header-extension.html
        let mut rtp_packet = RtpPacket {
            header: RtpHeader::read_from(reader)?,
            ..Default::default()
        };

        if rtp_packet.header.extension_flag == 1 {
            // header_extension = profile(2 bytes) + length(2 bytes) + header extension payload
            rtp_packet.header_extension_profile = reader.read_u16::<BigEndian>()?;
            rtp_packet.header_extension_length = reader.read_u16::<BigEndian>()?;
            rtp_packet.header_extension_payload =
                reader.read_bytes(4 * rtp_packet.header_extension_length as usize)?;
        }

        if rtp_packet.header.padding_flag == 1 {
            let padding_length = reader.get(reader.len() - 1)? as usize;
            rtp_packet
                .payload
                .put(reader.read_bytes(reader.len() - padding_length)?);
            rtp_packet.padding.put(reader.read_bytes(padding_length)?);
        } else {
            rtp_packet.payload.put(reader.extract_remaining_bytes());
        }

        Ok(rtp_packet)
    }

    pub fn to_bytes(&self) -> Result<BytesMut, BytesWriteError> {
        let mut writer = BytesWriter::new();

        let header_bytesmut = self.header.to_bytes()?;
        writer.write(&header_bytesmut[..])?;

        if self.header.extension_flag == 1 {
            writer.write_u16::<BigEndian>(self.header_extension_profile)?;
            writer.write_u16::<BigEndian>(self.header_extension_length)?;
            writer.write(&self.header_extension_payload[..])?;
        }

        writer.write(&self.payload[..])?;
        if self.header.padding_flag == 1 {
            writer.write(&self.padding[..])?;
        }

        Ok(writer.extract_current_bytes())
    }

    pub fn new(header: RtpHeader) -> Self {
        Self {
            header,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_packet_no_ext_no_padding() {
        let hdr = RtpHeader {
            version: 2,
            payload_type: 97,
            seq_number: 4000,
            timestamp: 0x77889900,
            ssrc: 0x01020304,
            marker: 0,
            ..Default::default()
        };

        let pkt = RtpPacket {
            header: hdr,
            payload: BytesMut::from(&b"abc"[..]),
            ..Default::default()
        };
        let bytes = pkt.to_bytes().unwrap();

        let mut reader = BytesReader::new(bytes);
        let parsed = RtpPacket::read_from(&mut reader).unwrap();

        assert_eq!(parsed.header.payload_type, 97);
        assert_eq!(parsed.header.seq_number, 4000);
        assert_eq!(parsed.payload, BytesMut::from(&b"abc"[..]));
    }
}
