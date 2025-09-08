use byteorder::BigEndian;
use bytes::BytesMut;
use bytesio::bytes_errors::{BytesReadError, BytesWriteError};
use bytesio::bytes_reader::BytesReader;
use bytesio::bytes_writer::BytesWriter;

#[derive(Debug, Clone, Default)]
pub struct RtpHeader {
    pub version: u8,        // 2 bits
    pub padding_flag: u8,   // 1 bit
    pub extension_flag: u8, // 1 bit
    pub cc: u8,             // 4 bits
    pub marker: u8,         // 1 bit
    pub payload_type: u8,   // 7 bits
    pub seq_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub csrcs: Vec<u32>,
}

impl RtpHeader {
    pub fn read_from(reader: &mut BytesReader) -> Result<Self, BytesReadError> {
        let mut rtp_header = RtpHeader::default();

        let byte_1st: u8 = reader.read_u8()?;
        rtp_header.version = byte_1st >> 6;
        rtp_header.padding_flag = byte_1st >> 5 & 0x01;
        rtp_header.extension_flag = byte_1st >> 4 & 0x01;
        rtp_header.cc = byte_1st & 0x0F;

        let byte_2nd = reader.read_u8()?;
        rtp_header.marker = byte_2nd >> 7;
        rtp_header.payload_type = byte_2nd & 0x7F;
        rtp_header.seq_number = reader.read_u16::<BigEndian>()?;
        rtp_header.timestamp = reader.read_u32::<BigEndian>()?;
        rtp_header.ssrc = reader.read_u32::<BigEndian>()?;

        for _ in 0..rtp_header.cc {
            rtp_header.csrcs.push(reader.read_u32::<BigEndian>()?);
        }

        Ok(rtp_header)
    }

    pub fn to_bytes(&self) -> Result<BytesMut, BytesWriteError> {
        //  0                   1                   2                   3
        //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        // |V=2|P|X|  CC   |M|     PT      |       sequence number         |
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        // |                           timestamp                           |
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        // |           synchronization source (SSRC) identifier            |
        // +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
        // |            contributing source (CSRC) identifiers             |
        // |                             ....                              |
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        let mut writer = BytesWriter::default();

        let byte_1st: u8 = (self.version << 6)
            | (self.padding_flag << 5)
            | (self.extension_flag << 4)
            | (self.cc & 0x0F);
        writer.write_u8(byte_1st)?;

        let byte_2nd: u8 = (self.marker << 7) | self.payload_type;
        writer.write_u8(byte_2nd)?;

        writer.write_u16::<BigEndian>(self.seq_number)?;
        writer.write_u32::<BigEndian>(self.timestamp)?;
        writer.write_u32::<BigEndian>(self.ssrc)?;

        for csrc in &self.csrcs {
            writer.write_u32::<BigEndian>(*csrc)?;
        }

        Ok(writer.extract_current_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_header() {
        let hdr = RtpHeader {
            version: 2,
            padding_flag: 0,
            extension_flag: 1,
            cc: 2,
            marker: 1,
            payload_type: 96,
            seq_number: 12345,
            timestamp: 0x11223344,
            ssrc: 0x55667788,
            csrcs: vec![0xAABBCCDD, 0x01020304],
        };

        let bytes = hdr.to_bytes().unwrap();
        let mut reader = BytesReader::new(bytes);
        let parsed = RtpHeader::read_from(&mut reader).unwrap();

        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.extension_flag, 1);
        assert_eq!(parsed.cc, 2);
        assert_eq!(parsed.marker, 1);
        assert_eq!(parsed.payload_type, 96);
        assert_eq!(parsed.seq_number, 12345);
        assert_eq!(parsed.timestamp, 0x11223344);
        assert_eq!(parsed.ssrc, 0x55667788);
        assert_eq!(parsed.csrcs, vec![0xAABBCCDD, 0x01020304]);
    }
}
