use bytes::BytesMut;
use bytesio::bytes_reader::BytesReader;
use xflv::{define, flv_tag_header::AudioTagHeader, Unmarshal};
use byteorder::BigEndian;

/// Extract raw AAC data from an FLV audio tag.
pub fn remux_aac(data: &BytesMut) -> Option<BytesMut> {
    let mut reader = BytesReader::new(data.clone());
    let tag = AudioTagHeader::unmarshal(&mut reader).ok()?;
    let remain = reader.extract_remaining_bytes();
    if tag.sound_format == define::SoundFormat::AAC as u8
        && tag.aac_packet_type == define::aac_packet_type::AAC_RAW
    {
        Some(remain)
    } else {
        None
    }
}

/// Extract a single H264 NALU payload from an FLV video tag.
/// Currently only supports AVC NALU packets with a single NALU.
pub fn remux_h264(data: &BytesMut) -> Option<BytesMut> {
    let mut reader = BytesReader::new(data.clone());
    let _ = reader.read_u8().ok()?; // flags
    let avc_packet_type = reader.read_u8().ok()?;
    let _ = reader.read_u24::<BigEndian>().ok()?; // composition time
    if avc_packet_type != define::avc_packet_type::AVC_NALU {
        return None;
    }
    let remain = reader.extract_remaining_bytes();
    if remain.len() < 4 {
        return None;
    }
    let mut r = BytesReader::new(remain);
    let _ = r.read_u32::<BigEndian>().ok()?;
    r.read_bytes(r.len()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remux_aac() {
        let data = BytesMut::from(&[0xAF, 0x01, 0x11, 0x22, 0x33][..]);
        let payload = remux_aac(&data).unwrap();
        assert_eq!(payload, BytesMut::from(&[0x11, 0x22, 0x33][..]));
    }

    #[test]
    fn test_remux_h264() {
        let mut data = BytesMut::new();
        data.extend_from_slice(&[0x17, 0x01, 0x00, 0x00, 0x00]);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x04]);
        data.extend_from_slice(&[0x65, 0x88, 0x84, 0x21]);
        let payload = remux_h264(&data).unwrap();
        assert_eq!(payload, BytesMut::from(&[0x65, 0x88, 0x84, 0x21][..]));
    }
}
