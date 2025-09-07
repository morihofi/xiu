use byteorder::BigEndian;
use bytes::BytesMut;
use bytesio::bytes_reader::BytesReader;
use xflv::{define, flv_tag_header::AudioTagHeader, Unmarshal};

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

/// Extract H264 NALUs from an FLV video tag and return Annex-B formatted data.
///
/// FLV video tags carrying H264 data use the AVC NALU packet type where the
/// payload is composed of a series of length-prefixed NAL units. Each NALU is
/// preceded by a 4-byte big-endian length field. This function iterates through
/// all NALUs and converts them to Annex-B by prefixing each with the four-byte
/// start code `0x00 0x00 0x00 0x01`.
pub fn remux_h264(data: &BytesMut) -> Option<BytesMut> {
    let mut reader = BytesReader::new(data.clone());
    let _ = reader.read_u8().ok()?; // flags
    let avc_packet_type = reader.read_u8().ok()?;
    let _ = reader.read_u24::<BigEndian>().ok()?; // composition time

    if avc_packet_type != define::avc_packet_type::AVC_NALU {
        return None;
    }

    let remain = reader.extract_remaining_bytes();
    let mut r = BytesReader::new(remain);
    let mut annexb = BytesMut::new();

    while r.len() >= 4 {
        let nalu_size = r.read_u32::<BigEndian>().ok()? as usize;
        if r.len() < nalu_size {
            return None;
        }
        annexb.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        let nalu = r.read_bytes(nalu_size).ok()?;
        annexb.extend_from_slice(&nalu[..]);
    }

    if annexb.is_empty() {
        None
    } else {
        Some(annexb)
    }
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
        assert_eq!(
            payload,
            BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21][..])
        );
    }

    #[test]
    fn test_remux_h264_multi_nalus() {
        let mut data = BytesMut::new();
        data.extend_from_slice(&[0x17, 0x01, 0x00, 0x00, 0x00]);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]);
        data.extend_from_slice(&[0x06, 0xE5]);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x03]);
        data.extend_from_slice(&[0x41, 0x9A, 0x22]);

        let payload = remux_h264(&data).unwrap();
        assert_eq!(
            payload,
            BytesMut::from(
                &[0x00, 0x00, 0x00, 0x01, 0x06, 0xE5, 0x00, 0x00, 0x00, 0x01, 0x41, 0x9A, 0x22,][..]
            )
        );
    }
}
