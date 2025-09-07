use bytes::BytesMut;
use bytesio::bytes_reader::BytesReader;
use streamhub::{define::MediaPacket, ProtocolAdapter};

use crate::rtp::{rtp_header::RtpHeader, utils::{Marshal, Unmarshal}, RtpPacket};

/// Adapter for converting between RTSP payloads and [`MediaPacket`].
pub struct RtspAdapter;

impl ProtocolAdapter for RtspAdapter {
    /// Convert an RTP/RTSP payload into a [`MediaPacket`].
    ///
    /// The input is expected to contain a raw RTP packet (optionally framed
    /// using the RTSP interleaved header). The RTP header is parsed in order to
    /// populate timestamp related fields on the resulting [`MediaPacket`]. The
    /// payload of the RTP packet is used as the [`MediaPacket`]'s payload.
    fn to_packet(&self, mut payload: BytesMut) -> MediaPacket {
        // Remove RTSP interleaved framing if present. An interleaved frame
        // starts with '$' followed by a channel byte and a 16-bit length field.
        if payload.len() >= 4 && payload[0] == b'$' {
            let mut reader = BytesReader::new(payload);
            // Skip '$' and channel id
            let _ = reader.advance_bytes(2);
            if let Ok(len) = reader.read_u16::<byteorder::BigEndian>() {
                payload = reader
                    .read_bytes(len as usize)
                    .unwrap_or_else(|_| reader.extract_remaining_bytes());
            } else {
                payload = reader.extract_remaining_bytes();
            }
        }

        // Try to parse the RTP packet to extract timestamp and marker
        let mut pkt_reader = BytesReader::new(payload.clone());
        if let Ok(rtp) = RtpPacket::unmarshal(&mut pkt_reader) {
            return MediaPacket {
                stream_id: Default::default(),
                audio_codec: None,
                video_codec: None,
                pts: rtp.header.timestamp as u64,
                dts: rtp.header.timestamp as u64,
                is_keyframe: rtp.header.marker == 1,
                payload: rtp.payload,
            };
        }

        // Fallback – return the original payload with default metadata if the
        // RTP packet could not be parsed.
        MediaPacket {
            stream_id: Default::default(),
            audio_codec: None,
            video_codec: None,
            pts: 0,
            dts: 0,
            is_keyframe: false,
            payload,
        }
    }

    /// Convert a [`MediaPacket`] back into an RTP/RTSP payload. The packet's
    /// timestamp and keyframe flag are written to the RTP header. The result is
    /// framed using the RTSP interleaved format with channel id 0.
    fn from_packet(&self, packet: MediaPacket) -> BytesMut {
        let rtp_packet = RtpPacket {
            header: RtpHeader {
                timestamp: packet.pts as u32,
                marker: if packet.is_keyframe { 1 } else { 0 },
                ..Default::default()
            },
            payload: packet.payload,
            ..Default::default()
        };

        if let Ok(bytes) = rtp_packet.marshal() {
            // Wrap with RTSP interleaved header ('$' + channel + len)
            let mut framed = BytesMut::with_capacity(bytes.len() + 4);
            framed.extend_from_slice(&[b'$', 0]);
            framed.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            framed.extend_from_slice(&bytes[..]);
            framed
        } else {
            // On failure, just return the raw payload
            rtp_packet.payload
        }
    }
}
