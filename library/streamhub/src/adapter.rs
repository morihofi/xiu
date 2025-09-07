use bytes::BytesMut;

use crate::{define::MediaPacket, stream::StreamIdentifier};

/// Trait used to translate between protocol specific payloads and [`MediaPacket`].
pub trait ProtocolAdapter {
    /// Convert a protocol specific payload into a [`MediaPacket`].
    fn to_packet(&self, payload: BytesMut) -> MediaPacket {
        MediaPacket {
            stream_id: StreamIdentifier::default(),
            audio_codec: None,
            video_codec: None,
            pts: 0,
            dts: 0,
            is_keyframe: false,
            payload,
        }
    }

    /// Convert a [`MediaPacket`] back into a protocol specific payload.
    fn from_packet(&self, packet: MediaPacket) -> BytesMut {
        packet.payload
    }
}
