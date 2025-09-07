use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{
    adapter::{register_adapter, DynAdapter},
    define::MediaPacket,
};

use crate::adapter::RtspAdapter;

/// Writer responsible for converting [`MediaPacket`]s into RTSP frames.
pub struct RtspWriter {
    adapter: Arc<RtspAdapter>,
    enabled: bool,
}

impl RtspWriter {
    /// Create a new writer. When `enabled` is true the underlying adapter is
    /// registered with the global [`StreamHub`] registry under the "rtsp" key.
    pub fn new(enabled: bool) -> Self {
        let adapter = Arc::new(RtspAdapter);
        if enabled {
            register_adapter("rtsp", adapter.clone() as DynAdapter);
        }
        Self { adapter, enabled }
    }

    /// Convert a [`MediaPacket`] into protocol specific bytes using the supplied
    /// RTSP interleaved channel identifier.
    pub fn write(&self, packet: MediaPacket, channel_id: u8) -> Option<BytesMut> {
        if !self.enabled {
            return None;
        }
        Some(self.adapter.from_packet(packet, channel_id))
    }
}
