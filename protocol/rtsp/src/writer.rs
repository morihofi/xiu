use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::RtspAdapter;

/// Writer responsible for converting [`MediaPacket`]s into RTSP frames.
pub struct RtspWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl RtspWriter {
    /// Create a new writer. When `enabled` is true the underlying adapter is
    /// registered with the global [`StreamHub`] registry under the "rtsp" key.
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(RtspAdapter);
        if enabled {
            register_adapter("rtsp", adapter.clone());
        }
        Self { adapter, enabled }
    }

    /// Convert a [`MediaPacket`] into protocol specific bytes.
    pub fn write(&self, packet: MediaPacket) -> Option<BytesMut> {
        if !self.enabled {
            return None;
        }
        Some(self.adapter.from_packet(packet))
    }
}
