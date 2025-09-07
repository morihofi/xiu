use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::RtmpAdapter;

/// Writer responsible for converting [`MediaPacket`]s into RTMP frames.
pub struct RtmpWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl RtmpWriter {
    /// Create a new writer. If `enabled` is true, the underlying adapter
    /// is registered with the global [`StreamHub`] registry.
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(RtmpAdapter);
        if enabled {
            register_adapter("rtmp", adapter.clone());
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
