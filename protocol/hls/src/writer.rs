use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::HlsAdapter;

/// Writer producing HLS segments from [`MediaPacket`]s.
pub struct HlsWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl HlsWriter {
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(HlsAdapter);
        if enabled {
            register_adapter("hls", adapter.clone());
        }
        Self { adapter, enabled }
    }

    pub fn write(&self, packet: MediaPacket) -> Option<BytesMut> {
        if !self.enabled {
            return None;
        }
        Some(self.adapter.from_packet(packet))
    }
}
