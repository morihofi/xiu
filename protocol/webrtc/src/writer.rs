use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::WebRtcAdapter;

/// Writer converting [`MediaPacket`]s into WebRTC-specific payloads.
pub struct WebRtcWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl WebRtcWriter {
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(WebRtcAdapter);
        if enabled {
            register_adapter("webrtc", adapter.clone());
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
