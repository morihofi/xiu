use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::MpegTsAdapter;

/// Writer that converts [`MediaPacket`]s into MPEG-TS segments.
pub struct MpegTsWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl MpegTsWriter {
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(MpegTsAdapter);
        if enabled {
            register_adapter("mpegts", adapter.clone());
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
