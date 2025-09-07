use bytes::BytesMut;
use std::sync::Arc;

use streamhub::{define::MediaPacket, adapter::{DynAdapter, register_adapter}};

use crate::adapter::HttpFlvAdapter;

/// Writer for HTTP-FLV frames derived from [`MediaPacket`]s.
pub struct HttpFlvWriter {
    adapter: DynAdapter,
    enabled: bool,
}

impl HttpFlvWriter {
    pub fn new(enabled: bool) -> Self {
        let adapter: DynAdapter = Arc::new(HttpFlvAdapter);
        if enabled {
            register_adapter("httpflv", adapter.clone());
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
