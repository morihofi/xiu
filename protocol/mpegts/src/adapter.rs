use std::sync::Arc;
use streamhub::{adapter::register_adapter, ProtocolAdapter};

/// Adapter for converting between MPEG-TS payloads and [`MediaPacket`].
pub struct MpegTsAdapter;

impl ProtocolAdapter for MpegTsAdapter {}

/// Register this protocol adapter in the global registry.
pub fn register() {
    register_adapter("mpegts", Arc::new(MpegTsAdapter));
}
