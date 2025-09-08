use std::sync::Arc;
use streamhub::{adapter::register_adapter, ProtocolAdapter};

/// Adapter for converting between HLS payloads and [`MediaPacket`].
pub struct HlsAdapter;

impl ProtocolAdapter for HlsAdapter {}

/// Register this protocol adapter in the global registry.
pub fn register() {
    register_adapter("hls", Arc::new(HlsAdapter));
}
