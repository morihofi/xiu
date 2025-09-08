use std::sync::Arc;
use streamhub::{adapter::register_adapter, ProtocolAdapter};

/// Adapter for converting between WebRTC payloads and [`MediaPacket`].
pub struct WebRtcAdapter;

impl ProtocolAdapter for WebRtcAdapter {}

/// Register this protocol adapter in the global registry.
pub fn register() {
    register_adapter("webrtc", Arc::new(WebRtcAdapter));
}
