use std::sync::Arc;
use streamhub::{adapter::register_adapter, ProtocolAdapter};

/// Adapter for converting between HTTP-FLV payloads and [`MediaPacket`].
pub struct HttpFlvAdapter;

impl ProtocolAdapter for HttpFlvAdapter {}

/// Register this protocol adapter in the global registry.
pub fn register() {
    register_adapter("httpflv", Arc::new(HttpFlvAdapter));
}
