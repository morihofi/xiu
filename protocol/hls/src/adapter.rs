use streamhub::ProtocolAdapter;

/// Adapter for converting between HLS payloads and [`MediaPacket`].
pub struct HlsAdapter;

impl ProtocolAdapter for HlsAdapter {}
