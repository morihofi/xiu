use streamhub::ProtocolAdapter;

/// Adapter for converting between RTMP payloads and [`MediaPacket`].
pub struct RtmpAdapter;

impl ProtocolAdapter for RtmpAdapter {}
