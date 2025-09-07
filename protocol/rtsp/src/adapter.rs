use streamhub::ProtocolAdapter;

/// Adapter for converting between RTSP payloads and [`MediaPacket`].
pub struct RtspAdapter;

impl ProtocolAdapter for RtspAdapter {}
