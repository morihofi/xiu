use streamhub::ProtocolAdapter;

/// Adapter for converting between WebRTC payloads and [`MediaPacket`].
pub struct WebRtcAdapter;

impl ProtocolAdapter for WebRtcAdapter {}
