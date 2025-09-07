use streamhub::ProtocolAdapter;

/// Adapter for converting between MPEG-TS payloads and [`MediaPacket`].
pub struct MpegTsAdapter;

impl ProtocolAdapter for MpegTsAdapter {}
