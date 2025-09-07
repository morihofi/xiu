use streamhub::ProtocolAdapter;

/// Adapter for converting between HTTP-FLV payloads and [`MediaPacket`].
pub struct HttpFlvAdapter;

impl ProtocolAdapter for HttpFlvAdapter {}
