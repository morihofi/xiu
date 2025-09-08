/// Macro to generate protocol-specific writers and registration helpers.
///
/// The basic form `stream_writer!(WriterType, AdapterType, "name")` generates a
/// struct `WriterType` holding an `Arc<AdapterType>` and a corresponding
/// `register` function. The writer exposes a `write` method that delegates to the
/// adapter's `from_packet` implementation.
///
/// An optional list of extra parameters can be supplied after the protocol name
/// to forward additional arguments to the adapter's `from_packet` method, e.g.
/// `stream_writer!(RtspWriter, RtspAdapter, "rtsp", channel_id: u8)`.
#[macro_export]
macro_rules! stream_writer {
    ($writer:ident, $adapter:ident, $name:expr) => {
        /// Register the adapter for this protocol with the global registry.
        pub fn register(enabled: bool) {
            if enabled {
                let adapter = std::sync::Arc::new($adapter);
                $crate::adapter::register_adapter($name, adapter.clone() as $crate::adapter::DynAdapter);
            }
        }

        /// Thin writer wrapper around the protocol adapter.
        pub struct $writer {
            adapter: std::sync::Arc<$adapter>,
            enabled: bool,
        }

        impl $writer {
            /// Create a new writer. The `enabled` flag determines whether the
            /// writer should actually produce data when [`write`] is called.
            pub fn new(enabled: bool) -> Self {
                Self {
                    adapter: std::sync::Arc::new($adapter),
                    enabled,
                }
            }

            /// Convert a [`MediaPacket`] into protocol specific bytes.
            pub fn write(&self, packet: $crate::define::MediaPacket) -> Option<bytes::BytesMut> {
                if !self.enabled {
                    return None;
                }
                Some($crate::ProtocolAdapter::from_packet(&*self.adapter, packet))
            }
        }
    };
    ($writer:ident, $adapter:ident, $name:expr, $( $arg:ident : $argty:ty ),+ ) => {
        /// Register the adapter for this protocol with the global registry.
        pub fn register(enabled: bool) {
            if enabled {
                let adapter = std::sync::Arc::new($adapter);
                $crate::adapter::register_adapter($name, adapter.clone() as $crate::adapter::DynAdapter);
            }
        }

        /// Thin writer wrapper around the protocol adapter.
        pub struct $writer {
            adapter: std::sync::Arc<$adapter>,
            enabled: bool,
        }

        impl $writer {
            /// Create a new writer. The `enabled` flag determines whether the
            /// writer should actually produce data when [`write`] is called.
            pub fn new(enabled: bool) -> Self {
                Self {
                    adapter: std::sync::Arc::new($adapter),
                    enabled,
                }
            }

            /// Convert a [`MediaPacket`] into protocol specific bytes using
            /// additional parameters forwarded to the adapter.
            pub fn write(&self, packet: $crate::define::MediaPacket, $( $arg : $argty ),+ ) -> Option<bytes::BytesMut> {
                if !self.enabled {
                    return None;
                }
                Some(self.adapter.from_packet(packet, $( $arg ),+ ))
            }
        }
    };
}
