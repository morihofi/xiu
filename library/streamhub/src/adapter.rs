use bytes::BytesMut;
use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::{Arc, RwLock}};

use crate::{define::MediaPacket, stream::StreamIdentifier};

/// Trait used to translate between protocol specific payloads and [`MediaPacket`].
pub trait ProtocolAdapter {
    /// Convert a protocol specific payload into a [`MediaPacket`].
    fn to_packet(&self, payload: BytesMut) -> MediaPacket {
        MediaPacket {
            stream_id: StreamIdentifier::default(),
            audio_codec: None,
            video_codec: None,
            pts: 0,
            dts: 0,
            is_keyframe: false,
            payload,
        }
    }

    /// Convert a [`MediaPacket`] back into a protocol specific payload.
    fn from_packet(&self, packet: MediaPacket) -> BytesMut {
        packet.payload
    }
}

/// Type alias for a boxed [`ProtocolAdapter`] that can be shared between threads.
pub type DynAdapter = Arc<dyn ProtocolAdapter + Send + Sync>;

/// Global registry for protocol adapters.
static ADAPTER_REGISTRY: Lazy<RwLock<HashMap<&'static str, DynAdapter>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

/// Register an adapter under a protocol name.
pub fn register_adapter(name: &'static str, adapter: DynAdapter) {
    ADAPTER_REGISTRY
        .write()
        .expect("adapter registry poisoned")
        .insert(name, adapter);
}

/// Retrieve a registered adapter by protocol name.
pub fn get_adapter(name: &str) -> Option<DynAdapter> {
    ADAPTER_REGISTRY
        .read()
        .expect("adapter registry poisoned")
        .get(name)
        .cloned()
}
