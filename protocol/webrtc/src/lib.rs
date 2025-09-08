pub mod errors;
// pub mod http;
pub mod adapter;
pub mod opus2aac;
pub mod rtp_queue;
pub mod session;
pub mod webrtc;
pub mod whep;
pub mod whip;
pub mod writer;

pub fn init(enabled: bool) {
    writer::register(enabled);
}
