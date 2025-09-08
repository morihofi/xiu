pub mod adapter;
pub mod define;
pub mod errors;
pub mod httpflv;
pub mod server;
pub mod server_test;
pub mod writer;

pub fn init(enabled: bool) {
    writer::register(enabled);
}
