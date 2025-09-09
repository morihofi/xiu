pub mod adapter;
pub mod writer;
pub mod http;

pub fn init(enabled: bool) {
    writer::register(enabled);
}
