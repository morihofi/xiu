pub mod adapter;
pub mod writer;

pub fn init(enabled: bool) {
    writer::register(enabled);
}
