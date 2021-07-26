pub mod create_stream;
pub mod open;
pub mod peer_properties;

pub trait Command {
    fn key(&self) -> u16;
}
