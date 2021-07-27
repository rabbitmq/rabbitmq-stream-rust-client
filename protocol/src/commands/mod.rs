pub mod create_stream;
pub mod open;
pub mod peer_properties;
pub mod sasl_handshake;
pub mod tune;

pub trait Command {
    fn key(&self) -> u16;
}
