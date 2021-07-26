pub mod create_stream;
pub mod open;
pub mod peer_properties;
pub mod sasl_authenticate;
pub mod sasl_handshake;

pub trait Command {
    fn key(&self) -> u16;
}
