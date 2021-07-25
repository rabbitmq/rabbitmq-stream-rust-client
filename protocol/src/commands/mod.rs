pub mod create_stream;
pub mod open;

pub trait Command {
    fn key(&self) -> u16;
}

pub trait Correlated {}

pub trait NoCorrelated {}
