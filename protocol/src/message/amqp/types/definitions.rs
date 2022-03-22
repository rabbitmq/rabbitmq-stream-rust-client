use super::{annotations::Annotations, List, Map, SimpleValue, Str, Value};

pub type SequenceNo = u32;
pub type Address = Str;

pub type Footer = Annotations;

pub type DeliveryAnnotations = Annotations;

pub type MessageAnnotations = Annotations;

pub type ApplicationProperties = Map<Str, SimpleValue>;

pub type AmqpSequence = List;
pub type AmqpValue = Value;
