use super::{annotations::Annotations, List, Map, SimpleValue, Str, Value};

pub type SequenceNo = u32;
pub type Address = Str;

/// Footer type definition [`Annotations`]
pub type Footer = Annotations;

/// Delivery annotations type definition [`Annotations`]
pub type DeliveryAnnotations = Annotations;

/// Message annotations type definition [`Annotations`]
pub type MessageAnnotations = Annotations;

/// Application properties type definition [`Map`]
pub type ApplicationProperties = Map<Str, SimpleValue>;

pub type AmqpSequence = List;
pub type AmqpValue = Value;
