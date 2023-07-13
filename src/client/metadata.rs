use std::collections::HashMap;

use rabbitmq_stream_protocol::{commands::metadata::MetadataResponse, ResponseCode};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Broker {
    pub host: String,
    pub port: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct StreamMetadata {
    pub stream: String,
    pub response_code: ResponseCode,
    pub leader: Broker,
    pub replicas: Vec<Broker>,
}

pub fn from_response(response: MetadataResponse) -> HashMap<String, StreamMetadata> {
    let brokers: HashMap<u16, Broker> = response
        .brokers
        .into_iter()
        .map(|broker| {
            (
                broker.reference,
                Broker {
                    host: broker.host,
                    port: broker.port,
                },
            )
        })
        .collect();

    response
        .stream_metadata
        .into_iter()
        .filter_map(|metadata| {
            brokers.get(&metadata.leader_reference).map(|leader| {
                (
                    metadata.stream_name.clone(),
                    StreamMetadata {
                        stream: metadata.stream_name,
                        response_code: metadata.code,
                        leader: leader.clone(),
                        replicas: metadata
                            .replicas_references
                            .into_iter()
                            .filter_map(|replica| brokers.get(&replica).cloned())
                            .collect(),
                    },
                )
            })
        })
        .collect()
}
