use crate::client::Client;

use murmur3::murmur3_32;
use rabbitmq_stream_protocol::message::Message;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Clone)]
pub struct DefaultSuperStreamMetadata {
    pub super_stream: String,
    pub client: Client,
    pub partitions: Vec<String>,
    pub routes: HashMap<String, Vec<String>>,
}

impl DefaultSuperStreamMetadata {
    pub async fn partitions(&mut self) -> &Vec<String> {
        if self.partitions.is_empty() {
            let response = self.client.partitions(self.super_stream.clone()).await;

            self.partitions = response.unwrap().streams;
        }
        &self.partitions
    }
    pub async fn routes(&mut self, routing_key: String) -> &Vec<String> {
        if !self.routes.contains_key(&routing_key) {
            let response = self
                .client
                .route(routing_key.clone(), self.super_stream.clone())
                .await;

            self.routes
                .insert(routing_key.clone(), response.unwrap().streams);
        }

        &self.routes.get(routing_key.as_str()).unwrap()
    }
}

#[derive(Clone)]
pub struct RoutingKeyRoutingStrategy {
    pub routing_extractor: &'static dyn Fn(&Message) -> String,
}

impl RoutingKeyRoutingStrategy {
    pub async fn routes(
        &self,
        message: &Message,
        metadata: &mut DefaultSuperStreamMetadata,
    ) -> Vec<String> {
        let key = (self.routing_extractor)(message);

        metadata.routes(key).await.clone()

    }
}

#[derive(Clone)]
pub struct HashRoutingMurmurStrategy {
    pub routing_extractor: &'static dyn Fn(&Message) -> String,
}

impl HashRoutingMurmurStrategy {
    pub async fn routes(
        &self,
        message: &Message,
        metadata: &mut DefaultSuperStreamMetadata,
    ) -> Vec<String> {
        let mut streams: Vec<String> = Vec::new();

        let key = (self.routing_extractor)(message);
        let hash_result = murmur3_32(&mut Cursor::new(key), 104729);

        let partitions = metadata.partitions().await;
        let number_of_partitions = partitions.len();
        let route = hash_result.unwrap() % number_of_partitions as u32;

        let stream = partitions.into_iter().nth(route as usize).unwrap();
        streams.push(stream.clone());

        streams
    }
}

#[derive(Clone)]
pub enum RoutingStrategy {
    HashRoutingStrategy(HashRoutingMurmurStrategy),
    RoutingKeyStrategy(RoutingKeyRoutingStrategy),
}
