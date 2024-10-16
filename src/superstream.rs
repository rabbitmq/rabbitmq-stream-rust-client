use crate::client::Client;

use murmur3::murmur3_32;
use rabbitmq_stream_protocol::message::Message;
use std::io::Cursor;

#[derive(Clone)]
pub struct DefaultSuperStreamMetadata {
    pub super_stream: String,
    pub client: Client,
    pub partitions: Vec<String>,
    pub routes: Vec<String>,
}

impl DefaultSuperStreamMetadata {
    pub async fn partitions(&mut self) -> Vec<String> {
        if self.partitions.len() == 0 {
            println!("partition len is 0");
            let response = self.client.partitions(self.super_stream.clone()).await;

            self.partitions = response.unwrap().streams;
        }
        return self.partitions.clone();
    }
    pub async fn routes(&mut self, routing_key: String) -> Vec<String> {
        if self.routes.len() == 0 {
            let response = self
                .client
                .route(routing_key, self.super_stream.clone())
                .await;

            self.routes = response.unwrap().streams;
        }

        return self.routes.clone();
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

        let routes = metadata.routes(key).await;

        return routes;
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
        println!("im in routes");
        let mut streams: Vec<String> = Vec::new();

        let key = (self.routing_extractor)(message);
        let hash_result = murmur3_32(&mut Cursor::new(key), 104729);

        let number_of_partitions = metadata.partitions().await.len();
        let route = hash_result.unwrap() % number_of_partitions as u32;
        let partitions = metadata.partitions().await;
        let stream = partitions.into_iter().nth(route as usize).unwrap();
        streams.push(stream);

        return streams;
    }
}

#[derive(Clone)]
pub enum RoutingStrategy {
    HashRoutingStrategy(HashRoutingMurmurStrategy),
    RoutingKeyStrategy(RoutingKeyRoutingStrategy),
}
