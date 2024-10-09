use crate::{
    client::Client,
};

use murmur3::murmur3_32;
use std::any::Any;
use rabbitmq_stream_protocol::message::Message;
use std::io::Cursor;

trait Metadata   {

    async fn partitions(&mut self) -> Vec<String>;
    async fn routes(&mut self, routing_key: String) -> Vec<String>;

}

struct DefaultSuperStreamMetadata   {
    super_stream: String,
    client: Client,
    partitions: Vec<String>,
    routes: Vec<String>,
}

impl Metadata for DefaultSuperStreamMetadata    {

    async fn partitions(&mut self) -> Vec<String>   {

        if self.partitions.len() == 0    {

            let response = self.client.partitions(self.super_stream.clone()).await;

            self.partitions = response.unwrap().streams;

        }

        return self.partitions.clone()

    }
    async fn routes(&mut self, routing_key: String) -> Vec<String>    {

        if self.routes.len() == 0    {

            let response = self.client.route(routing_key, self.super_stream.clone()).await;

            self.routes = response.unwrap().streams;

        }

        return self.routes.clone()

    }

}

trait RoutingStrategy   {
    async fn routes(&self, message: Message, metadata: & mut impl Metadata) -> Vec<String>;
}

struct RoutingKeyRoutingStrategy   {
    routing_extractor: &'static dyn Fn(Message) -> String,
}

impl RoutingStrategy for RoutingKeyRoutingStrategy    {

    async fn routes(&self, message: Message, metadata: & mut impl Metadata) -> Vec<String>   {

        let key = (self.routing_extractor)(message);

        let routes = metadata.routes(key).await;

        return routes;

    }
}

struct HashRoutingMurmurStrategy   {
    routing_extractor: &'static dyn Fn(Message) -> String,
}

impl RoutingStrategy for HashRoutingMurmurStrategy {

    async fn routes(&self, message: Message, metadata: & mut impl Metadata) -> Vec<String>   {

        let mut streams: Vec<String> = Vec::new();

        let key = (self.routing_extractor)(message);
        let hash_result = murmur3_32(&mut Cursor::new(key), 104729);

        let number_of_partitions = metadata.partitions().await.len();
        let route = hash_result.unwrap() % number_of_partitions as u32;
        let partitions: Vec<String> = metadata.partitions().await;
        let stream = partitions.into_iter().nth(route as usize).unwrap();
        streams.push(stream);

        return streams

    }
}