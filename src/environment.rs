use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::types::OffsetSpecification;
use crate::{client::TlsConfiguration, producer::NoDedup};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::HashMap;

use crate::{
    client::{Client, ClientOptions, MetricsCollector},
    consumer::ConsumerBuilder,
    error::{ConsumerCreateError, ProducerCreateError, StreamDeleteError},
    producer::ProducerBuilder,
    stream_creator::StreamCreator,
    superstream::RoutingStrategy,
    superstream_consumer::SuperStreamConsumerBuilder,
    superstream_producer::SuperStreamProducerBuilder,
    RabbitMQStreamResult,
};

/// Main access point to a node
#[derive(Clone)]
pub struct Environment {
    pub(crate) options: EnvironmentOptions,
}

impl Environment {
    pub fn builder() -> EnvironmentBuilder {
        EnvironmentBuilder(EnvironmentOptions::default())
    }

    #[cfg(feature = "serde")]
    pub async fn from_client_option(
        client_options: impl Into<ClientOptions>,
    ) -> RabbitMQStreamResult<Self> {
        let env_option = EnvironmentOptions {
            client_options: client_options.into(),
        };
        Self::boostrap(env_option).await
    }

    async fn boostrap(options: EnvironmentOptions) -> RabbitMQStreamResult<Self> {
        // check connection
        let client = Client::connect(options.client_options.clone()).await?;
        client.close().await?;
        Ok(Environment { options })
    }

    pub(crate) async fn create_producer_client(
        self,
        stream: &str,
        client_provided_name: String,
    ) -> Result<Client, ProducerCreateError> {
        let mut opt_with_client_provided_name = self.options.client_options.clone();
        opt_with_client_provided_name.client_provided_name = client_provided_name.clone();

        let mut client = self
            .create_client_with_options(opt_with_client_provided_name.clone())
            .await?;

        if let Some(metadata) = client.metadata(vec![stream.to_string()]).await?.get(stream) {
            tracing::debug!(
                "Connecting to leader node {:?} of stream {}",
                metadata.leader,
                stream
            );
            let load_balancer_mode = self.options.client_options.load_balancer_mode;
            if load_balancer_mode {
                // Producer must connect to leader node
                let options: ClientOptions = self.options.client_options.clone();
                loop {
                    let temp_client = Client::connect(options.clone()).await?;
                    let mapping = temp_client.connection_properties().await;
                    if let Some(advertised_host) = mapping.get("advertised_host") {
                        if *advertised_host == metadata.leader.host.clone() {
                            client.close().await?;
                            client = temp_client;
                            break;
                        }
                    }
                    temp_client.close().await?;
                }
            } else {
                client.close().await?;
                client = Client::connect(ClientOptions {
                    host: metadata.leader.host.clone(),
                    port: metadata.leader.port as u16,
                    ..opt_with_client_provided_name.clone()
                })
                .await?
            };
        } else {
            return Err(ProducerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

        Ok(client)
    }

    pub(crate) async fn create_consumer_client(
        self,
        stream: &str,
        client_provided_name: String,
    ) -> Result<Client, ConsumerCreateError> {
        let mut opt_with_client_provided_name = self.options.client_options.clone();
        opt_with_client_provided_name.client_provided_name = client_provided_name.clone();

        let mut client = self
            .create_client_with_options(opt_with_client_provided_name.clone())
            .await?;

        if let Some(metadata) = client.metadata(vec![stream.to_string()]).await?.get(stream) {
            // If there are no replicas we do not reassign client, meaning we just keep reading from the leader.
            // This is desired behavior in case there is only one node in the cluster.
            if let Some(replica) = metadata.replicas.choose(&mut StdRng::from_entropy()) {
                tracing::debug!(
                    "Picked replica {:?} out of possible candidates {:?} for stream {}",
                    replica,
                    metadata.replicas,
                    stream
                );
                let load_balancer_mode = self.options.client_options.load_balancer_mode;
                if load_balancer_mode {
                    let options = self.options.client_options.clone();
                    loop {
                        let temp_client = Client::connect(options.clone()).await?;
                        let mapping = temp_client.connection_properties().await;
                        if let Some(advertised_host) = mapping.get("advertised_host") {
                            if *advertised_host == replica.host.clone() {
                                client.close().await?;
                                client = temp_client;
                                break;
                            }
                        }
                        temp_client.close().await?;
                    }
                } else {
                    client.close().await?;
                    client = Client::connect(ClientOptions {
                        host: replica.host.clone(),
                        port: replica.port as u16,
                        ..self.options.client_options
                    })
                    .await?;
                }
            }
        } else {
            return Err(ConsumerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

        Ok(client)
    }

    /// Returns a builder for creating a stream with a specific configuration
    pub fn stream_creator(&self) -> StreamCreator {
        StreamCreator::new(self.clone())
    }

    /// Returns a builder for creating a producer
    pub fn producer(&self) -> ProducerBuilder<NoDedup> {
        ProducerBuilder {
            environment: self.clone(),
            name: None,
            batch_size: 100,
            batch_publishing_delay: Duration::from_millis(100),
            data: PhantomData,
            filter_value_extractor: None,
            client_provided_name: String::from("rust-stream-producer"),
        }
    }

    pub fn super_stream_producer(
        &self,
        routing_strategy: RoutingStrategy,
    ) -> SuperStreamProducerBuilder<NoDedup> {
        SuperStreamProducerBuilder {
            environment: self.clone(),
            data: PhantomData,
            filter_value_extractor: None,
            route_strategy: routing_strategy,
            client_provided_name: String::from("rust-super-stream-producer"),
        }
    }

    /// Returns a builder for creating a consumer
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder {
            consumer_name: None,
            environment: self.clone(),
            offset_specification: OffsetSpecification::Next,
            filter_configuration: None,
            consumer_update_listener: None,
            client_provided_name: String::from("rust-stream-consumer"),
            properties: HashMap::new(),
            is_single_active_consumer: false,
        }
    }

    pub fn super_stream_consumer(&self) -> SuperStreamConsumerBuilder {
        SuperStreamConsumerBuilder {
            super_stream_consumer_name: None,
            environment: self.clone(),
            offset_specification: OffsetSpecification::Next,
            filter_configuration: None,
            consumer_update_listener: None,
            client_provided_name: String::from("rust-super-stream-consumer"),
            properties: HashMap::new(),
            is_single_active_consumer: false,
        }
    }

    pub(crate) async fn create_client(&self) -> RabbitMQStreamResult<Client> {
        Client::connect(self.options.client_options.clone()).await
    }
    pub(crate) async fn create_client_with_options(
        &self,
        opts: impl Into<ClientOptions>,
    ) -> RabbitMQStreamResult<Client> {
        Client::connect(opts).await
    }

    /// Delete a stream
    pub async fn delete_stream(&self, stream: &str) -> Result<(), StreamDeleteError> {
        let client = self.create_client().await?;
        let response = client.delete_stream(stream).await?;
        client.close().await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamDeleteError::Delete {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub async fn delete_super_stream(&self, super_stream: &str) -> Result<(), StreamDeleteError> {
        let client = self.create_client().await?;
        let response = client.delete_super_stream(super_stream).await?;
        client.close().await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamDeleteError::Delete {
                stream: super_stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }
}

/// Builder for [`Environment`]
pub struct EnvironmentBuilder(EnvironmentOptions);

impl EnvironmentBuilder {
    pub async fn build(self) -> RabbitMQStreamResult<Environment> {
        Environment::boostrap(self.0).await
    }

    pub fn host(mut self, host: &str) -> EnvironmentBuilder {
        self.0.client_options.host = host.to_owned();
        self
    }

    pub fn username(mut self, username: &str) -> EnvironmentBuilder {
        self.0.client_options.user = username.to_owned();
        self
    }

    pub fn password(mut self, password: &str) -> EnvironmentBuilder {
        self.0.client_options.password = password.to_owned();
        self
    }

    pub fn virtual_host(mut self, virtual_host: &str) -> EnvironmentBuilder {
        self.0.client_options.v_host = virtual_host.to_owned();
        self
    }
    pub fn port(mut self, port: u16) -> EnvironmentBuilder {
        self.0.client_options.port = port;
        self
    }

    pub fn tls(mut self, tls_configuration: TlsConfiguration) -> EnvironmentBuilder {
        self.0.client_options.tls = tls_configuration;

        self
    }

    pub fn heartbeat(mut self, heartbeat: u32) -> EnvironmentBuilder {
        self.0.client_options.heartbeat = heartbeat;
        self
    }
    pub fn metrics_collector(
        mut self,
        collector: impl MetricsCollector + 'static,
    ) -> EnvironmentBuilder {
        self.0.client_options.collector = Arc::new(collector);
        self
    }

    pub fn load_balancer_mode(mut self, load_balancer_mode: bool) -> EnvironmentBuilder {
        self.0.client_options.load_balancer_mode = load_balancer_mode;
        self
    }

    pub fn client_provided_name(mut self, name: &str) -> EnvironmentBuilder {
        self.0.client_options.client_provided_name = name.to_owned();
        self
    }
}
#[derive(Clone, Default)]
pub struct EnvironmentOptions {
    pub(crate) client_options: ClientOptions,
}
