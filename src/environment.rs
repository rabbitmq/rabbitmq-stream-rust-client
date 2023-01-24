use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::producer::NoDedup;
use crate::types::OffsetSpecification;

use crate::{
    client::{Client, ClientOptions, MetricsCollector},
    consumer::ConsumerBuilder,
    error::StreamDeleteError,
    producer::ProducerBuilder,
    stream_creator::StreamCreator,
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

    async fn boostrap(options: EnvironmentOptions) -> RabbitMQStreamResult<Self> {
        // check connection
        let client = Client::connect(options.client_options.clone()).await?;
        client.close().await?;
        Ok(Environment { options })
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
        }
    }

    /// Returns a builder for creating a consumer
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder {
            environment: self.clone(),
            offset_specification: OffsetSpecification::Next,
        }
    }
    pub(crate) async fn create_client(&self) -> RabbitMQStreamResult<Client> {
        Client::connect(self.options.client_options.clone()).await
    }

    /// Delete a stream
    pub async fn delete_stream(&self, stream: &str) -> Result<(), StreamDeleteError> {
        let response = self.create_client().await?.delete_stream(stream).await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamDeleteError::Delete {
                stream: stream.to_owned(),
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
        self.0
            .client_options
            .tls
            .trust_everything(tls_configuration.trust_everything_enabled());
        self.0
            .client_options
            .tls
            .hostname_verification_enable(tls_configuration.hostname_verification_enabled());
        self.0
            .client_options
            .tls
            .enable(tls_configuration.enabled());

        self
    }

    pub fn metrics_collector(
        mut self,
        collector: impl MetricsCollector + Send + Sync + 'static,
    ) -> EnvironmentBuilder {
        self.0.client_options.collector = Arc::new(collector);
        self
    }
}
#[derive(Clone, Default)]
pub struct EnvironmentOptions {
    pub(crate) client_options: ClientOptions,
}

/** Helper for tls configuration */
#[derive(Clone, Copy)]
pub struct TlsConfiguration {
    pub(crate) enabled: bool,
    pub(crate) hostname_verification: bool,
    pub(crate) trust_everything: bool,
}

impl Default for TlsConfiguration {
    fn default() -> TlsConfiguration {
        TlsConfiguration {
            enabled: true,
            trust_everything: false,
            hostname_verification: true,
        }
    }
}

impl TlsConfiguration {
    pub fn trust_everything(&mut self, trust_everything: bool) {
        self.trust_everything = trust_everything
    }

    pub fn enable(&mut self, enabled: bool) {
        self.enabled = enabled
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn hostname_verification_enable(&mut self, hostname_verification: bool) {
        self.hostname_verification = hostname_verification
    }

    pub fn hostname_verification_enabled(&self) -> bool {
        self.hostname_verification
    }

    pub fn trust_everything_enabled(&self) -> bool {
        self.trust_everything
    }
}
