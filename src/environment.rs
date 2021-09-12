use crate::{
    error::StreamDeleteError, stream_creator::StreamCreator, Client, ClientOptions,
    RabbitMQStreamResult,
};

#[derive(Clone)]
pub struct Environment {
    pub(crate) options: EnvironmentOptions,
}

impl Environment {
    pub fn builder() -> EnvironmentBuilder {
        EnvironmentBuilder(EnvironmentOptions::default())
    }

    async fn boostrap(options: EnvironmentOptions) -> RabbitMQStreamResult<Self> {
        Client::connect(options.client_options.clone()).await?;
        Ok(Environment { options })
    }

    pub fn stream_creator(&self) -> StreamCreator {
        StreamCreator::new(self.clone())
    }

    pub(crate) async fn create_client(&self) -> RabbitMQStreamResult<Client> {
        Client::connect(self.options.client_options.clone()).await
    }

    pub async fn delete_stream(&self, stream: &str) -> Result<(), StreamDeleteError> {
        let response = self.create_client().await?.delete_stream(stream).await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamDeleteError::DeleteError {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }
}

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
}
#[derive(Clone)]
pub struct EnvironmentOptions {
    pub(crate) client_options: ClientOptions,
}

impl Default for EnvironmentOptions {
    fn default() -> Self {
        Self {
            client_options: ClientOptions::default(),
        }
    }
}
