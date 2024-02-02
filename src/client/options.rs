use std::{fmt::Debug, sync::Arc};

use super::metrics::{MetricsCollector, NopMetricsCollector};
use crate::environment::TlsConfiguration;

#[derive(Clone)]
pub struct ClientOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) v_host: String,
    pub(crate) heartbeat: u32,
    pub(crate) max_frame_size: u32,
    pub(crate) tls: TlsConfiguration,
    pub(crate) collector: Arc<dyn MetricsCollector>,
}

impl Debug for ClientOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password)
            .field("v_host", &self.v_host)
            .field("heartbeat", &self.heartbeat)
            .field("max_frame_size", &self.max_frame_size)
            .finish()
    }
}
impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            host: "localhost".to_owned(),
            port: 5552,
            user: "guest".to_owned(),
            password: "guest".to_owned(),
            v_host: "/".to_owned(),
            heartbeat: 60,
            max_frame_size: 1048576,
            collector: Arc::new(NopMetricsCollector {}),
            tls: TlsConfiguration {
                enabled: false,
                trust_certificates: false,
                root_certificates_path: String::from(""),
                client_certificates_path: String::from(""),
                client_keys_path: String::from(""),
            },
        }
    }
}

impl ClientOptions {
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder(ClientOptions::default())
    }

    pub fn get_tls(&self) -> TlsConfiguration {
        self.tls.clone()
    }

    pub fn enable_tls(&mut self) {
        self.tls.enable(true);
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}

pub struct ClientOptionsBuilder(ClientOptions);

impl ClientOptionsBuilder {
    pub fn host(mut self, host: &str) -> Self {
        self.0.host = host.to_owned();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.0.port = port;
        self
    }

    pub fn user(mut self, user: &str) -> Self {
        self.0.user = user.to_owned();
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.0.password = password.to_owned();
        self
    }

    pub fn v_host(mut self, v_host: &str) -> Self {
        self.0.v_host = v_host.to_owned();
        self
    }

    pub fn heartbeat(mut self, heartbeat: u32) -> Self {
        self.0.heartbeat = heartbeat;
        self
    }

    pub fn max_frame_size(mut self, max_frame_size: u32) -> Self {
        self.0.max_frame_size = max_frame_size;
        self
    }

    pub fn tls(mut self, tls: TlsConfiguration) -> Self {
        self.0.tls = tls;
        self
    }

    pub fn collector(mut self, collector: Arc<dyn MetricsCollector>) -> Self {
        self.0.collector = collector;
        self
    }

    pub fn build(self) -> ClientOptions {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientOptions, NopMetricsCollector, TlsConfiguration};
    use std::sync::Arc;

    #[test]
    fn test_client_options_builder() {
        let options = ClientOptions::builder()
            .host("test")
            .port(8888)
            .user("test_user")
            .password("test_pass")
            .v_host("/test_vhost")
            .heartbeat(10000)
            .max_frame_size(1)
            .tls(TlsConfiguration {
                enabled: true,
                trust_certificates: false,
                root_certificates_path: String::from(""),
                client_certificates_path: String::from(""),
                client_keys_path: String::from(""),
            })
            .collector(Arc::new(NopMetricsCollector {}))
            .build();
        assert_eq!(options.host, "test");
        assert_eq!(options.port, 8888);
        assert_eq!(options.user, "test_user");
        assert_eq!(options.password, "test_pass");
        assert_eq!(options.v_host, "/test_vhost");
        assert_eq!(options.heartbeat, 10000);
        assert_eq!(options.max_frame_size, 1);
        assert_eq!(options.tls.enabled, true);
    }
}
