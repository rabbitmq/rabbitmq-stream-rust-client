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
                hostname_verification: false,
                trust_everything: false,
            },
        }
    }
}

impl ClientOptions {
    pub fn get_tls(&self) -> TlsConfiguration {
        self.tls
    }

    pub fn enable_tls(&mut self) {
        self.tls.enable(true);
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}
