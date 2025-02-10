use std::{convert::TryFrom, fmt::Debug, path::PathBuf, sync::Arc};

use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer},
        ClientConfig,
    },
    TlsConnector,
};

use crate::error::ClientError;

use super::{
    metrics::{MetricsCollector, NopMetricsCollector},
    GenericTcpStream,
};

#[derive(Clone)]
pub struct ClientOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) v_host: String,
    pub(crate) heartbeat: u32,
    pub(crate) max_frame_size: u32,
    pub(crate) load_balancer_mode: bool,
    pub(crate) tls: TlsConfiguration,
    pub(crate) collector: Arc<dyn MetricsCollector>,
    pub(crate) client_provided_name: String,
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
            .field("client_provided_name", &self.client_provided_name)
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
            load_balancer_mode: false,
            collector: Arc::new(NopMetricsCollector {}),
            tls: Default::default(),
            client_provided_name: String::from("rust-stream"),
        }
    }
}

impl ClientOptions {
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder(ClientOptions::default())
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    pub fn set_client_provided_name(&mut self, name: &str) {
        self.client_provided_name = name.to_owned();
    }

    pub(crate) async fn build_generic_tcp_stream(&self) -> Result<GenericTcpStream, ClientError> {
        async fn create_tls_connection(
            host: String,
            port: u16,
            config: ClientConfig,
        ) -> Result<GenericTcpStream, ClientError> {
            let stream = TcpStream::connect((host.clone(), port)).await?;
            let domain = ServerName::try_from(host.clone()).unwrap();
            let connector = TlsConnector::from(Arc::new(config));
            let conn = connector.connect(domain, stream).await?;
            Ok(GenericTcpStream::SecureTcp(conn))
        }
        match &self.tls {
            TlsConfiguration::Trusted {
                root_certificates_path,
                client_certificates,
            } => {
                let roots = build_root_store(root_certificates_path).await?;

                let builder = ClientConfig::builder().with_root_certificates(roots);
                let config = match client_certificates {
                    Some(client_certificates) => {
                        let client_certs = match build_client_certificates(
                            &client_certificates.client_certificates_path,
                        ) {
                            Ok(certs) => certs,
                            Err(e) => return Err(ClientError::Io(e)),
                        };
                        let client_keys = match build_client_private_keys(
                            &client_certificates.client_private_key_path,
                        ) {
                            Ok(keys) => keys,
                            Err(e) => return Err(ClientError::Io(e)),
                        };
                        match builder.with_client_auth_cert(
                            client_certs,
                            client_keys.into_iter().next().unwrap(),
                        ) {
                            Ok(config) => config,
                            Err(e) => return Err(ClientError::GenericError(Box::new(e))),
                        }
                    }
                    None => builder.with_no_client_auth(),
                };
                create_tls_connection(self.host.clone(), self.port, config).await
            }
            TlsConfiguration::Untrusted => {
                let config: ClientConfig = build_tls_client_configuration_untrusted().await?;
                create_tls_connection(self.host.clone(), self.port, config).await
            }
            TlsConfiguration::Disabled => {
                let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
                Ok(GenericTcpStream::Tcp(stream))
            }
        }
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

    pub fn load_balancer_mode(mut self, load_balancer_mode: bool) -> Self {
        self.0.load_balancer_mode = load_balancer_mode;
        self
    }

    pub fn build(self) -> ClientOptions {
        self.0
    }
}

/** Helper for tls configuration */
#[derive(Clone)]
pub enum TlsConfiguration {
    Disabled,
    Untrusted,
    Trusted {
        root_certificates_path: PathBuf,
        client_certificates: Option<ClientTlsConfiguration>,
    },
}
impl Default for TlsConfiguration {
    fn default() -> Self {
        TlsConfiguration::Disabled
    }
}

#[derive(Clone)]
pub struct ClientTlsConfiguration {
    pub(crate) client_certificates_path: PathBuf,
    pub(crate) client_private_key_path: PathBuf,
}

impl TlsConfiguration {
    pub fn builder() -> TlsConfigurationBuilder {
        TlsConfigurationBuilder {
            enabled: false,
            root_certificates_path: None,
            client_certificates_path: None,
            client_private_key_path: None,
        }
    }
}

pub struct TlsConfigurationBuilder {
    enabled: bool,
    root_certificates_path: Option<PathBuf>,
    client_certificates_path: Option<PathBuf>,
    client_private_key_path: Option<PathBuf>,
}
impl Default for TlsConfigurationBuilder {
    fn default() -> Self {
        TlsConfigurationBuilder {
            enabled: false,
            root_certificates_path: None,
            client_certificates_path: None,
            client_private_key_path: None,
        }
    }
}

impl TlsConfigurationBuilder {
    pub fn enable(mut self, enabled: bool) -> TlsConfigurationBuilder {
        self.enabled = enabled;
        self
    }

    pub fn add_root_certificates<T>(mut self, root_certificates_path: T) -> TlsConfigurationBuilder
    where
        T: Into<PathBuf>,
    {
        self.root_certificates_path = Some(root_certificates_path.into());
        self
    }

    pub fn add_client_certificates_keys<T1, T2>(
        mut self,
        client_certificates_path: T1,
        client_private_key_path: T2,
    ) -> TlsConfigurationBuilder
    where
        T1: Into<PathBuf>,
        T2: Into<PathBuf>,
    {
        self.client_certificates_path = Some(client_certificates_path.into());
        self.client_private_key_path = Some(client_private_key_path.into());
        self
    }

    pub fn build(self) -> Result<TlsConfiguration, &'static str> {
        if self.enabled {
            let root_certificates_path = match self.root_certificates_path {
                Some(root_certificates_path) => root_certificates_path,
                None => {
                    if self.client_certificates_path.is_some()
                        || self.client_private_key_path.is_some()
                    {
                        return Err("Root certificates path is required when client certificates are provided");
                    }
                    return Ok(TlsConfiguration::Untrusted);
                }
            };

            let client_certificates =
                match (self.client_certificates_path, self.client_private_key_path) {
                    (Some(client_certificates_path), Some(client_private_key_path)) => {
                        Some(ClientTlsConfiguration {
                            client_certificates_path,
                            client_private_key_path,
                        })
                    }
                    (None, None) => None,
                    // This state is unreachable because the properties are set together
                    _ => unreachable!("Unreachable state"),
                };

            Ok(TlsConfiguration::Trusted {
                root_certificates_path,
                client_certificates,
            })
        } else {
            Ok(TlsConfiguration::Disabled)
        }
    }
}

fn build_client_certificates(
    client_cert: &PathBuf,
) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(client_cert)?;
    let mut pem = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut pem)
        .map(|c| c.map(CertificateDer::into_owned))
        .collect()
}

async fn build_tls_client_configuration_untrusted() -> Result<ClientConfig, ClientError> {
    mod danger {
        use rustls::client::danger::HandshakeSignatureValid;
        use rustls::client::danger::ServerCertVerified;
        use tokio_rustls::rustls::{
            self, client::danger::ServerCertVerifier, pki_types::ServerName,
        };

        #[derive(Debug)]
        pub struct NoCertificateVerification {}

        impl ServerCertVerifier for NoCertificateVerification {
            fn verify_tls12_signature(
                &self,
                _: &[u8],
                _: &rustls::pki_types::CertificateDer<'_>,
                _: &rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _: &[u8],
                _: &rustls::pki_types::CertificateDer<'_>,
                _: &rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                use rustls::SignatureScheme;
                // I know know if this is correct
                vec![
                    SignatureScheme::RSA_PKCS1_SHA1,
                    SignatureScheme::ECDSA_SHA1_Legacy,
                    SignatureScheme::RSA_PKCS1_SHA256,
                    SignatureScheme::ECDSA_NISTP256_SHA256,
                    SignatureScheme::RSA_PKCS1_SHA384,
                    SignatureScheme::ECDSA_NISTP384_SHA384,
                    SignatureScheme::RSA_PKCS1_SHA512,
                    SignatureScheme::ECDSA_NISTP521_SHA512,
                    SignatureScheme::RSA_PSS_SHA256,
                    SignatureScheme::RSA_PSS_SHA384,
                    SignatureScheme::RSA_PSS_SHA512,
                    SignatureScheme::ED25519,
                    SignatureScheme::ED448,
                ]
            }

            fn verify_server_cert(
                &self,
                _: &rustls::pki_types::CertificateDer<'_>,
                _: &[rustls::pki_types::CertificateDer<'_>],
                _: &ServerName<'_>,
                _: &[u8],
                _: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(ServerCertVerified::assertion())
            }
        }
    }

    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification {}))
        .with_no_client_auth();

    Ok(config)
}

async fn build_root_store(root_ca_cert: &PathBuf) -> std::io::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    let cert_bytes = std::fs::read(root_ca_cert)?;

    let root_cert_store: Result<Vec<_>, _> =
        rustls_pemfile::certs(&mut cert_bytes.as_ref()).collect();
    let root_cert_store = root_cert_store?;

    root_cert_store
        .into_iter()
        .for_each(|cert| roots.add(cert).unwrap());
    Ok(roots)
}

fn build_client_private_keys(
    client_private_key: &PathBuf,
) -> std::io::Result<Vec<PrivateKeyDer<'static>>> {
    let file = std::fs::File::open(client_private_key)?;
    let mut pem = std::io::BufReader::new(file);
    let keys: Result<Vec<_>, _> = rustls_pemfile::pkcs8_private_keys(&mut pem).collect();
    let keys = keys?;
    let keys = keys.into_iter().map(PrivateKeyDer::from).collect();
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::Path, sync::Arc};

    #[test]
    fn test_tls_builder() {
        let tls = TlsConfiguration::builder().build().unwrap();
        assert!(matches!(tls, TlsConfiguration::Disabled));
        let tls = TlsConfiguration::builder().enable(true).build().unwrap();
        assert!(matches!(tls, TlsConfiguration::Untrusted));

        let tls = TlsConfiguration::builder()
            .enable(true)
            .add_root_certificates("test")
            .build()
            .unwrap();
        let TlsConfiguration::Trusted {
            root_certificates_path,
            client_certificates,
        } = tls
        else {
            panic!("Expected Trusted configuration")
        };
        assert_eq!(root_certificates_path.as_path(), Path::new("test"));
        assert!(client_certificates.is_none());

        let tls = TlsConfiguration::builder()
            .enable(true)
            .add_root_certificates("test")
            .add_client_certificates_keys("cert", "priv")
            .build()
            .unwrap();
        let TlsConfiguration::Trusted {
            root_certificates_path,
            client_certificates,
        } = tls
        else {
            panic!("Expected Trusted configuration")
        };
        assert_eq!(root_certificates_path.as_path(), Path::new("test"));
        let Some(client_certificates) = client_certificates else {
            panic!("Expected client certificates")
        };
        assert_eq!(
            client_certificates.client_certificates_path,
            Path::new("cert")
        );
        assert_eq!(
            client_certificates.client_private_key_path,
            Path::new("priv")
        );

        let tls = TlsConfiguration::builder()
            .enable(true)
            // .add_root_certificates("test")
            .add_client_certificates_keys("cert", "priv")
            .build();
        assert!(tls.is_err());
    }

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
            .tls(TlsConfiguration::builder().enable(true).build().unwrap())
            .collector(Arc::new(NopMetricsCollector {}))
            .load_balancer_mode(true)
            .build();
        assert_eq!(options.host, "test");
        assert_eq!(options.port, 8888);
        assert_eq!(options.user, "test_user");
        assert_eq!(options.password, "test_pass");
        assert_eq!(options.v_host, "/test_vhost");
        assert_eq!(options.heartbeat, 10000);
        assert_eq!(options.max_frame_size, 1);
        assert!(matches!(options.tls, TlsConfiguration::Untrusted));
        assert_eq!(options.load_balancer_mode, true);
    }
}
