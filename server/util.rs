use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::lookup_host;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use crate::error::ServerOpenError;
use crate::parameters::config::EncryptionConfig;

pub fn configure_tonic_tls_config(encryption_config: &EncryptionConfig) -> Result<ServerTlsConfig, ServerOpenError> {
    let cert_path = encryption_config.cert.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificate {})?;
    let cert = fs::read_to_string(cert_path).map_err(|source| ServerOpenError::CouldNotReadTLSCertificate {
        path: cert_path.display().to_string(),
        source: Arc::new(source),
    })?;
    let cert_key_path =
        encryption_config.cert_key.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificateKey {})?;
    let cert_key =
        fs::read_to_string(cert_key_path).map_err(|source| ServerOpenError::CouldNotReadTLSCertificateKey {
            path: cert_key_path.display().to_string(),
            source: Arc::new(source),
        })?;
    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, cert_key));

    if let Some(root_ca_path) = &encryption_config.root_ca {
        let root_ca = fs::read_to_string(root_ca_path).map_err(|source| ServerOpenError::CouldNotReadRootCA {
            path: root_ca_path.display().to_string(),
            source: Arc::new(source),
        })?;
        tls_config = tls_config.client_ca_root(Certificate::from_pem(root_ca)).client_auth_optional(true);
    }
    Ok(tls_config)
}

pub async fn resolve_address(address: String) -> SocketAddr {
    lookup_host(address.clone())
        .await
        .unwrap()
        .next()
        .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
}