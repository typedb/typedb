/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use itertools::Itertools;
pub(crate) use tokio_rustls::rustls::ServerConfig as HttpTlsConfig;
use tokio_rustls::rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore,
};

use crate::{error::ServerOpenError, parameters::config::EncryptionConfig};

pub(crate) fn prepare_tls_config(
    encryption_config: &EncryptionConfig,
) -> Result<Option<HttpTlsConfig>, ServerOpenError> {
    if !encryption_config.enabled {
        return Ok(None);
    }

    let cert_path = encryption_config.cert.as_ref().ok_or(ServerOpenError::MissingTLSCertificate {})?;
    let cert_iter = CertificateDer::pem_file_iter(cert_path.as_path()).map_err(|source| {
        ServerOpenError::HttpCouldNotReadTlsCertificate {
            path: cert_path.display().to_string(),
            source: Arc::new(source),
        }
    })?;
    let certs: Vec<_> = cert_iter.try_collect().map_err(|source| ServerOpenError::HttpCouldNotReadTlsCertificate {
        path: cert_path.display().to_string(),
        source: Arc::new(source),
    })?;

    let cert_key_path = encryption_config.cert_key.as_ref().ok_or(ServerOpenError::MissingTLSCertificateKey {})?;
    let key = PrivateKeyDer::from_pem_file(cert_key_path.as_path()).map_err(|source| {
        ServerOpenError::HttpCouldNotReadTlsCertificateKey {
            path: cert_key_path.display().to_string(),
            source: Arc::new(source),
        }
    })?;

    let client_cert_verifier = match &encryption_config.root_ca {
        Some(root_ca_path) => {
            let mut client_auth_roots = RootCertStore::empty();
            let mut root_ca_iter = CertificateDer::pem_file_iter(root_ca_path.as_path()).map_err(|source| {
                ServerOpenError::HttpCouldNotReadRootCa {
                    path: root_ca_path.display().to_string(),
                    source: Arc::new(source),
                }
            })?;
            while let Some(root_ca) = root_ca_iter.next() {
                client_auth_roots
                    .add(root_ca.map_err(|source| ServerOpenError::HttpTlsPemFileError { source: Arc::new(source) })?)
                    .map_err(|source| ServerOpenError::HttpTlsFailedConfiguration { source: Arc::new(source) })?;
            }
            Some(
                WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                    .build()
                    .map_err(|source| ServerOpenError::HttpInvalidRootCa { source: Arc::new(source) })?,
            )
        }
        None => None,
    };

    let config_builder = match client_cert_verifier {
        Some(client_cert_verifier) => HttpTlsConfig::builder().with_client_cert_verifier(client_cert_verifier),
        None => HttpTlsConfig::builder().with_no_client_auth(),
    };

    let config = config_builder
        .with_single_cert(certs, key)
        .map_err(|source| ServerOpenError::HttpTlsFailedConfiguration { source: Arc::new(source) })?;

    Ok(Some(config))
}
