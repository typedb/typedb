/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs, sync::Arc};

pub(crate) use tonic::transport::ServerTlsConfig as GrpcTlsConfig;
use tonic::transport::{Certificate, Identity};

use crate::{error::ServerOpenError, parameters::config::EncryptionConfig};

pub(crate) fn prepare_tls_config(
    encryption_config: &EncryptionConfig,
) -> Result<Option<GrpcTlsConfig>, ServerOpenError> {
    if !encryption_config.enabled {
        return Ok(None);
    }

    let cert_path = encryption_config.cert.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificate {})?;
    let cert = fs::read_to_string(cert_path).map_err(|source| ServerOpenError::GrpcCouldNotReadTlsCertificate {
        path: cert_path.display().to_string(),
        source: Arc::new(source),
    })?;

    let cert_key_path =
        encryption_config.cert_key.as_ref().ok_or_else(|| ServerOpenError::MissingTLSCertificateKey {})?;
    let cert_key =
        fs::read_to_string(cert_key_path).map_err(|source| ServerOpenError::GrpcCouldNotReadTlsCertificateKey {
            path: cert_key_path.display().to_string(),
            source: Arc::new(source),
        })?;

    let mut tls_config = GrpcTlsConfig::new().identity(Identity::from_pem(cert, cert_key));

    if let Some(root_ca_path) = &encryption_config.root_ca {
        let root_ca = fs::read_to_string(root_ca_path).map_err(|source| ServerOpenError::GrpcCouldNotReadRootCa {
            path: root_ca_path.display().to_string(),
            source: Arc::new(source),
        })?;
        tls_config = tls_config.client_ca_root(Certificate::from_pem(root_ca)).client_auth_optional(true);
    } else {
        tls_config = tls_config.client_auth_optional(false)
    }

    Ok(Some(tls_config))
}
