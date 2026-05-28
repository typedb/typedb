/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::Path,
    sync::Arc,
};

use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::error::AdminError;

pub(super) fn verify_endpoint(path: &Path) -> Result<(), AdminError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| AdminError::SocketPathInaccessible {
        path: path.display().to_string(),
        source: Arc::new(source),
    })?;
    if !metadata.file_type().is_socket() {
        return Err(AdminError::SocketNotASocket { path: path.display().to_string() });
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(AdminError::SocketPermissionsTooWide { path: path.display().to_string(), mode });
    }
    Ok(())
}

pub(super) async fn connect(endpoint: Endpoint, path: &Path) -> Result<Channel, AdminError> {
    let connector_path = path.to_path_buf();
    let error_path = path.display().to_string();
    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            // tonic 0.12 uses hyper 1.x I/O traits, which tokio's UnixStream doesn't implement
            // directly. TokioIo bridges the two trait surfaces. Could be updated later
            let path = connector_path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await
        .map_err(|source| AdminError::ConnectionFailed { address: error_path, source: Arc::new(source) })
}
