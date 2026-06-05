/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::Path, sync::Arc, time::Duration};

use hyper_util::rt::TokioIo;
use tokio::net::windows::named_pipe::ClientOptions;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::error::AdminError;

// Win32 `ERROR_PIPE_BUSY` (231) — all pipe instances are servicing other clients.
// https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-
const ERROR_PIPE_BUSY: i32 = 231;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_CONNECT_ATTEMPTS: u32 = 20;

/// Named Pipes don't have a filesystem inode we can `stat`, so unlike Unix we
/// can't pre-check the DACL. The kernel enforces it at `CreateFile` time and a wrong
/// DACL surfaces as ERROR_ACCESS_DENIED from the connect call.
pub(super) fn verify_endpoint(path: &Path) -> Result<(), AdminError> {
    let s = path.to_string_lossy();
    if !s.starts_with(r"\\.\pipe\") && !s.starts_with(r"\\?\pipe\") {
        return Err(AdminError::InvalidArgument {
            name: "socket-path".to_string(),
            reason: format!("the admin endpoint must be a Named Pipe path (starting with \\\\.\\pipe\\); got '{s}'"),
        });
    }
    Ok(())
}

pub(super) async fn connect(endpoint: Endpoint, path: &Path) -> Result<Channel, AdminError> {
    let connector_path = path.to_string_lossy().into_owned();
    let error_path = connector_path.clone();
    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = connector_path.clone();
            async move {
                let client = open_named_pipe(&path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(client))
            }
        }))
        .await
        .map_err(|source| AdminError::ConnectionFailed { address: error_path, source: Arc::new(source) })
}

async fn open_named_pipe(path: &str) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    for _ in 0..MAX_CONNECT_ATTEMPTS {
        match ClientOptions::new().open(path) {
            Ok(client) => return Ok(client),
            Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        format!("admin Named Pipe '{path}' was busy after {MAX_CONNECT_ATTEMPTS} attempts"),
    ))
}
