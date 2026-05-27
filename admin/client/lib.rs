/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod command;
pub mod commands;
pub mod error;
pub mod repl;

use std::{os::unix::fs::{FileTypeExt, PermissionsExt}, path::Path, sync::Arc};

use hyper_util::rt::TokioIo;
use server_admin_proto::type_db_admin_client::TypeDbAdminClient;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::error::AdminError;

pub type AdminClient = TypeDbAdminClient<Channel>;

pub async fn connect(socket_path: &Path) -> Result<AdminClient, AdminError> {
    let channel = connect_channel(socket_path).await?;
    Ok(TypeDbAdminClient::new(channel))
}

pub async fn connect_channel(socket_path: &Path) -> Result<Channel, AdminError> {
    verify_socket_file(socket_path)?;
    let socket_path = socket_path.to_path_buf();
    // tonic's Endpoint always needs a URI even for Unix connections; the URI is
    // never dialed because connect_with_connector overrides the transport with our
    // UnixStream factory. The "http://[::]" placeholder follows the tonic-examples
    // convention for UDS clients.
    let endpoint = Endpoint::try_from("http://[::]").map_err(|source| AdminError::ConnectionFailed {
        address: socket_path.display().to_string(),
        source: Arc::new(source),
    })?;
    let socket_path_for_connector = socket_path.clone();
    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            // tonic 0.12 uses hyper 1.x I/O traits, which tokio's UnixStream doesn't
            // implement directly. TokioIo bridges the two trait surfaces.
            let socket_path = socket_path_for_connector.clone();
            async move { UnixStream::connect(socket_path).await.map(TokioIo::new) }
        }))
        .await
        .map_err(|source| AdminError::ConnectionFailed {
            address: socket_path.display().to_string(),
            source: Arc::new(source),
        })
}

/// Verify the path is a Unix socket with owner-only permissions before connecting.
///
/// The kernel would already refuse a `connect(2)` from a UID that can't `open(2)` the
/// socket file, so this check is not the load-bearing security fence — that's the file
/// permissions enforced by the server at bind time. What this catches is the case where
/// somebody (an operator, a misbehaving script, a buggy installer) loosened the socket's
/// mode after the server wrote it: the kernel would still let the call through, but the
/// trust set has widened beyond what the server intended. We make the client refuse so
/// the operator notices and fixes it rather than silently using a wide-open admin
/// channel. Mirrors PostgreSQL's `~/.pgpass` permissions check.
fn verify_socket_file(path: &Path) -> Result<(), AdminError> {
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

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::net::UnixListener};

    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("typedb-admin-uds-test-{}-{name}", std::process::id()));
        let _ = fs::remove_file(&dir);
        dir
    }

    fn make_socket(path: &Path, mode: u32) {
        let _ = fs::remove_file(path);
        UnixListener::bind(path).expect("bind");
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("chmod");
    }

    #[test]
    fn accepts_owner_only_socket() {
        let path = temp_path("accept");
        make_socket(&path, 0o600);
        verify_socket_file(&path).expect("0600 socket must be accepted");
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_group_readable_socket() {
        let path = temp_path("group");
        make_socket(&path, 0o660);
        let err = verify_socket_file(&path).expect_err("0660 socket must be rejected");
        match err {
            AdminError::SocketPermissionsTooWide { mode, .. } => assert_eq!(mode, 0o660),
            other => panic!("expected SocketPermissionsTooWide, got {other:?}"),
        }
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_world_readable_socket() {
        let path = temp_path("world");
        make_socket(&path, 0o666);
        let err = verify_socket_file(&path).expect_err("0666 socket must be rejected");
        match err {
            AdminError::SocketPermissionsTooWide { mode, .. } => assert_eq!(mode, 0o666),
            other => panic!("expected SocketPermissionsTooWide, got {other:?}"),
        }
        fs::remove_file(&path).ok();
    }

    #[test]
    fn rejects_regular_file() {
        let path = temp_path("regular");
        let _ = fs::remove_file(&path);
        fs::write(&path, "not a socket").expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod");
        let err = verify_socket_file(&path).expect_err("regular file must be rejected");
        match err {
            AdminError::SocketNotASocket { .. } => {}
            other => panic!("expected SocketNotASocket, got {other:?}"),
        }
        fs::remove_file(&path).ok();
    }
}
