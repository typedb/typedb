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
use resource::constants::{common::PERMISSION_BITS_ALL, server::ADMIN_SOCKET_FILE_MODE};
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
    let mode = metadata.permissions().mode() & PERMISSION_BITS_ALL;
    if mode != ADMIN_SOCKET_FILE_MODE {
        return Err(AdminError::SocketPermissionsUnexpected {
            path: path.display().to_string(),
            mode,
            expected: ADMIN_SOCKET_FILE_MODE,
        });
    }
    Ok(())
}

pub(super) async fn connect(endpoint: Endpoint, path: &Path) -> Result<Channel, AdminError> {
    let connector_path = path.to_path_buf();
    let error_path = path.display().to_string();
    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            // tonic 0.12 uses hyper 1.x I/O traits; tokio's UnixStream doesn't implement
            // them directly, so TokioIo bridges the two trait surfaces.
            let path = connector_path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await
        .map_err(|source| AdminError::ConnectionFailed { address: error_path, source: Arc::new(source) })
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::net::UnixListener, path::PathBuf};

    use test_utils::{TempDir, create_tmp_dir};

    use super::*;

    fn temp_dir() -> TempDir {
        create_tmp_dir("admin_verify")
    }

    fn bind_socket_with_mode(dir: &TempDir, name: &str, mode: u32) -> (PathBuf, UnixListener) {
        let path = dir.join(name);
        let listener = UnixListener::bind(&path).expect("bind UDS");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("chmod UDS");
        (path, listener)
    }

    #[test]
    fn rejects_missing_path() {
        let dir = temp_dir();
        let path = dir.join("missing.sock");
        match verify_endpoint(&path) {
            Err(AdminError::SocketPathInaccessible { .. }) => {}
            other => panic!("expected SocketPathInaccessible, got {other:?}"),
        }
    }

    #[test]
    fn rejects_regular_file() {
        let dir = temp_dir();
        let path = dir.join("regular");
        fs::write(&path, b"not a socket").unwrap();
        match verify_endpoint(&path) {
            Err(AdminError::SocketNotASocket { .. }) => {}
            other => panic!("expected SocketNotASocket, got {other:?}"),
        }
    }

    #[test]
    fn rejects_directory() {
        let dir = temp_dir();
        let path = dir.join("nested");
        fs::create_dir(&path).unwrap();
        match verify_endpoint(&path) {
            Err(AdminError::SocketNotASocket { .. }) => {}
            other => panic!("expected SocketNotASocket, got {other:?}"),
        }
    }

    #[test]
    fn accepts_socket_mode_0o600() {
        let dir = temp_dir();
        let (path, _listener) = bind_socket_with_mode(&dir, "ok.sock", 0o600);
        verify_endpoint(&path).expect("0o600 should be accepted");
    }

    #[test]
    fn rejects_socket_mode_0o400() {
        let dir = temp_dir();
        let (path, _listener) = bind_socket_with_mode(&dir, "narrow.sock", 0o400);
        match verify_endpoint(&path) {
            Err(AdminError::SocketPermissionsUnexpected { mode, .. }) => assert_eq!(mode, 0o400),
            other => panic!("expected SocketPermissionsUnexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_socket_mode_0o644() {
        let dir = temp_dir();
        let (path, _listener) = bind_socket_with_mode(&dir, "bad.sock", 0o644);
        match verify_endpoint(&path) {
            Err(AdminError::SocketPermissionsUnexpected { mode, .. }) => assert_eq!(mode, 0o644),
            other => panic!("expected SocketPermissionsUnexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_socket_mode_0o660() {
        let dir = temp_dir();
        let (path, _listener) = bind_socket_with_mode(&dir, "bad.sock", 0o660);
        match verify_endpoint(&path) {
            Err(AdminError::SocketPermissionsUnexpected { mode, .. }) => assert_eq!(mode, 0o660),
            other => panic!("expected SocketPermissionsUnexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_socket_mode_0o700() {
        let dir = temp_dir();
        let (path, _listener) = bind_socket_with_mode(&dir, "bad.sock", 0o700);
        match verify_endpoint(&path) {
            Err(AdminError::SocketPermissionsUnexpected { mode, .. }) => assert_eq!(mode, 0o700),
            other => panic!("expected SocketPermissionsUnexpected, got {other:?}"),
        }
    }
}
