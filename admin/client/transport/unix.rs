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
    if mode & !ADMIN_SOCKET_FILE_MODE != 0 {
        return Err(AdminError::SocketPermissionsTooWide { path: path.display().to_string(), mode });
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
    use std::{
        fs,
        os::unix::net::UnixListener,
        path::PathBuf,
        sync::atomic::{AtomicU32, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    struct TempPath(PathBuf);

    impl TempPath {
        fn new(suffix: &str) -> Self {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let path = std::env::temp_dir().join(format!(
                "typedb-admin-verify-{}-{}-{}-{}",
                std::process::id(),
                nanos,
                n,
                suffix
            ));
            let _ = fs::remove_file(&path);
            let _ = fs::remove_dir_all(&path);
            Self(path)
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn bind_socket_with_mode(suffix: &str, mode: u32) -> (TempPath, UnixListener) {
        let temp = TempPath::new(suffix);
        let listener = UnixListener::bind(&temp.0).expect("bind UDS");
        fs::set_permissions(&temp.0, fs::Permissions::from_mode(mode)).expect("chmod UDS");
        (temp, listener)
    }

    #[test]
    fn rejects_missing_path() {
        let temp = TempPath::new("missing.sock");
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketPathInaccessible { .. }) => {}
            other => panic!("expected SocketPathInaccessible, got {other:?}"),
        }
    }

    #[test]
    fn rejects_regular_file() {
        let temp = TempPath::new("regular");
        fs::write(&temp.0, b"not a socket").unwrap();
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketNotASocket { .. }) => {}
            other => panic!("expected SocketNotASocket, got {other:?}"),
        }
    }

    #[test]
    fn rejects_directory() {
        let temp = TempPath::new("dir");
        fs::create_dir(&temp.0).unwrap();
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketNotASocket { .. }) => {}
            other => panic!("expected SocketNotASocket, got {other:?}"),
        }
    }

    #[test]
    fn accepts_socket_mode_0o600() {
        let (temp, _listener) = bind_socket_with_mode("ok600", 0o600);
        verify_endpoint(&temp.0).expect("0o600 should be accepted");
    }

    #[test]
    fn accepts_socket_mode_0o400() {
        let (temp, _listener) = bind_socket_with_mode("ok400", 0o400);
        verify_endpoint(&temp.0).expect("0o400 has no extra bits and should be accepted");
    }

    #[test]
    fn rejects_socket_mode_0o644() {
        let (temp, _listener) = bind_socket_with_mode("bad644", 0o644);
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketPermissionsTooWide { mode, .. }) => assert_eq!(mode, 0o644),
            other => panic!("expected SocketPermissionsTooWide, got {other:?}"),
        }
    }

    #[test]
    fn rejects_socket_mode_0o660() {
        let (temp, _listener) = bind_socket_with_mode("bad660", 0o660);
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketPermissionsTooWide { mode, .. }) => assert_eq!(mode, 0o660),
            other => panic!("expected SocketPermissionsTooWide, got {other:?}"),
        }
    }

    #[test]
    fn rejects_socket_mode_0o700() {
        let (temp, _listener) = bind_socket_with_mode("bad700", 0o700);
        match verify_endpoint(&temp.0) {
            Err(AdminError::SocketPermissionsTooWide { mode, .. }) => assert_eq!(mode, 0o700),
            other => panic!("expected SocketPermissionsTooWide, got {other:?}"),
        }
    }
}
