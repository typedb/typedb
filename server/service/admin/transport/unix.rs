/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Unix domain socket transport for the admin endpoint.
//!
//! Trust anchor: the socket file's mode bits, kernel-enforced via the standard
//! `inode_permission` check on `connect(2)`. The server binds the socket as `0600`
//! owned by the typedb service account.

use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use resource::constants::server::{ADMIN_SOCKET_FILE_MODE, ADMIN_SOCKET_FILE_NON_OWNER_BITS};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{info, warn};

use crate::error::ServerOpenError;

pub type AdminPath = PathBuf;

pub type AdminConnection = tokio::net::UnixStream;

pub struct AdminListener {
    inner: UnixListener,
    path: PathBuf,
}

impl AdminListener {
    pub fn into_incoming(self) -> UnixListenerStream {
        UnixListenerStream::new(self.inner)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn bind_admin_endpoint(path: &Path) -> Result<AdminListener, ServerOpenError> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|source| ServerOpenError::AdminSocketBind {
                path: path.to_string_lossy().into_owned(),
                source: Arc::new(source),
            })?;
        }
    }

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if metadata.file_type().is_socket() {
                fs::remove_file(path).map_err(|source| ServerOpenError::AdminSocketCleanup {
                    path: path.to_string_lossy().into_owned(),
                    source: Arc::new(source),
                })?;
            } else {
                return Err(ServerOpenError::AdminSocketPathInUse { path: path.to_string_lossy().into_owned() });
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ServerOpenError::AdminSocketBind {
                path: path.to_string_lossy().into_owned(),
                source: Arc::new(source),
            });
        }
    }

    // Bracket the bind with a restrictive umask so the socket file is created with
    // owner-only mode atomically. Otherwise there's a brief window where the socket
    // exists with whatever the operator's umask happens to permit. The subsequent
    // `set_permissions` is belt-and-braces.
    let listener = {
        let _restrictive_umask = ScopedUmask::new(ADMIN_SOCKET_FILE_NON_OWNER_BITS);
        UnixListener::bind(path)
    }
    .map_err(|source| ServerOpenError::AdminSocketBind {
        path: path.to_string_lossy().into_owned(),
        source: Arc::new(source),
    })?;

    fs::set_permissions(path, fs::Permissions::from_mode(ADMIN_SOCKET_FILE_MODE)).map_err(|source| {
        ServerOpenError::AdminSocketChmod { path: path.to_string_lossy().into_owned(), source: Arc::new(source) }
    })?;

    info!("Admin Unix socket bound at {} (mode {:#o})", path.display(), ADMIN_SOCKET_FILE_MODE);
    Ok(AdminListener { inner: listener, path: path.to_path_buf() })
}

// Best-effort: unlinks the UDS file on Unix, no-op on Windows (it has auto cleanups)
pub fn cleanup_admin_endpoint(path: &Path) {
    if let Err(err) = fs::remove_file(path) {
        if err.kind() != io::ErrorKind::NotFound {
            warn!("Could not remove admin socket file '{}' after shutdown: {err}", path.display());
        }
    }
}

fn set_process_umask(mask: u32) -> u32 {
    // SAFETY: `umask(2)` has no pointer args and no UB. It always succeeds
    unsafe extern "C" {
        fn umask(mask: u32) -> u32;
    }
    unsafe { umask(mask) }
}

/// RAII guard around the per-process umask. Kept around exactly one syscall so other
/// threads' file creations during the window inherit the (stricter) mask harmlessly.
struct ScopedUmask {
    previous: u32,
}

impl ScopedUmask {
    fn new(mask: u32) -> Self {
        Self { previous: set_process_umask(mask) }
    }
}

impl Drop for ScopedUmask {
    fn drop(&mut self) {
        set_process_umask(self.previous);
    }
}
