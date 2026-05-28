/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Unix domain socket transport for the admin endpoint.
//!
//! Trust anchor: the socket file's mode bits, enforced by the kernel via the standard
//! `inode_permission` check on `connect(2)`. The server binds the socket as 0600 owned
//! by the typedb service account, so only that account (and root) can dial it.

use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use resource::constants::server::ADMIN_SOCKET_FILE_MODE;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{info, warn};

use crate::error::ServerOpenError;

/// Endpoint address on Unix — a filesystem path to the socket file.
pub type AdminPath = PathBuf;

/// Server-side listener wrapping the bound socket.
pub struct AdminListener {
    inner: UnixListener,
    path: PathBuf,
}

/// A connected client — what tonic sees as an inbound connection.
pub type AdminConnection = tokio::net::UnixStream;

impl AdminListener {
    /// Convert into a [`Stream`] of inbound connections for `serve_with_incoming`.
    pub fn into_incoming(self) -> UnixListenerStream {
        UnixListenerStream::new(self.inner)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Bind the admin Unix domain socket with restrictive permissions.
///
/// The cleanup-then-umask-bind-chmod sequence is load-bearing:
///
/// 1. If the path already exists as a socket from a previous run, unlink it — `bind(2)`
///    would otherwise fail with EADDRINUSE.
/// 2. If the path exists but is *not* a socket (regular file, symlink, etc.), refuse:
///    overwriting an arbitrary file could be a foothold for a malicious local process.
/// 3. Set umask 0o077 so `bind(2)` creates the socket with at-most owner-only
///    permissions *atomically*. Restore umask. Without this, there's a window where
///    `bind` has run but `chmod` hasn't, and the socket exists with whatever umask
///    permitted (0755 on a default system, 0777 with umask 0).
/// 4. `chmod 0600` as belt-and-braces: normalises the mode regardless of the umask.
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

    let listener = {
        let _restrictive_umask = ScopedUmask::new(0o077);
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

/// Best-effort socket file removal on shutdown.
///
/// Failures are only logged — on a hard crash this never runs anyway, and the
/// stale-socket cleanup at bind time is the durable mechanism.
pub fn cleanup_admin_endpoint(path: &Path) {
    if let Err(err) = fs::remove_file(path) {
        if err.kind() != io::ErrorKind::NotFound {
            warn!("Could not remove admin socket file '{}' after shutdown: {err}", path.display());
        }
    }
}

/// Set the process file-creation umask and return the previous value.
fn set_process_umask(mask: u32) -> u32 {
    // SAFETY: `umask(2)` is a trivial libc function with no pointer arguments and no UB.
    // It always succeeds and returns the previous umask.
    unsafe extern "C" {
        fn umask(mask: u32) -> u32;
    }
    unsafe { umask(mask) }
}

/// RAII guard that sets the process umask on construction and restores it on drop.
///
/// Used to bracket a single `bind(2)` call so the socket file is created atomically
/// with owner-only permissions. The bracketed region must be as short as possible —
/// umask is a per-process global, and other threads creating files during the window
/// inherit the guard's mask. For our usage that window is microseconds and any side
/// effect can only narrow permissions, never widen them.
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
