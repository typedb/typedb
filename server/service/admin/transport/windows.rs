/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Windows Named Pipe transport for the admin endpoint.
//!
//! Trust anchor: a Windows DACL on the named pipe, set at creation time. The DACL
//! grants full access to the pipe's owner (the typedb service process owner),
//! BUILTIN\Administrators, and LOCAL SYSTEM; nothing else. The kernel enforces this
//! on every client `CreateFile` call against the pipe, equivalent to how it enforces
//! mode bits on a Unix domain socket.
//!
//! This is the same model MySQL, SQL Server and Oracle use for their local
//! privileged channels on Windows. The configuration knob is the pipe name (e.g.
//! `\\.\pipe\typedb-admin`), the analog of the Unix socket path.
//!
//! Implementation notes:
//! - Tokio's `ServerOptions::create_with_security_attributes_raw` accepts a raw
//!   `*mut SECURITY_ATTRIBUTES`, so we declare the struct ourselves and build the
//!   security descriptor via the Win32 `ConvertStringSecurityDescriptorToSecurityDescriptorW`
//!   from an SDDL string. This avoids a dependency on the `windows`/`windows-sys`
//!   crates.
//! - Named pipes are per-connection on the server: each `NamedPipeServer` handles
//!   exactly one client, and a new instance must be created for the next client. We
//!   spawn a small accept loop that creates the next instance before yielding the
//!   current one, so connections aren't dropped between clients.

use std::{
    ffi::c_void,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;
use tracing::{info, warn};

use crate::error::ServerOpenError;

/// Endpoint address on Windows — a pipe name like `\\.\pipe\typedb-admin`.
pub type AdminPath = String;

/// Server-side listener. Holds the channel of inbound connections and the pipe name
/// so the caller can convert to a stream and clean up on shutdown.
pub struct AdminListener {
    incoming: mpsc::Receiver<io::Result<AdminConnection>>,
    pipe_name: String,
}

impl AdminListener {
    pub fn into_incoming(self) -> ReceiverStream<io::Result<AdminConnection>> {
        ReceiverStream::new(self.incoming)
    }

    pub fn path(&self) -> &str {
        &self.pipe_name
    }
}

/// Bind the admin Named Pipe with a restrictive DACL.
///
/// SDDL `D:P(A;;GA;;;OW)(A;;GA;;;BA)(A;;GA;;;SY)` means:
/// - DACL is Protected (don't inherit ACEs from parent containers).
/// - Allow GenericAll to the OWner of the security descriptor (the process owner).
/// - Allow GenericAll to BUILTIN\Administrators (BA).
/// - Allow GenericAll to LOCAL SYSTEM (SY).
/// - No ACEs for any other principal — anyone else gets ERROR_ACCESS_DENIED on
///   `CreateFile`.
///
/// The first pipe instance is created with `first_pipe_instance(true)` so that
/// `CreateNamedPipe` fails if some other process has already created a pipe with this
/// name. This prevents a squatting attack where a malicious local process pre-creates
/// the pipe with permissive ACLs.
pub fn bind_admin_endpoint(pipe_name: &str) -> Result<AdminListener, ServerOpenError> {
    // Build the security descriptor once; subsequent instances reuse the same SDDL.
    let sd = SecurityDescriptor::from_sddl("D:P(A;;GA;;;OW)(A;;GA;;;BA)(A;;GA;;;SY)")
        .map_err(|source| ServerOpenError::AdminPipeBind { name: pipe_name.to_string(), source: Arc::new(source) })?;

    // Create the first pipe instance synchronously here so bind errors propagate.
    let first = create_pipe_instance(pipe_name, &sd, true)
        .map_err(|source| ServerOpenError::AdminPipeBind { name: pipe_name.to_string(), source: Arc::new(source) })?;

    info!("Admin Named Pipe bound at {} (DACL: owner + Administrators + SYSTEM)", pipe_name);

    let (tx, rx) = mpsc::channel(16);
    let pipe_name_for_task = pipe_name.to_string();
    let sd_for_task = sd; // SecurityDescriptor is not Send by default; wrap in Mutex or move

    tokio::spawn(async move {
        let mut next = Some(first);
        loop {
            // Take the pre-created server and wait for a client to connect to it.
            let server = match next.take() {
                Some(s) => s,
                None => match create_pipe_instance(&pipe_name_for_task, &sd_for_task, false) {
                    Ok(s) => s,
                    Err(err) => {
                        warn!("Failed to create next admin pipe instance: {err}");
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                },
            };

            if let Err(err) = server.connect().await {
                warn!("Admin pipe connect() failed: {err}");
                // Try again on next loop with a fresh instance.
                continue;
            }

            // Create the NEXT instance before yielding the current connection so
            // there's no window where a client could fail to find any listening pipe.
            next = match create_pipe_instance(&pipe_name_for_task, &sd_for_task, false) {
                Ok(s) => Some(s),
                Err(err) => {
                    warn!("Failed to create next admin pipe instance after accept: {err}");
                    // Fall through — still yield the current connection, but on the
                    // next iteration we'll try to create again.
                    None
                }
            };

            if tx.send(Ok(AdminConnection(server))).await.is_err() {
                // Receiver dropped → server is shutting down.
                break;
            }
        }
    });

    Ok(AdminListener { incoming: rx, pipe_name: pipe_name.to_string() })
}

/// No-op on Windows — Named Pipes are cleaned up by the kernel when the server
/// closes its handles. There's no filesystem path to unlink.
pub fn cleanup_admin_endpoint(_pipe_name: &str) {}

/// Create a single pipe instance with the configured security descriptor.
fn create_pipe_instance(name: &str, sd: &SecurityDescriptor, is_first: bool) -> io::Result<NamedPipeServer> {
    let mut sa = sd.as_security_attributes();
    let mut opts = ServerOptions::new();
    if is_first {
        opts.first_pipe_instance(true);
    }
    // SAFETY: `sa` points at a valid SECURITY_ATTRIBUTES with a valid
    // SECURITY_DESCRIPTOR owned by `sd`. Tokio passes it through to CreateNamedPipeW
    // without retaining the pointer past the call. We hold `sd` alive for the lifetime
    // of the server loop, so the descriptor is not freed while in use.
    unsafe { opts.create_with_security_attributes_raw(name, &mut sa as *mut _ as *mut _) }
}

/// Wrapper around a connected `NamedPipeServer` so we can implement tonic's
/// [`Connected`] trait on it (orphan rules prevent impl-ing it on the foreign type).
pub struct AdminConnection(NamedPipeServer);

impl AsyncRead for AdminConnection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        // SAFETY: structural pinning is fine — we never move the inner pipe out.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll_read(cx, buf)
    }
}

impl AsyncWrite for AdminConnection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}

#[derive(Clone, Debug)]
pub struct NamedPipeConnectInfo {}

impl Connected for AdminConnection {
    type ConnectInfo = NamedPipeConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        NamedPipeConnectInfo {}
    }
}

// ---------------------------------------------------------------------------
// Win32 FFI: security descriptor construction from SDDL.
// ---------------------------------------------------------------------------

#[repr(C)]
struct SECURITY_ATTRIBUTES {
    n_length: u32,
    lp_security_descriptor: *mut c_void,
    b_inherit_handle: i32,
}

const SDDL_REVISION_1: u32 = 1;

unsafe extern "system" {
    fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
        string_security_descriptor: *const u16,
        string_sd_revision: u32,
        security_descriptor: *mut *mut c_void,
        security_descriptor_size: *mut u32,
    ) -> i32;
    fn LocalFree(h_mem: *mut c_void) -> *mut c_void;
}

/// RAII wrapper around a security descriptor allocated by Win32.
/// Frees via `LocalFree` on drop.
///
/// The pointer is sent across threads via the spawned accept loop; security descriptors
/// are immutable once built, and Win32 explicitly allows them to be passed between
/// threads, so this is safe to mark `Send + Sync` despite holding a raw pointer.
struct SecurityDescriptor {
    ptr: *mut c_void,
}

// SAFETY: SECURITY_DESCRIPTOR contents are immutable after creation; the kernel
// duplicates them into pipe metadata on CreateNamedPipe. The pointer itself is just
// a heap address into LocalAlloc storage and is safe to share across threads.
unsafe impl Send for SecurityDescriptor {}
unsafe impl Sync for SecurityDescriptor {}

impl SecurityDescriptor {
    fn from_sddl(sddl: &str) -> io::Result<Self> {
        let wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();
        let mut sd: *mut c_void = std::ptr::null_mut();
        // SAFETY: `wide` is a null-terminated UTF-16 string. `&mut sd` is a valid
        // pointer to receive the output. Returning 0 indicates failure.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 || sd.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { ptr: sd })
    }

    fn as_security_attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            n_length: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lp_security_descriptor: self.ptr,
            b_inherit_handle: 0,
        }
    }
}

impl Drop for SecurityDescriptor {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `ptr` was returned by `ConvertStringSecurityDescriptorToSecurityDescriptorW`,
            // which documents LocalFree as the correct deallocator.
            unsafe {
                LocalFree(self.ptr);
            }
        }
    }
}
