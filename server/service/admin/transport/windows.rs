/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Windows Named Pipe transport for the admin endpoint.
//!
//! Trust anchor: a Windows DACL on the named pipe, set at creation time. Grants full
//! access to the pipe owner (typedb process), `BUILTIN\Administrators`, and
//! `LOCAL SYSTEM`; everyone else gets `ERROR_ACCESS_DENIED` from `CreateFile`.

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

pub type AdminPath = String;

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

// SDDL: protected DACL granting GenericAll to Owner (OW), BUILTIN\Administrators (BA),
// LOCAL SYSTEM (SY).
// https://learn.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-string-format
const ADMIN_PIPE_SDDL: &str = "D:P(A;;GA;;;OW)(A;;GA;;;BA)(A;;GA;;;SY)";

pub fn bind_admin_endpoint(pipe_name: &str) -> Result<AdminListener, ServerOpenError> {
    let sd = SecurityDescriptor::from_sddl(ADMIN_PIPE_SDDL)
        .map_err(|source| ServerOpenError::AdminPipeBind { name: pipe_name.to_string(), source: Arc::new(source) })?;

    let first = create_pipe_instance(pipe_name, &sd, true)
        .map_err(|source| ServerOpenError::AdminPipeBind { name: pipe_name.to_string(), source: Arc::new(source) })?;
    info!("Admin Named Pipe bound at {} (DACL: owner + Administrators + SYSTEM)", pipe_name);

    let (tx, rx) = mpsc::channel(16);
    let pipe_name_for_task = pipe_name.to_string();
    let sd_for_task = sd;

    tokio::spawn(async move {
        let mut next = Some(first);
        loop {
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
                continue;
            }

            // Windows requires one pipe per one user connection.
            // Create the next instance BEFORE yielding the current connection so a client
            // arriving between yields still finds a listening pipe.
            next = match create_pipe_instance(&pipe_name_for_task, &sd_for_task, false) {
                Ok(s) => Some(s),
                Err(err) => {
                    warn!("Failed to create next admin pipe instance after accept: {err}");
                    None
                }
            };

            if tx.send(Ok(AdminConnection(server))).await.is_err() {
                break;
            }
        }
    });

    Ok(AdminListener { incoming: rx, pipe_name: pipe_name.to_string() })
}

/// No-op on Windows — pipes are cleaned up by the kernel when the server closes its handles.
pub fn cleanup_admin_endpoint(_pipe_name: &str) {}

fn create_pipe_instance(name: &str, sd: &SecurityDescriptor, is_first: bool) -> io::Result<NamedPipeServer> {
    let mut sa = sd.as_security_attributes();
    let mut opts = ServerOptions::new();
    if is_first {
        opts.first_pipe_instance(true);
    }
    // SAFETY: `sa` points at a valid SECURITY_ATTRIBUTES whose security descriptor is
    // owned by `sd` and outlives this call. Tokio forwards the pointer to
    // CreateNamedPipeW without retaining it
    unsafe { opts.create_with_security_attributes_raw(name, &mut sa as *mut _ as *mut _) }
}

pub struct AdminConnection(NamedPipeServer);

impl AsyncRead for AdminConnection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        // SAFETY: structural pinning — we never move the inner pipe out
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
// Win32 FFI for the security descriptor.
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

struct SecurityDescriptor {
    ptr: *mut c_void,
}

// SAFETY: SECURITY_DESCRIPTOR contents are immutable after construction. The kernel
// duplicates them into pipe metadata on CreateNamedPipe. The pointer is into LocalAlloc
// storage and is safe to share across threads
unsafe impl Send for SecurityDescriptor {}
unsafe impl Sync for SecurityDescriptor {}

impl SecurityDescriptor {
    fn from_sddl(sddl: &str) -> io::Result<Self> {
        let wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();
        let mut sd: *mut c_void = std::ptr::null_mut();
        // SAFETY: `wide` is a null-terminated UTF-16 string. `&mut sd` is valid to write.
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
            // SAFETY: `ptr` was returned by the SDDL conversion above; LocalFree is the
            // documented deallocator
            unsafe {
                LocalFree(self.ptr);
            }
        }
    }
}
