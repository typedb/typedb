/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Platform-specific transport for the admin endpoint.
//!
//! The admin endpoint exposes a small gRPC service for privileged local operations
//! (server inspection, force-setting a user's password). Its trust anchor is
//! kernel-enforced access control on the endpoint itself: on Unix that's the mode bits
//! on a Unix domain socket file; on Windows it's the DACL on a Named Pipe.
//!
//! Both transports converge on the same shape — a stream of byte-oriented connections
//! that tonic can serve gRPC over — so server-side bind, client-side connect, and tests
//! all take the same path regardless of OS, with per-OS impls under [`unix`] and
//! [`windows`].

#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

#[cfg(unix)]
pub use unix::{AdminConnection, AdminListener, AdminPath, bind_admin_endpoint, cleanup_admin_endpoint};
#[cfg(windows)]
pub use windows::{AdminConnection, AdminListener, AdminPath, bind_admin_endpoint, cleanup_admin_endpoint};

/// Render an endpoint identifier as a string for display, logging, and status RPCs.
///
/// On Unix the [`AdminPath`] is a [`PathBuf`]; on Windows it's a [`String`] pipe name.
/// Both render the same way for display purposes.
pub fn endpoint_to_string(endpoint: &AdminPath) -> String {
    #[cfg(unix)]
    {
        endpoint.to_string_lossy().into_owned()
    }
    #[cfg(windows)]
    {
        endpoint.clone()
    }
}
