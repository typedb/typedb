/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Platform-specific transport for the admin client.
//!
//! On Unix the admin endpoint is a Unix domain socket addressed by filesystem path; on
//! Windows it's a Named Pipe addressed by pipe name. The two transports use different
//! tokio APIs underneath; the client connect path is gated per OS.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

use std::{path::Path, sync::Arc};

use tonic::transport::{Channel, Endpoint};

use crate::error::AdminError;

/// Endpoint identifier — a filesystem path string on Unix, a pipe name on Windows.
///
/// The CLI accepts the same `--socket-path` argument on both platforms; on Unix it's
/// interpreted as a path, on Windows as a Named Pipe name (e.g. `\\.\pipe\typedb-admin`).
pub type AdminEndpoint = Path;

/// Connect to the admin gRPC endpoint, returning a tonic [`Channel`].
///
/// The kernel-enforced permission check on the endpoint is the load-bearing fence: on
/// Unix that's the socket file's mode bits; on Windows it's the Named Pipe DACL. We do
/// a defensive client-side check first (refuses an obviously-loose endpoint and gives
/// the operator a clear error), then dial.
pub async fn connect_channel(endpoint: &AdminEndpoint) -> Result<Channel, AdminError> {
    #[cfg(unix)]
    {
        unix::verify_endpoint(endpoint)?;
    }
    #[cfg(windows)]
    {
        windows::verify_endpoint(endpoint)?;
    }

    // tonic's Endpoint always needs a URI even for non-IP transports; the URI is never
    // dialed because connect_with_connector overrides the transport with our factory.
    let placeholder_endpoint = Endpoint::try_from("http://[::]").map_err(|source| AdminError::ConnectionFailed {
        address: endpoint.display().to_string(),
        source: Arc::new(source),
    })?;

    #[cfg(unix)]
    {
        unix::connect(placeholder_endpoint, endpoint).await
    }
    #[cfg(windows)]
    {
        windows::connect(placeholder_endpoint, endpoint).await
    }
}
