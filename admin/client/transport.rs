/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

use std::{path::Path, sync::Arc};

use tonic::transport::{Channel, Endpoint};

use crate::error::AdminError;

const DEFAULT_PLACEHOLDER_IP: &str = "http://127.0.0.1";

pub async fn connect_channel(endpoint: &Path) -> Result<Channel, AdminError> {
    #[cfg(unix)]
    {
        unix::verify_endpoint(endpoint)?;
    }
    #[cfg(windows)]
    {
        windows::verify_endpoint(endpoint)?;
    }

    // tonic requires a URI even for non-IP transports; `connect_with_connector` overrides it
    let placeholder_endpoint = Endpoint::try_from(DEFAULT_PLACEHOLDER_IP).map_err(|source| {
        AdminError::ConnectionFailed { address: endpoint.display().to_string(), source: Arc::new(source) }
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
