/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::net::SocketAddr;
use tokio::net::lookup_host;

pub async fn resolve_address(address: String) -> SocketAddr {
    lookup_host(address.clone())
        .await
        .unwrap()
        .next()
        .unwrap_or_else(|| panic!("Unable to map address '{}' to any IP addresses", address))
}