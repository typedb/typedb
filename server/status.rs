/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

#[derive(Clone, Debug)]
pub struct LocalServerStatus {
    grpc_address: String,
    http_address: Option<String>,
}

impl LocalServerStatus {
    pub fn new(grpc_address: String, http_address: Option<String>) -> Self {
        Self { grpc_address, http_address }
    }

    pub fn from_addresses(grpc_address: SocketAddr, http_address: Option<SocketAddr>) -> Self {
        Self::new(grpc_address.to_string(), http_address.map(|address| address.to_string()))
    }
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;

    fn grpc_address(&self) -> &str;

    fn http_address(&self) -> Option<&str>;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server { address: Some(self.grpc_address.clone()), replica_status: None }
    }

    fn grpc_address(&self) -> &str {
        &self.grpc_address
    }

    fn http_address(&self) -> Option<&str> {
        self.http_address.as_ref().map(|address| address.as_str())
    }
}
