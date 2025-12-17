/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

#[derive(Clone, Debug)]
pub struct LocalServerStatus {
    grpc_serving_address: String,
    grpc_connection_address: String,
    http_address: Option<String>,
}

impl LocalServerStatus {
    pub fn new(grpc_serving_address: String, grpc_connection_address: String, http_address: Option<String>) -> Self {
        Self { grpc_serving_address, grpc_connection_address, http_address }
    }

    pub fn from_addresses(
        grpc_serving_address: SocketAddr,
        grpc_connection_address: SocketAddr,
        http_address: Option<SocketAddr>,
    ) -> Self {
        Self::new(
            grpc_serving_address.to_string(),
            grpc_connection_address.to_string(),
            http_address.map(|address| address.to_string()),
        )
    }
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;

    fn grpc_serving_address(&self) -> Option<&str>;

    fn grpc_connection_address(&self) -> Option<&str>;

    fn http_address(&self) -> Option<&str>;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server {
            serving_address: Some(self.grpc_serving_address.clone()),
            connection_address: Some(self.grpc_connection_address.clone()),
            replica_status: None,
        }
    }

    fn grpc_serving_address(&self) -> Option<&str> {
        Some(&self.grpc_serving_address)
    }

    fn grpc_connection_address(&self) -> Option<&str> {
        Some(&self.grpc_connection_address)
    }

    fn http_address(&self) -> Option<&str> {
        self.http_address.as_ref().map(|address| address.as_str())
    }
}
