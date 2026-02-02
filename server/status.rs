/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

use crate::service::http::message::server::{BoxHttpServerResponse, LocalServerResponse};

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
        grpc_connection_address: String, // reference info (can be an alias), do not resolve!
        http_address: Option<SocketAddr>,
    ) -> Self {
        Self::new(
            grpc_serving_address.to_string(),
            grpc_connection_address,
            http_address.map(|address| address.to_string()),
        )
    }

    pub fn grpc_serving_address(&self) -> Option<&str> {
        Some(&self.grpc_serving_address)
    }

    pub fn grpc_connection_address(&self) -> Option<&str> {
        Some(&self.grpc_connection_address)
    }

    pub fn http_address(&self) -> Option<&str> {
        self.http_address.as_ref().map(|address| address.as_str())
    }
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;

    fn to_http(&self) -> BoxHttpServerResponse;

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

    fn to_http(&self) -> BoxHttpServerResponse {
        Box::new(LocalServerResponse {
            grpc_serving_address: Some(self.grpc_serving_address.clone()),
            grpc_connection_address: Some(self.grpc_connection_address.clone()),
            http_address: self.http_address.clone(),
        })
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
