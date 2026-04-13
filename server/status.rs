/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

use crate::service::http::message::server::{BoxHttpServerResponse, LocalServerResponse};

#[derive(Clone, Debug)]
pub struct PublicEndpointAddress {
    serving_address: String,
    connection_address: String,
}

impl PublicEndpointAddress {
    pub fn new(serving_address: String, connection_address: String) -> Self {
        Self { serving_address, connection_address }
    }

    pub fn from_socket_addr(
        serving_address: SocketAddr,
        connection_address: String, // reference info (can be an alias), do not resolve!
    ) -> Self {
        Self::new(serving_address.to_string(), connection_address)
    }

    pub fn serving_address(&self) -> &str {
        &self.serving_address
    }

    pub fn connection_address(&self) -> &str {
        &self.connection_address
    }
}

#[derive(Clone, Debug)]
pub struct PrivateEndpointAddress {
    address: String,
}

impl PrivateEndpointAddress {
    pub fn new(address: String) -> Self {
        Self { address }
    }

    pub fn from_socket_addr(address: SocketAddr) -> Self {
        Self::new(address.to_string())
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

#[derive(Clone, Debug)]
pub struct LocalServerStatus {
    grpc: PublicEndpointAddress,
    http: Option<PublicEndpointAddress>,
    admin: Option<PrivateEndpointAddress>,
}

impl LocalServerStatus {
    pub fn new(
        grpc: PublicEndpointAddress,
        http: Option<PublicEndpointAddress>,
        admin: Option<PrivateEndpointAddress>,
    ) -> Self {
        Self { grpc, http, admin }
    }
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;

    fn to_http(&self) -> BoxHttpServerResponse;

    fn grpc_serving_address(&self) -> Option<&str>;

    fn grpc_connection_address(&self) -> Option<&str>;

    fn http_serving_address(&self) -> Option<&str>;

    fn http_connection_address(&self) -> Option<&str>;

    fn admin_address(&self) -> Option<&str>;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server { address: Some(self.grpc.connection_address().to_string()), replica_status: None }
    }

    fn to_http(&self) -> BoxHttpServerResponse {
        Box::new(LocalServerResponse { address: self.http.as_ref().map(|http| http.connection_address().to_string()) })
    }

    fn grpc_serving_address(&self) -> Option<&str> {
        Some(&self.grpc.serving_address())
    }

    fn grpc_connection_address(&self) -> Option<&str> {
        Some(&self.grpc.connection_address())
    }

    fn http_serving_address(&self) -> Option<&str> {
        self.http.as_ref().map(|http| http.serving_address())
    }

    fn http_connection_address(&self) -> Option<&str> {
        self.http.as_ref().map(|http| http.connection_address())
    }

    fn admin_address(&self) -> Option<&str> {
        self.admin.as_ref().map(|admin| admin.address())
    }
}
