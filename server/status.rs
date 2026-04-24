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
    listen_address: String,
    advertise_address: String,
}

impl PublicEndpointAddress {
    pub fn new(listen_address: String, advertise_address: String) -> Self {
        Self { listen_address, advertise_address }
    }

    pub fn from_socket_addr(
        listen_address: SocketAddr,
        advertise_address: String, // reference info (can be an alias), do not resolve!
    ) -> Self {
        Self::new(listen_address.to_string(), advertise_address)
    }

    pub fn listen_address(&self) -> &str {
        &self.listen_address
    }

    pub fn advertise_address(&self) -> &str {
        &self.advertise_address
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

    fn grpc_listen_address(&self) -> Option<&str>;

    fn grpc_advertise_address(&self) -> Option<&str>;

    fn http_listen_address(&self) -> Option<&str>;

    fn http_advertise_address(&self) -> Option<&str>;

    fn admin_address(&self) -> Option<&str>;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server { address: Some(self.grpc.advertise_address().to_string()), replica_status: None }
    }

    fn to_http(&self) -> BoxHttpServerResponse {
        Box::new(LocalServerResponse { address: self.http.as_ref().map(|http| http.advertise_address().to_string()) })
    }

    fn grpc_listen_address(&self) -> Option<&str> {
        Some(&self.grpc.listen_address())
    }

    fn grpc_advertise_address(&self) -> Option<&str> {
        Some(&self.grpc.advertise_address())
    }

    fn http_listen_address(&self) -> Option<&str> {
        self.http.as_ref().map(|http| http.listen_address())
    }

    fn http_advertise_address(&self) -> Option<&str> {
        self.http.as_ref().map(|http| http.advertise_address())
    }

    fn admin_address(&self) -> Option<&str> {
        self.admin.as_ref().map(|admin| admin.address())
    }
}
