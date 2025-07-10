/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

#[derive(Clone, Copy, Debug)]
pub struct LocalServerStatus {
    grpc_address: SocketAddr,
    http_address: Option<SocketAddr>,
}

impl LocalServerStatus {
    pub(crate) fn new(grpc_address: SocketAddr, http_address: Option<SocketAddr>) -> Self {
        Self { grpc_address, http_address }
    }
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;

    fn grpc_address(&self) -> SocketAddr;

    fn http_address(&self) -> Option<SocketAddr>;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server { address: self.grpc_address.to_string(), replica_status: None }
    }

    fn grpc_address(&self) -> SocketAddr {
        self.grpc_address
    }

    fn http_address(&self) -> Option<SocketAddr> {
        self.http_address
    }
}
