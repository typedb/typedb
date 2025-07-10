/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt::Debug, net::SocketAddr};

use typedb_protocol;

#[derive(Clone, Copy, Debug)]
pub struct LocalServerStatus {
    pub address: SocketAddr,
}

pub trait ServerStatus: Debug {
    fn to_proto(&self) -> typedb_protocol::Server;
}

impl ServerStatus for LocalServerStatus {
    fn to_proto(&self) -> typedb_protocol::Server {
        typedb_protocol::Server { address: self.address.to_string(), replica_status: None }
    }
}
