/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};

use crate::state::BoxServerStatus;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerResponse {
    pub grpc_serving_address: Option<String>,
    pub grpc_connection_address: Option<String>,
    pub http_address: Option<String>,
    // TODO: Can we put ReplicaStatus here? Or shall we return a dyn ServerResponse overridden in cluster?
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServersResponse {
    pub servers: Vec<ServerResponse>,
}

pub(crate) fn encode_server(server: &BoxServerStatus) -> ServerResponse {
    ServerResponse {
        grpc_serving_address: server.grpc_serving_address().map(|s| s.to_string()),
        grpc_connection_address: server.grpc_connection_address().map(|s| s.to_string()),
        http_address: server.http_address().map(|s| s.to_string()),
    }
}

pub(crate) fn encode_servers(servers: Vec<BoxServerStatus>) -> ServersResponse {
    ServersResponse { servers: servers.iter().map(|s| encode_server(&s)).collect() }
}
