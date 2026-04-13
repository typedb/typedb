/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Debug;

use erased_serde::serialize_trait_object;
use serde::{Deserialize, Serialize};

use crate::state::BoxServerStatus;

pub trait HttpServerResponse: erased_serde::Serialize + Debug + Send + Sync {}

serialize_trait_object!(HttpServerResponse);

pub type BoxHttpServerResponse = Box<dyn HttpServerResponse>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalServerResponse {
    pub address: Option<String>,
}

impl HttpServerResponse for LocalServerResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServersResponse {
    pub servers: Vec<BoxHttpServerResponse>,
}

pub(crate) fn encode_servers(servers: Vec<BoxServerStatus>) -> ServersResponse {
    ServersResponse { servers: servers.iter().map(|s| s.to_http()).collect() }
}
