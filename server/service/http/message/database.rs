/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::service::http::message::from_request_parts_impl;

#[derive(Debug)]
pub(crate) struct DatabasePath {
    pub(crate) database_name: String,
}

from_request_parts_impl!(DatabasePath { database_name: String });

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabasesResponse {
    pub databases: Vec<DatabaseResponse>,
}

pub(crate) fn encode_databases(database_names: Vec<String>) -> DatabasesResponse {
    DatabasesResponse { databases: database_names.into_iter().map(|name| encode_database(name)).collect_vec() }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseResponse {
    pub name: String,
}

pub(crate) fn encode_database(name: String) -> DatabaseResponse {
    DatabaseResponse { name }
}
