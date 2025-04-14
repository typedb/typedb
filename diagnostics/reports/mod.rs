/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use chrono::{DateTime, Utc};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::{to_value, Map, Value};

use crate::{
    metrics::{ActionKind, ClientEndpoint, LoadKind},
    DatabaseHash,
};

pub(crate) mod json_monitoring;
pub(crate) mod posthog;
pub(crate) mod prometheus_monitoring;

pub trait ToJsonMap {
    fn to_json_map(&self) -> Map<String, Value>;
}

impl<T: Serialize> ToJsonMap for T {
    fn to_json_map(&self) -> Map<String, Value> {
        match to_value(self) {
            Ok(Value::Object(map)) => map,
            Ok(_) => panic!("Expected struct to serialize to a JSON object"),
            Err(e) => panic!("Serialization error: {e}"),
        }
    }
}

fn format_datetime(datetime: DateTime<Utc>) -> String {
    datetime.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
}

fn serialize_timestamp<S>(datetime: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format_datetime(*datetime))
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseReport(pub DatabaseHash);

impl Serialize for DatabaseReport {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DatabaseReport", 1)?;
        let DatabaseReport(database_hash) = self;
        state.serialize_field("database", &format!("{:.0}", database_hash))?;
        state.end()
    }
}

impl fmt::Display for DatabaseReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DatabaseReport(database_hash) = self;
        write!(f, "{:.0}", database_hash)
    }
}

#[derive(Debug)]
pub(crate) struct ServerPropertiesReport {
    pub deployment_id: String,
    pub server_id: String,
    pub distribution: String,
    pub enabled: bool,
}

#[derive(Debug)]
pub(crate) struct ServerReport {
    pub version: String,
    pub sensitive_part: Option<ServerReportSensitivePart>,
}

#[derive(Debug)]
pub(crate) struct ServerReportSensitivePart {
    pub uptime_in_seconds: i64,
    pub os: OsReport,
    pub memory_used_in_bytes: u64,
    pub memory_available_in_bytes: u64,
    pub disk_used_in_bytes: u64,
    pub disk_available_in_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct OsReport {
    pub name: String,
    pub arch: String,
    pub version: String,
}

#[derive(Debug)]
pub(crate) struct LoadReport {
    pub database: DatabaseReport,
    pub schema: Option<SchemaLoadReport>,
    pub data: Option<DataLoadReport>,
    pub connection: Option<ConnectionLoadReport>,
}

impl LoadReport {
    pub fn new(database_hash: DatabaseHash) -> Self {
        Self { database: DatabaseReport(database_hash), schema: None, data: None, connection: None }
    }
}

#[derive(Debug)]
pub(crate) struct SchemaLoadReport {
    pub type_count: u64,
}

#[derive(Debug)]
pub(crate) struct DataLoadReport {
    pub entity_count: u64,
    pub relation_count: u64,
    pub attribute_count: u64,
    pub has_count: u64,
    pub role_count: u64,
    pub storage_in_bytes: u64,
    pub storage_key_count: u64,
}

pub type ConnectionLoadReport = HashMap<ClientEndpoint, HashMap<LoadKind, u64>>;

#[derive(Debug)]
pub(crate) struct ActionReport {
    pub database: Option<DatabaseReport>,
    pub kind: ActionKind,
    pub successful: i64,
    pub failed: i64,
}

#[derive(Debug)]
pub(crate) struct ErrorReport {
    pub database: Option<DatabaseReport>,
    pub code: String,
    pub count: i64,
}
