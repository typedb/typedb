/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::{json, Value};

use crate::{
    metrics::{ActionKind, ALL_CLIENT_ENDPOINTS},
    reports::{
        serialize_timestamp, ActionReport, DataLoadReport, DatabaseReport, ErrorReport, LoadReport, OsReport,
        SchemaLoadReport, ServerPropertiesReport, ServerReport, ServerReportSensitivePart,
    },
    Diagnostics,
};

const MONITORING_API_VERSION: usize = 1;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringReport {
    #[serde(flatten)]
    pub server_properties: JsonMonitoringServerPropertiesReport,

    pub server: JsonMonitoringServerReport,
    pub load: Vec<JsonMonitoringLoadReport>,
    pub actions: Vec<JsonMonitoringActionReport>,
    pub errors: Vec<JsonMonitoringErrorReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringServerPropertiesReport {
    pub version: usize,

    #[serde(rename = "deploymentID")]
    pub deployment_id: String,

    #[serde(rename = "serverID")]
    pub server_id: String,

    pub distribution: String,

    #[serde(serialize_with = "serialize_timestamp")]
    pub timestamp: DateTime<Utc>,
}

impl From<ServerPropertiesReport> for JsonMonitoringServerPropertiesReport {
    fn from(value: ServerPropertiesReport) -> Self {
        Self {
            version: MONITORING_API_VERSION,
            deployment_id: value.deployment_id,
            server_id: value.server_id,
            distribution: value.distribution,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringServerReport {
    pub version: String,

    #[serde(flatten)]
    pub sensitive_part: Option<JsonMonitoringServerReportSensitivePart>,
}

impl From<ServerReport> for JsonMonitoringServerReport {
    fn from(value: ServerReport) -> Self {
        Self { version: value.version, sensitive_part: value.sensitive_part.map(|part| part.into()) }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringServerReportSensitivePart {
    pub uptime_in_seconds: i64,
    pub os: JsonMonitoringOsReport,
    pub memory_used_in_bytes: u64,
    pub memory_available_in_bytes: u64,
    pub disk_used_in_bytes: u64,
    pub disk_available_in_bytes: u64,
}

impl From<ServerReportSensitivePart> for JsonMonitoringServerReportSensitivePart {
    fn from(value: ServerReportSensitivePart) -> Self {
        Self {
            uptime_in_seconds: value.uptime_in_seconds,
            os: value.os.into(),
            memory_used_in_bytes: value.memory_used_in_bytes,
            memory_available_in_bytes: value.memory_available_in_bytes,
            disk_used_in_bytes: value.disk_used_in_bytes,
            disk_available_in_bytes: value.disk_available_in_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringOsReport {
    pub name: String,
    pub arch: String,
    pub version: String,
}

impl From<OsReport> for JsonMonitoringOsReport {
    fn from(value: OsReport) -> Self {
        Self { name: value.name, arch: value.arch, version: value.version }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonMonitoringLoadReport {
    pub database: String,
    pub schema: Option<JsonMonitoringSchemaLoadReport>,
    pub data: Option<JsonMonitoringDataLoadReport>,
}

impl From<LoadReport> for JsonMonitoringLoadReport {
    fn from(value: LoadReport) -> Self {
        Self {
            database: value.database.to_string(),
            schema: value.schema.map(|schema| schema.into()),
            data: value.data.map(|data| data.into()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringSchemaLoadReport {
    pub type_count: u64,
}

impl From<SchemaLoadReport> for JsonMonitoringSchemaLoadReport {
    fn from(value: SchemaLoadReport) -> Self {
        Self { type_count: value.type_count }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringDataLoadReport {
    pub entity_count: u64,
    pub relation_count: u64,
    pub attribute_count: u64,
    pub has_count: u64,
    pub role_count: u64,
    pub storage_in_bytes: u64,
    pub storage_key_count: u64,
}

impl From<DataLoadReport> for JsonMonitoringDataLoadReport {
    fn from(value: DataLoadReport) -> Self {
        Self {
            entity_count: value.entity_count,
            relation_count: value.relation_count,
            attribute_count: value.attribute_count,
            has_count: value.has_count,
            role_count: value.role_count,
            storage_in_bytes: value.storage_in_bytes,
            storage_key_count: value.storage_key_count,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringActionReport {
    pub name: ActionKind,

    #[serde(flatten)]
    pub database: Option<DatabaseReport>,

    pub attempted: i64,
    pub successful: i64,
}

impl From<ActionReport> for JsonMonitoringActionReport {
    fn from(value: ActionReport) -> Self {
        Self {
            name: value.kind,
            database: value.database,
            attempted: value.successful + value.failed,
            successful: value.successful,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringErrorReport {
    pub code: String,

    #[serde(flatten)]
    pub database: Option<DatabaseReport>,

    pub count: i64,
}

impl From<ErrorReport> for JsonMonitoringErrorReport {
    fn from(value: ErrorReport) -> Self {
        Self { code: value.code, database: value.database, count: value.count }
    }
}

pub(crate) struct JsonMonitoringActionReportsBuilder {
    reports: HashMap<Option<DatabaseReport>, HashMap<ActionKind, JsonMonitoringActionReport>>,
}

impl JsonMonitoringActionReportsBuilder {
    pub fn new() -> Self {
        Self { reports: HashMap::new() }
    }

    pub fn build(self) -> Vec<JsonMonitoringActionReport> {
        self.reports.into_iter().flat_map(|(_, inner)| inner.into_values()).collect()
    }

    pub fn insert(&mut self, report: JsonMonitoringActionReport) {
        self.reports
            .entry(report.database.clone())
            .or_insert_with(HashMap::new)
            .entry(report.name)
            .and_modify(|existing| {
                existing.attempted += report.attempted;
                existing.successful += report.successful;
            })
            .or_insert(report);
    }
}

pub(crate) struct JsonMonitoringErrorReportsBuilder {
    reports: HashMap<Option<DatabaseReport>, HashMap<String, JsonMonitoringErrorReport>>,
}

impl JsonMonitoringErrorReportsBuilder {
    pub fn new() -> Self {
        Self { reports: HashMap::new() }
    }

    pub fn build(self) -> Vec<JsonMonitoringErrorReport> {
        self.reports.into_iter().flat_map(|(_, inner)| inner.into_values()).collect()
    }

    pub fn insert(&mut self, report: JsonMonitoringErrorReport) {
        self.reports
            .entry(report.database.clone())
            .or_insert_with(HashMap::new)
            .entry(report.code.clone())
            .and_modify(|existing| {
                existing.count += report.count;
            })
            .or_insert(report);
    }
}

pub(crate) fn to_monitoring_json(diagnostics: &Diagnostics) -> Value {
    json!(to_monitoring_report(diagnostics))
}

pub(crate) fn to_monitoring_report(diagnostics: &Diagnostics) -> JsonMonitoringReport {
    let server_properties = diagnostics.server_properties.to_state_report().into();
    let server = diagnostics.server_metrics.to_full_state_report().into();

    let load = diagnostics
        .lock_load_metrics_read()
        .iter()
        .filter_map(|(database_hash, metrics)| {
            metrics
                .to_state_report(database_hash, diagnostics.is_owned(database_hash))
                .map(|load_report| load_report.into())
        })
        .collect();

    let mut actions_builder = JsonMonitoringActionReportsBuilder::new();
    let mut errors_builder = JsonMonitoringErrorReportsBuilder::new();

    for client in ALL_CLIENT_ENDPOINTS {
        for (&database_hash, metrics) in diagnostics.lock_action_metrics_read(client).iter() {
            let action_reports = metrics.to_state_reports(&database_hash);
            for action_report in action_reports {
                actions_builder.insert(action_report.into());
            }
        }

        for (&database_hash, metrics) in diagnostics.lock_error_metrics_read(client).iter() {
            let error_reports = metrics.to_state_reports(&database_hash);
            for error_report in error_reports {
                errors_builder.insert(error_report.into());
            }
        }
    }

    JsonMonitoringReport {
        server_properties,
        server,
        load,
        actions: actions_builder.build(),
        errors: errors_builder.build(),
    }
}
