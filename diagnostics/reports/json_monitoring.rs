/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use serde_json::{Value, json};

use crate::{
    CoreMetrics, DatabaseId,
    metrics::{ALL_CLIENT_ENDPOINTS, ActionKind, HistogramSnapshot, HistogramUnit, LoadKind, QueryType},
    reports::{
        ActionReport, DataLoadReport, DatabaseReport, ErrorReport, LoadReport, OsReport, ProcessReport,
        SchemaLoadReport, ServerPropertiesReport, ServerReport, ServerReportSensitivePart, serialize_timestamp,
    },
};

/// Name + hash wrapper for the JSON monitoring exposition. Serializes as
/// `{"database": "<name>", "databaseId": "<hash>"}
#[derive(Debug, Clone)]
pub(crate) struct MonitoringDatabaseId(Arc<DatabaseId>);

impl MonitoringDatabaseId {
    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn hash_value(&self) -> u64 {
        self.0.hash_value()
    }
}

impl<T> From<T> for MonitoringDatabaseId
where
    T: Into<DatabaseReport>,
{
    fn from(id: T) -> Self {
        Self(id.into().0)
    }
}

impl Serialize for MonitoringDatabaseId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MonitoringDatabaseId", 2)?;
        state.serialize_field("database", self.0.name())?;
        state.serialize_field("databaseId", &format!("{:.0}", self.0.hash_value()))?;
        state.end()
    }
}

impl PartialEq for MonitoringDatabaseId {
    fn eq(&self, other: &Self) -> bool {
        self.0.hash_value() == other.0.hash_value()
    }
}

impl Eq for MonitoringDatabaseId {}

impl std::hash::Hash for MonitoringDatabaseId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash_value().hash(state);
    }
}

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

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub query_duration: Vec<JsonMonitoringHistogramByQueryKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transaction_duration: Vec<JsonMonitoringHistogramByTransactionKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries_per_transaction: Vec<JsonMonitoringHistogramPerDatabase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transaction_lifecycle: Vec<JsonMonitoringTransactionLifecycleEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wal_fsync_duration: Vec<JsonMonitoringHistogramPerDatabase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wal_bytes_written: Vec<JsonMonitoringDatabaseCounter>,
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
    pub process: JsonMonitoringProcessReport,
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
            process: value.process.into(),
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
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringProcessReport {
    pub cpu_seconds_total: f64,
    pub resident_memory_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub start_time_unix_seconds: u64,
}

impl From<ProcessReport> for JsonMonitoringProcessReport {
    fn from(value: ProcessReport) -> Self {
        Self {
            cpu_seconds_total: value.cpu_seconds_total,
            resident_memory_bytes: value.resident_memory_bytes,
            virtual_memory_bytes: value.virtual_memory_bytes,
            start_time_unix_seconds: value.start_time_unix_seconds,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringLoadReport {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub schema: Option<JsonMonitoringSchemaLoadReport>,
    pub data: Option<JsonMonitoringDataLoadReport>,
    // Flat list (not a nested map) so JSON output ordering is stable and
    // Prometheus exposition can iterate cleanly. Empty when no transactions
    // are in flight for this database.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_transactions: Vec<JsonMonitoringActiveTransactionEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringActiveTransactionEntry {
    pub client: crate::metrics::ClientEndpoint,
    pub kind: LoadKind,
    pub count: u64,
}

impl From<LoadReport> for JsonMonitoringLoadReport {
    fn from(value: LoadReport) -> Self {
        // Connection map → flat list. Sorted by (client, kind) so the
        // Prometheus writer emits a deterministic series order.
        let mut active_transactions: Vec<JsonMonitoringActiveTransactionEntry> = value
            .connection
            .map(|conn| {
                conn.into_iter()
                    .flat_map(|(client, by_kind)| {
                        by_kind.into_iter().map(move |(kind, count)| JsonMonitoringActiveTransactionEntry {
                            client,
                            kind,
                            count,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        active_transactions.sort_by_key(|e| {
            (
                match e.client {
                    crate::metrics::ClientEndpoint::Grpc => 0,
                    crate::metrics::ClientEndpoint::Http => 1,
                },
                match e.kind {
                    LoadKind::ReadTransactions => 0,
                    LoadKind::WriteTransactions => 1,
                    LoadKind::SchemaTransactions => 2,
                },
            )
        });
        Self {
            database: value.database.into(),
            schema: value.schema.map(|schema| schema.into()),
            data: value.data.map(|data| data.into()),
            active_transactions,
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
    pub database: Option<MonitoringDatabaseId>,

    pub attempted: i64,
    pub successful: i64,
}

impl From<ActionReport> for JsonMonitoringActionReport {
    fn from(value: ActionReport) -> Self {
        Self {
            name: value.kind,
            database: value.database.map(MonitoringDatabaseId),
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
    pub database: Option<MonitoringDatabaseId>,

    pub count: i64,
}

impl From<ErrorReport> for JsonMonitoringErrorReport {
    fn from(value: ErrorReport) -> Self {
        Self { code: value.code, database: value.database.map(MonitoringDatabaseId), count: value.count }
    }
}

pub(crate) struct JsonMonitoringActionReportsBuilder {
    reports: HashMap<Option<MonitoringDatabaseId>, HashMap<ActionKind, JsonMonitoringActionReport>>,
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
    reports: HashMap<Option<MonitoringDatabaseId>, HashMap<String, JsonMonitoringErrorReport>>,
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

/// One cumulative bucket in a histogram. `le` is the upper bound in display
/// units (seconds for duration histograms, raw counts for count histograms);
/// "+Inf" is emitted as the literal string `+Inf`.
#[derive(Debug, Serialize)]
pub(crate) struct JsonMonitoringHistogramBucket {
    pub le: String,
    pub count: u64,
}

/// Histogram report in JSON. `sum` is in display units (seconds for duration,
/// raw counts for count); `count` is total observations.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringHistogramReport {
    pub buckets: Vec<JsonMonitoringHistogramBucket>,
    pub count: u64,
    pub sum: f64,
}

impl From<HistogramSnapshot> for JsonMonitoringHistogramReport {
    fn from(snap: HistogramSnapshot) -> Self {
        let scale = match snap.unit {
            HistogramUnit::Nanoseconds => 1.0 / 1_000_000_000.0,
            HistogramUnit::Count => 1.0,
        };
        let mut buckets = Vec::with_capacity(snap.bucket_bounds.len() + 1);
        for (i, &bound) in snap.bucket_bounds.iter().enumerate() {
            buckets.push(JsonMonitoringHistogramBucket {
                le: format_le(bound as f64 * scale),
                count: snap.cumulative_counts[i],
            });
        }
        buckets.push(JsonMonitoringHistogramBucket { le: "+Inf".to_owned(), count: snap.count });
        Self { buckets, count: snap.count, sum: snap.sum as f64 * scale }
    }
}

/// Format a histogram bucket upper bound for the `le` label.
/// `%g`-style: drops trailing zeros, never uses exponential for these magnitudes.
pub(crate) fn format_le(value: f64) -> String {
    // Manual formatting because Rust's f64 Display uses scientific notation for
    // very small numbers (1e-5 instead of 0.00001), which would break Prometheus
    // parsers that expect fixed-point in `le` labels.
    if value >= 1.0 {
        // Integer or sub-integer: format with enough precision.
        if value.fract() == 0.0 { format!("{}", value as i64) } else { format!("{}", value) }
    } else {
        // sub-1: fixed-point with 9 decimal places, trim trailing zeros.
        let s = format!("{:.9}", value);
        let trimmed = s.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() { "0".to_owned() } else { trimmed.to_owned() }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringHistogramByQueryKind {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub kind: QueryType,
    pub histogram: JsonMonitoringHistogramReport,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringHistogramByTransactionKind {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub kind: LoadKind,
    pub histogram: JsonMonitoringHistogramReport,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringHistogramPerDatabase {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub histogram: JsonMonitoringHistogramReport,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringDatabaseCounter {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub value: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JsonMonitoringTransactionLifecycleEntry {
    #[serde(flatten)]
    pub database: MonitoringDatabaseId,
    pub kind: LoadKind,
    pub started: u64,
    pub committed: u64,
    pub rolled_back: u64,
    pub closed: u64,
}

// QueryType + LoadKind need Serialize for the JSON output. We add a
// lowercased-name implementation via serde derive on the source types.

pub(crate) fn to_monitoring_json(core_metrics: &CoreMetrics) -> serde_json::Map<String, Value> {
    crate::reports::ToJsonMap::to_json_map(&to_monitoring_report(core_metrics))
}

pub(crate) fn to_monitoring_report(core_metrics: &CoreMetrics) -> JsonMonitoringReport {
    let server_properties = core_metrics.server_properties.to_state_report().into();
    let server = core_metrics.server_metrics.to_full_state_report().into();

    let load = core_metrics
        .lock_load_metrics_read()
        .values()
        .filter_map(|metrics| metrics.to_state_report().map(|load_report| load_report.into()))
        .collect();

    let mut actions_builder = JsonMonitoringActionReportsBuilder::new();
    let mut errors_builder = JsonMonitoringErrorReportsBuilder::new();

    for client in ALL_CLIENT_ENDPOINTS {
        for metrics in core_metrics.lock_action_metrics_read(client).values() {
            for action_report in metrics.to_state_reports() {
                actions_builder.insert(action_report.into());
            }
        }

        for metrics in core_metrics.lock_error_metrics_read(client).values() {
            for error_report in metrics.to_state_reports() {
                errors_builder.insert(error_report.into());
            }
        }
    }

    // Sort by hash so exposition order is stable across scrapes.
    let mut histogram_snapshots = core_metrics.histogram_snapshots();
    histogram_snapshots.sort_by_key(|(id, _)| id.hash_value());

    let mut query_duration = Vec::new();
    let mut transaction_duration = Vec::new();
    let mut queries_per_transaction = Vec::new();
    let mut transaction_lifecycle = Vec::new();
    let mut wal_fsync_duration = Vec::new();
    let mut wal_bytes_written = Vec::new();
    for (id, snap) in histogram_snapshots {
        let db = MonitoringDatabaseId(id);
        for (kind, hist) in snap.query_duration {
            if hist.count == 0 {
                continue;
            }
            query_duration.push(JsonMonitoringHistogramByQueryKind {
                database: db.clone(),
                kind,
                histogram: hist.into(),
            });
        }
        for (kind, hist) in snap.transaction_duration {
            if hist.count == 0 {
                continue;
            }
            transaction_duration.push(JsonMonitoringHistogramByTransactionKind {
                database: db.clone(),
                kind,
                histogram: hist.into(),
            });
        }
        if snap.queries_per_transaction.count != 0 {
            queries_per_transaction.push(JsonMonitoringHistogramPerDatabase {
                database: db.clone(),
                histogram: snap.queries_per_transaction.into(),
            });
        }
        let lc = &snap.transaction_lifecycle;
        for (i, (kind, started)) in lc.started.iter().enumerate() {
            let committed = lc.committed[i].1;
            let rolled_back = lc.rolled_back[i].1;
            let closed = lc.closed[i].1;
            if *started == 0 && committed == 0 && rolled_back == 0 && closed == 0 {
                continue;
            }
            transaction_lifecycle.push(JsonMonitoringTransactionLifecycleEntry {
                database: db.clone(),
                kind: *kind,
                started: *started,
                committed,
                rolled_back,
                closed,
            });
        }
        if snap.wal_fsync_duration.count != 0 {
            wal_fsync_duration.push(JsonMonitoringHistogramPerDatabase {
                database: db.clone(),
                histogram: snap.wal_fsync_duration.into(),
            });
        }
        if snap.wal_bytes_written != 0 {
            wal_bytes_written
                .push(JsonMonitoringDatabaseCounter { database: db.clone(), value: snap.wal_bytes_written });
        }
    }

    JsonMonitoringReport {
        server_properties,
        server,
        load,
        actions: actions_builder.build(),
        errors: errors_builder.build(),
        query_duration,
        transaction_duration,
        queries_per_transaction,
        transaction_lifecycle,
        wal_fsync_duration,
        wal_bytes_written,
    }
}
