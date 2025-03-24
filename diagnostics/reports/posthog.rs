/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::HashMap,
    iter::Sum,
    ops::{Add, AddAssign},
};

use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::{json, Map, Value};

use crate::{
    metrics::{ActionKind, ClientEndpoint, ALL_CLIENT_ENDPOINTS},
    reports::{
        ActionReport, DataLoadReport, DatabaseReport, ErrorReport, LoadReport, OsReport, SchemaLoadReport,
        ServerPropertiesReport, ServerReport, ServerReportSensitivePart, ToJsonMap,
    },
    DatabaseHashOpt, Diagnostics,
};

const REPORTING_API_VERSION: usize = 2;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogPayload {
    pub api_key: String,
    pub batch: Vec<PosthogReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogReport {
    pub event: &'static str,
    pub properties: Map<String, Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerPropertiesReport {
    pub distinct_id: String,
    pub server_id: String,
    pub distribution: String,
    pub reporting_version: usize,
    pub enabled: bool,
}

impl From<ServerPropertiesReport> for PosthogServerPropertiesReport {
    fn from(value: ServerPropertiesReport) -> Self {
        Self {
            distinct_id: value.deployment_id,
            server_id: value.server_id,
            distribution: value.distribution,
            reporting_version: REPORTING_API_VERSION,
            enabled: value.enabled,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerReport {
    pub version: String,

    #[serde(flatten)]
    pub sensitive_part: Option<PosthogServerReportSensitivePart>,
}

impl From<ServerReport> for PosthogServerReport {
    fn from(value: ServerReport) -> Self {
        Self { version: value.version, sensitive_part: value.sensitive_part.map(|part| part.into()) }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerReportSensitivePart {
    pub uptime_in_seconds: i64,
    pub os: PosthogOsReport,
    pub memory_used_in_bytes: u64,
    pub memory_available_in_bytes: u64,
    pub disk_used_in_bytes: u64,
    pub disk_available_in_bytes: u64,
}

impl From<ServerReportSensitivePart> for PosthogServerReportSensitivePart {
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
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogOsReport {
    pub name: String,
    pub arch: String,
    pub version: String,
}

impl From<OsReport> for PosthogOsReport {
    fn from(value: OsReport) -> Self {
        Self { name: value.name, arch: value.arch, version: value.version }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct PosthogLoadReport {
    schema: Option<PosthogSchemaLoadReport>,
    data: Option<PosthogDataLoadReport>,
    connection: PosthogConnectionLoadReport,
}

impl From<LoadReport> for PosthogLoadReport {
    fn from(value: LoadReport) -> Self {
        let mut connection = PosthogConnectionLoadReport::new();
        if let Some(connection_reports) = value.connection {
            for (client, report) in connection_reports {
                for (load_kind, count) in report {
                    let inner = connection.entry(load_kind.to_posthog_name().to_string()).or_insert_with(HashMap::new);
                    inner.insert(client, count);
                }
            }
        }

        Self { schema: value.schema.map(|schema| schema.into()), data: value.data.map(|data| data.into()), connection }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogSchemaLoadReport {
    type_count: u64,
}

impl From<SchemaLoadReport> for PosthogSchemaLoadReport {
    fn from(value: SchemaLoadReport) -> Self {
        Self { type_count: value.type_count }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogDataLoadReport {
    entity_count: u64,
    relation_count: u64,
    attribute_count: u64,
    has_count: u64,
    role_count: u64,
    storage_in_bytes: u64,
    storage_key_count: u64,
}

impl From<DataLoadReport> for PosthogDataLoadReport {
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

pub type PosthogConnectionLoadReport = HashMap<String, HashMap<ClientEndpoint, u64>>;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogActionReport {
    pub successful: i64,
    pub failed: i64,
}

impl PosthogActionReport {
    pub fn new() -> Self {
        Self { successful: 0, failed: 0 }
    }
}

impl From<ActionReport> for PosthogActionReport {
    fn from(value: ActionReport) -> Self {
        Self { successful: value.successful, failed: value.failed }
    }
}

impl Add<PosthogActionReport> for PosthogActionReport {
    type Output = PosthogActionReport;

    fn add(self, rhs: PosthogActionReport) -> Self::Output {
        PosthogActionReport { successful: self.successful + rhs.successful, failed: self.failed + rhs.failed }
    }
}

impl AddAssign<PosthogActionReport> for PosthogActionReport {
    fn add_assign(&mut self, rhs: PosthogActionReport) {
        self.successful += rhs.successful;
        self.failed += rhs.failed;
    }
}

impl Sum for PosthogActionReport {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::new(), |lhs, rhs| lhs + rhs)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogErrorReport {
    count: i64,
}

impl From<ErrorReport> for PosthogErrorReport {
    fn from(value: ErrorReport) -> Self {
        Self { count: value.count }
    }
}

type PosthogActionReports = HashMap<ActionKind, HashMap<ClientEndpoint, PosthogActionReport>>;
type PosthogErrorReports = HashMap<String, HashMap<ClientEndpoint, PosthogErrorReport>>;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
enum PosthogReportKind {
    ServerUsage,
    DatabaseUsage,
}

impl PosthogReportKind {
    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            PosthogReportKind::ServerUsage => "server_usage",
            PosthogReportKind::DatabaseUsage => "database_usage",
        }
    }
}

struct PosthogReportBuilder {
    kind: PosthogReportKind,
    pub flat_properties: Map<String, Value>,
    pub load_report: Option<PosthogLoadReport>,
    pub action_reports: PosthogActionReports,
    pub error_reports: PosthogErrorReports,
}

impl PosthogReportBuilder {
    pub fn server_usage() -> Self {
        Self::new(PosthogReportKind::ServerUsage)
    }

    pub fn database_usage() -> Self {
        Self::new(PosthogReportKind::DatabaseUsage)
    }

    fn new(kind: PosthogReportKind) -> Self {
        Self {
            kind,
            flat_properties: Map::new(),
            load_report: None,
            action_reports: PosthogActionReports::new(),
            error_reports: PosthogErrorReports::new(),
        }
    }

    pub fn set_load(&mut self, load_report: LoadReport) {
        self.load_report = Some(load_report.into());
    }

    pub fn insert_action(&mut self, client: ClientEndpoint, action_report: ActionReport) {
        let inner = self.action_reports.entry(action_report.kind).or_insert_with(HashMap::new);
        inner.insert(client, action_report.into());
    }

    pub fn insert_error(&mut self, client: ClientEndpoint, error_report: ErrorReport) {
        let inner = self.error_reports.entry(error_report.code.clone()).or_insert_with(HashMap::new);
        inner.insert(client, error_report.into());
    }

    fn build(self) -> PosthogReport {
        let mut properties = self.flat_properties;

        if let Some(load_report) = self.load_report {
            properties.extend(load_report.to_json_map());
        }

        let mut total_queries = PosthogActionReport::new();
        for (action_kind, report) in self.action_reports {
            total_queries += Self::get_total_queries(action_kind, &report);
            properties.insert(action_kind.to_posthog_name().to_string(), json!(report));
        }

        if matches!(self.kind, PosthogReportKind::DatabaseUsage) {
            properties.insert("total_queries".to_string(), json!(total_queries));
        }

        if !self.error_reports.is_empty() {
            properties.insert("user_errors".to_string(), json!(self.error_reports));
        }

        PosthogReport { event: self.kind.to_posthog_name(), properties }
    }

    fn get_total_queries(
        action_kind: ActionKind,
        report: &HashMap<ClientEndpoint, PosthogActionReport>,
    ) -> PosthogActionReport {
        match action_kind.is_query() {
            true => report.values().cloned().sum(),
            false => PosthogActionReport::new(),
        }
    }
}

struct PosthogPayloadBuilder {
    api_key: String,
    usage_reports: HashMap<DatabaseHashOpt, PosthogReportBuilder>,
    server_properties: Map<String, Value>,
    server_metrics_full: Map<String, Value>,
    server_metrics_minimal: Map<String, Value>,
}

impl PosthogPayloadBuilder {
    fn new(diagnostics: &Diagnostics, api_key: String) -> Self {
        let server_properties: PosthogServerPropertiesReport = diagnostics.server_properties.to_state_report().into();
        let server_metrics_full: PosthogServerReport = diagnostics.server_metrics.to_full_state_report().into();
        let server_metrics_minimal: PosthogServerReport = diagnostics.server_metrics.to_minimal_state_report().into();
        Self {
            api_key,
            usage_reports: HashMap::new(),
            server_properties: server_properties.to_json_map(),
            server_metrics_full: server_metrics_full.to_json_map(),
            server_metrics_minimal: server_metrics_minimal.to_json_map(),
        }
    }

    fn build(self) -> PosthogPayload {
        let batch = if self.usage_reports.is_empty() {
            let mut server_usage = PosthogReportBuilder::server_usage();
            server_usage.flat_properties.extend(self.server_properties.to_json_map());
            server_usage.flat_properties.extend(self.server_metrics_minimal);
            vec![server_usage.build()]
        } else {
            self.usage_reports.into_values().map(|builder| builder.build()).collect()
        };

        PosthogPayload { api_key: self.api_key, batch }
    }

    fn get_or_init_report(&mut self, database_hash: DatabaseHashOpt) -> &mut PosthogReportBuilder {
        self.init_report_if_needed(database_hash);
        self.usage_reports.get_mut(&database_hash).expect("Expected to get by a just inserted key")
    }

    fn init_report_if_needed(&mut self, database_hash: DatabaseHashOpt) {
        if !self.usage_reports.contains_key(&database_hash) {
            let mut usage = if let Some(database_hash) = database_hash {
                let mut database_usage = PosthogReportBuilder::database_usage();
                database_usage.flat_properties.extend(DatabaseReport(database_hash).to_json_map());
                database_usage.flat_properties.extend(self.server_metrics_minimal.clone());
                database_usage
            } else {
                let mut server_usage = PosthogReportBuilder::server_usage();
                server_usage.flat_properties.extend(self.server_metrics_full.clone());
                server_usage
            };
            usage.flat_properties.extend(self.server_properties.clone());
            self.usage_reports.insert(database_hash, usage);
        }
    }
}

pub(crate) fn to_full_posthog_reporting_json(diagnostics: &Diagnostics, api_key: &str) -> Value {
    let mut builder = PosthogPayloadBuilder::new(diagnostics, api_key.to_string());
    builder.init_report_if_needed(None);

    for (database_hash, metrics) in diagnostics.lock_load_metrics_read().iter() {
        match metrics.to_peak_report(database_hash, diagnostics.is_owned(database_hash)) {
            Some(load_report) => {
                let report_builder = builder.get_or_init_report(Some(*database_hash));
                report_builder.set_load(load_report);
            }
            None => continue,
        }
    }

    for client in ALL_CLIENT_ENDPOINTS {
        for (&database_hash, metrics) in diagnostics.lock_action_metrics_read(client).iter() {
            let action_reports = metrics.to_diff_reports(database_hash);
            if action_reports.is_empty() {
                continue;
            }

            let report_builder = builder.get_or_init_report(database_hash);
            for action_report in action_reports {
                report_builder.insert_action(client, action_report);
            }
        }

        for (&database_hash, metrics) in diagnostics.lock_error_metrics_read(client).iter() {
            let error_reports = metrics.to_diff_reports(database_hash);
            if error_reports.is_empty() {
                continue;
            }

            let report_builder = builder.get_or_init_report(database_hash);
            for error_report in error_reports {
                report_builder.insert_error(client, error_report);
            }
        }
    }

    json!(builder.build())
}

pub(crate) fn to_minimal_posthog_reporting_json(diagnostics: &Diagnostics, api_key: &str) -> Value {
    json!(PosthogPayloadBuilder::new(diagnostics, api_key.to_string()).build())
}
