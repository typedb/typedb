/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{collections::HashMap, fmt::Write};

use crate::{
    DatabaseHash, Diagnostics,
    reports::json_monitoring::{JsonMonitoringHistogramReport, JsonMonitoringReport, to_monitoring_report},
};

pub fn to_monitoring_prometheus(diagnostics: &Diagnostics) -> String {
    to_prometheus(to_monitoring_report(diagnostics), &diagnostics.database_names_snapshot())
}

/// Emits `database="<name>", database_id="<hash>"`. If the name hasn't been
/// observed yet (transient at startup), falls back to the hash for both labels.
fn db_labels(hash_str: &str, names: &HashMap<DatabaseHash, String>) -> String {
    let hash_u64 = hash_str.parse::<u64>().ok();
    match hash_u64.and_then(|h| names.get(&h)) {
        Some(name) => format!("database=\"{}\", database_id=\"{}\"", name, hash_str),
        None => format!("database=\"{}\", database_id=\"{}\"", hash_str, hash_str),
    }
}

pub fn to_prometheus(report: JsonMonitoringReport, names: &HashMap<DatabaseHash, String>) -> String {
    use std::fmt::Write;

    let mut out = String::new();

    writeln!(out, "# distribution: {}", report.server_properties.distribution).unwrap();
    writeln!(out, "# version: {}", report.server.version).unwrap();
    if let Some(sensitive) = &report.server.sensitive_part {
        let os = &sensitive.os;
        writeln!(out, "# os: {} {} {}", os.name, os.arch, os.version).unwrap();
    }

    writeln!(out, "\n# HELP typedb_build_info Build and runtime identity of this TypeDB server.").unwrap();
    writeln!(out, "# TYPE typedb_build_info gauge").unwrap();
    let (os_name, os_arch, os_version) = match &report.server.sensitive_part {
        Some(sensitive) => (sensitive.os.name.as_str(), sensitive.os.arch.as_str(), sensitive.os.version.as_str()),
        None => ("", "", ""),
    };
    writeln!(
        out,
        "typedb_build_info{{version=\"{}\", distribution=\"{}\", os_name=\"{}\", os_arch=\"{}\", os_version=\"{}\"}} 1",
        report.server.version, report.server_properties.distribution, os_name, os_arch, os_version
    )
    .unwrap();

    if let Some(sensitive) = &report.server.sensitive_part {
        writeln!(out, "\n# TYPE server_resources_count gauge").unwrap();
        writeln!(out, "server_resources_count{{kind=\"memoryUsedInBytes\"}} {}", sensitive.memory_used_in_bytes)
            .unwrap();
        writeln!(
            out,
            "server_resources_count{{kind=\"memoryAvailableInBytes\"}} {}",
            sensitive.memory_available_in_bytes
        )
        .unwrap();
        writeln!(out, "server_resources_count{{kind=\"diskUsedInBytes\"}} {}", sensitive.disk_used_in_bytes).unwrap();
        writeln!(out, "server_resources_count{{kind=\"diskAvailableInBytes\"}} {}", sensitive.disk_available_in_bytes)
            .unwrap();

        let p = &sensitive.process;
        writeln!(out, "\n# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.").unwrap();
        writeln!(out, "# TYPE process_cpu_seconds_total counter").unwrap();
        writeln!(out, "process_cpu_seconds_total {}", p.cpu_seconds_total).unwrap();
        writeln!(out, "# HELP process_resident_memory_bytes Resident memory size in bytes.").unwrap();
        writeln!(out, "# TYPE process_resident_memory_bytes gauge").unwrap();
        writeln!(out, "process_resident_memory_bytes {}", p.resident_memory_bytes).unwrap();
        writeln!(out, "# HELP process_virtual_memory_bytes Virtual memory size in bytes.").unwrap();
        writeln!(out, "# TYPE process_virtual_memory_bytes gauge").unwrap();
        writeln!(out, "process_virtual_memory_bytes {}", p.virtual_memory_bytes).unwrap();
        writeln!(out, "# HELP process_start_time_seconds Start time of the process since unix epoch in seconds.")
            .unwrap();
        writeln!(out, "# TYPE process_start_time_seconds gauge").unwrap();
        writeln!(out, "process_start_time_seconds {}", p.start_time_unix_seconds).unwrap();
    }

    writeln!(out, "\n# TYPE typedb_schema_data_count gauge").unwrap();
    for db in &report.load {
        let labels = db_labels(db.database.as_str(), names);
        if let Some(schema) = &db.schema {
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"typeCount\"}} {}", labels, schema.type_count).unwrap();
        }
        if let Some(data) = &db.data {
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"entityCount\"}} {}", labels, data.entity_count)
                .unwrap();
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"relationCount\"}} {}", labels, data.relation_count)
                .unwrap();
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"attributeCount\"}} {}", labels, data.attribute_count)
                .unwrap();
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"hasCount\"}} {}", labels, data.has_count).unwrap();
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"roleCount\"}} {}", labels, data.role_count).unwrap();
            writeln!(out, "typedb_schema_data_count{{{}, kind=\"storageInBytes\"}} {}", labels, data.storage_in_bytes)
                .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{{}, kind=\"storageKeyCount\"}} {}",
                labels, data.storage_key_count
            )
            .unwrap();
        }
    }

    // Live in-flight transaction counts per (database, client, kind). All six
    // (client × kind) entries are emitted per observed database, including
    // zeros — matches the `process_*` family's "emit even when zero/unsupported"
    // posture so dashboards render continuous flat lines at rest.
    if !report.load.is_empty() {
        writeln!(out, "\n# HELP typedb_transactions_active In-flight transactions by client and kind.").unwrap();
        writeln!(out, "# TYPE typedb_transactions_active gauge").unwrap();
        for db in &report.load {
            let labels = db_labels(db.database.as_str(), names);
            for entry in &db.active_transactions {
                writeln!(
                    out,
                    "typedb_transactions_active{{{}, client=\"{}\", kind=\"{}\"}} {}",
                    labels,
                    entry.client,
                    txn_kind_label(&entry.kind),
                    entry.count
                )
                .unwrap();
            }
        }
    }

    writeln!(out, "\n# TYPE typedb_attempted_requests_total counter").unwrap();
    for action in &report.actions {
        if let Some(db) = &action.database {
            let labels = db_labels(&db.0.to_string(), names);
            writeln!(
                out,
                "typedb_attempted_requests_total{{{}, kind=\"{}\"}} {}",
                labels, action.name, action.attempted
            )
            .unwrap();
        } else {
            writeln!(out, "typedb_attempted_requests_total{{kind=\"{}\"}} {}", action.name, action.attempted).unwrap();
        }
    }

    writeln!(out, "\n# TYPE typedb_successful_requests_total counter").unwrap();
    for action in &report.actions {
        if let Some(db) = &action.database {
            let labels = db_labels(&db.0.to_string(), names);
            writeln!(
                out,
                "typedb_successful_requests_total{{{}, kind=\"{}\"}} {}",
                labels, action.name, action.successful
            )
            .unwrap();
        } else {
            writeln!(out, "typedb_successful_requests_total{{kind=\"{}\"}} {}", action.name, action.successful)
                .unwrap();
        }
    }

    writeln!(out, "\n# TYPE typedb_error_total counter").unwrap();
    for err in &report.errors {
        if let Some(db) = &err.database {
            let labels = db_labels(&db.0.to_string(), names);
            writeln!(out, "typedb_error_total{{{}, code=\"{}\"}} {}", labels, err.code, err.count).unwrap();
        }
    }

    writeln!(out, "\n# HELP typedb_query_duration_seconds Query execution latency.").unwrap();
    writeln!(out, "# TYPE typedb_query_duration_seconds histogram").unwrap();
    for entry in &report.query_duration {
        let labels = db_labels(&entry.database.0.to_string(), names);
        let kind_label = format!("{}, kind=\"{}\"", labels, query_kind_label(&entry.kind));
        write_histogram_body(&mut out, "typedb_query_duration_seconds", &kind_label, &entry.histogram);
    }

    writeln!(
        out,
        "\n# HELP typedb_transaction_duration_seconds Transaction lifetime (open\u{2192}commit/rollback/abort)."
    )
    .unwrap();
    writeln!(out, "# TYPE typedb_transaction_duration_seconds histogram").unwrap();
    for entry in &report.transaction_duration {
        let labels = db_labels(&entry.database.0.to_string(), names);
        let kind_label = format!("{}, kind=\"{}\"", labels, txn_kind_label(&entry.kind));
        write_histogram_body(&mut out, "typedb_transaction_duration_seconds", &kind_label, &entry.histogram);
    }

    writeln!(out, "\n# HELP typedb_queries_per_transaction Queries executed per transaction.").unwrap();
    writeln!(out, "# TYPE typedb_queries_per_transaction histogram").unwrap();
    for entry in &report.queries_per_transaction {
        let labels = db_labels(&entry.database.0.to_string(), names);
        write_histogram_body(&mut out, "typedb_queries_per_transaction", &labels, &entry.histogram);
    }

    write_lifecycle_counter(
        &mut out,
        "typedb_transactions_started_total",
        "Transactions opened, by transaction kind.",
        &report,
        names,
        |e| e.started,
    );
    write_lifecycle_counter(
        &mut out,
        "typedb_transactions_committed_total",
        "Transactions that committed successfully.",
        &report,
        names,
        |e| e.committed,
    );
    write_lifecycle_counter(
        &mut out,
        "typedb_transactions_rolled_back_total",
        "Transactions explicitly rolled back by the client.",
        &report,
        names,
        |e| e.rolled_back,
    );
    write_lifecycle_counter(
        &mut out,
        "typedb_transactions_closed_total",
        "Transactions closed without a successful commit (force-closed, dropped, timed out).",
        &report,
        names,
        |e| e.closed,
    );

    writeln!(out, "\n# HELP typedb_wal_fsync_duration_seconds WAL fsync latency.").unwrap();
    writeln!(out, "# TYPE typedb_wal_fsync_duration_seconds histogram").unwrap();
    for entry in &report.wal_fsync_duration {
        let labels = db_labels(&entry.database.0.to_string(), names);
        write_histogram_body(&mut out, "typedb_wal_fsync_duration_seconds", &labels, &entry.histogram);
    }

    if !report.wal_bytes_written.is_empty() {
        writeln!(out, "\n# HELP typedb_wal_bytes_written_total Bytes written to the WAL.").unwrap();
        writeln!(out, "# TYPE typedb_wal_bytes_written_total counter").unwrap();
        for entry in &report.wal_bytes_written {
            let labels = db_labels(&entry.database.0.to_string(), names);
            writeln!(out, "typedb_wal_bytes_written_total{{{}}} {}", labels, entry.value).unwrap();
        }
    }

    out
}

fn write_histogram_body(out: &mut String, metric_name: &str, labels: &str, histogram: &JsonMonitoringHistogramReport) {
    for bucket in &histogram.buckets {
        writeln!(out, "{}_bucket{{{}, le=\"{}\"}} {}", metric_name, labels, bucket.le, bucket.count).unwrap();
    }
    writeln!(out, "{}_count{{{}}} {}", metric_name, labels, histogram.count).unwrap();
    writeln!(out, "{}_sum{{{}}} {}", metric_name, labels, histogram.sum).unwrap();
}

fn write_lifecycle_counter<F>(
    out: &mut String,
    metric_name: &str,
    help_text: &str,
    report: &JsonMonitoringReport,
    names: &HashMap<DatabaseHash, String>,
    field: F,
) where
    F: Fn(&crate::reports::json_monitoring::JsonMonitoringTransactionLifecycleEntry) -> u64,
{
    writeln!(out, "\n# HELP {} {}", metric_name, help_text).unwrap();
    writeln!(out, "# TYPE {} counter", metric_name).unwrap();
    for entry in &report.transaction_lifecycle {
        let labels = db_labels(&entry.database.0.to_string(), names);
        writeln!(out, "{}{{{}, kind=\"{}\"}} {}", metric_name, labels, txn_kind_label(&entry.kind), field(entry))
            .unwrap();
    }
}

fn query_kind_label(kind: &crate::metrics::QueryType) -> &'static str {
    kind.as_label()
}

fn txn_kind_label(kind: &crate::metrics::LoadKind) -> &'static str {
    match kind {
        crate::metrics::LoadKind::ReadTransactions => "read",
        crate::metrics::LoadKind::WriteTransactions => "write",
        crate::metrics::LoadKind::SchemaTransactions => "schema",
    }
}
