/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use crate::{
    DatabaseHash, Diagnostics,
    reports::json_monitoring::{JsonMonitoringReport, to_monitoring_report},
};

pub fn to_monitoring_prometheus(diagnostics: &Diagnostics, expose_database_names: bool) -> String {
    let names = if expose_database_names { diagnostics.database_names_snapshot() } else { HashMap::new() };
    to_prometheus(to_monitoring_report(diagnostics), &names, expose_database_names)
}

/// Render a `database` label fragment. When `expose_database_names` is false,
/// emits only `database="<hash>"` for backward compatibility with the original 3.x
/// hash-only exposition format — no human name is included and no `database_id`
/// label appears either (callers that scrape with the flag off should not have
/// to learn a new label name). When true, emits `database="<name>", database_id="<hash>"`
/// (falling back to hash for `database` if the name hasn't been observed yet).
fn db_labels(hash_str: &str, names: &HashMap<DatabaseHash, String>, expose_database_names: bool) -> String {
    if !expose_database_names {
        return format!("database=\"{}\"", hash_str);
    }
    let hash_u64 = hash_str.parse::<u64>().ok();
    match hash_u64.and_then(|h| names.get(&h)) {
        Some(name) => format!("database=\"{}\", database_id=\"{}\"", name, hash_str),
        None => format!("database=\"{}\", database_id=\"{}\"", hash_str, hash_str),
    }
}

pub fn to_prometheus(
    report: JsonMonitoringReport,
    names: &HashMap<DatabaseHash, String>,
    expose_database_names: bool,
) -> String {
    use std::fmt::Write;

    let mut out = String::new();

    writeln!(out, "# distribution: {}", report.server_properties.distribution).unwrap();
    writeln!(out, "# version: {}", report.server.version).unwrap();
    if let Some(sensitive) = &report.server.sensitive_part {
        let os = &sensitive.os;
        writeln!(out, "# os: {} {} {}", os.name, os.arch, os.version).unwrap();
    }

    // typedb_build_info follows the Prometheus convention used by node_exporter,
    // postgres_exporter, etc.: a value-1 gauge whose label set carries build/identity
    // facts that should survive scrape aggregation (you can't aggregate scrape-time
    // header comments). Empty strings for os labels keep the line valid even when
    // the minimal report is in use (sensitive_part = None).
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
    }

    writeln!(out, "\n# TYPE typedb_schema_data_count gauge").unwrap();
    for db in &report.load {
        let labels = db_labels(db.database.as_str(), names, expose_database_names);
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

    writeln!(out, "\n# TYPE typedb_attempted_requests_total counter").unwrap();
    for action in &report.actions {
        if let Some(db) = &action.database {
            let labels = db_labels(&db.0.to_string(), names, expose_database_names);
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
            let labels = db_labels(&db.0.to_string(), names, expose_database_names);
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
            let labels = db_labels(&db.0.to_string(), names, expose_database_names);
            writeln!(out, "typedb_error_total{{{}, code=\"{}\"}} {}", labels, err.code, err.count).unwrap();
        }
    }

    out
}
