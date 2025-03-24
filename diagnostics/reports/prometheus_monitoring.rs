/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::{
    reports::json_monitoring::{to_monitoring_report, JsonMonitoringReport},
    Diagnostics,
};

pub fn to_monitoring_prometheus(diagnostics: &Diagnostics) -> String {
    to_prometheus(to_monitoring_report(diagnostics))
}

pub fn to_prometheus(report: JsonMonitoringReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();

    writeln!(out, "# distribution: {}", report.server_properties.distribution).unwrap();
    writeln!(out, "# version: {}", report.server.version).unwrap();
    if let Some(sensitive) = &report.server.sensitive_part {
        let os = &sensitive.os;
        writeln!(out, "# os: {} {} {}", os.name, os.arch, os.version).unwrap();
    }

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
        let db_name = db.database.as_str();
        if let Some(schema) = &db.schema {
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"typeCount\"}} {}",
                db_name, schema.type_count
            )
            .unwrap();
        }
        if let Some(data) = &db.data {
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"entityCount\"}} {}",
                db_name, data.entity_count
            )
            .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"relationCount\"}} {}",
                db_name, data.relation_count
            )
            .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"attributeCount\"}} {}",
                db_name, data.attribute_count
            )
            .unwrap();
            writeln!(out, "typedb_schema_data_count{{database=\"{}\", kind=\"hasCount\"}} {}", db_name, data.has_count)
                .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"roleCount\"}} {}",
                db_name, data.role_count
            )
            .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"storageInBytes\"}} {}",
                db_name, data.storage_in_bytes
            )
            .unwrap();
            writeln!(
                out,
                "typedb_schema_data_count{{database=\"{}\", kind=\"storageKeyCount\"}} {}",
                db_name, data.storage_key_count
            )
            .unwrap();
        }
    }

    writeln!(out, "\n# TYPE typedb_attempted_requests_total counter").unwrap();
    for action in &report.actions {
        if let Some(db) = &action.database {
            writeln!(
                out,
                "typedb_attempted_requests_total{{database=\"{}\", kind=\"{}\"}} {}",
                db.0, action.name, action.attempted
            )
            .unwrap();
        } else {
            writeln!(out, "typedb_attempted_requests_total{{kind=\"{}\"}} {}", action.name, action.attempted).unwrap();
        }
    }

    writeln!(out, "\n# TYPE typedb_successful_requests_total counter").unwrap();
    for action in &report.actions {
        if let Some(db) = &action.database {
            writeln!(
                out,
                "typedb_successful_requests_total{{database=\"{}\", kind=\"{}\"}} {}",
                db.0, action.name, action.successful
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
            writeln!(out, "typedb_error_total{{database=\"{}\", code=\"{}\"}} {}", db.0, err.code, err.count).unwrap();
        }
    }

    out
}
