/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use diagnostics::{
    Diagnostics, hash_string_consistently,
    metrics::{
        ActionKind, ClientEndpoint, DataLoadMetrics, DatabaseMetricsSnapshot, LoadKind, QueryType, SchemaLoadMetrics,
        TransactionOutcome,
    },
};

const SECRET_NAME: &str = "TopSecretCustomerDatabase";

fn build_populated_diagnostics(is_reporting_enabled: bool) -> Diagnostics {
    let diagnostics = Diagnostics::new(
        "deploy".into(),
        "server".into(),
        "edition".into(),
        "version".into(),
        PathBuf::from("/tmp/diagnostics-test"),
        is_reporting_enabled,
        /* metrics_enabled */ true,
    );
    let name: Arc<str> = SECRET_NAME.into();

    let mut snapshots = HashMap::new();
    snapshots.insert(
        name.clone(),
        DatabaseMetricsSnapshot {
            schema: SchemaLoadMetrics { type_count: 7 },
            data: DataLoadMetrics { entity_count: 42, ..Default::default() },
        },
    );
    diagnostics.submit_database_metrics(snapshots);

    diagnostics.record_transaction_outcome(&name, LoadKind::WriteTransactions, TransactionOutcome::Committed);
    diagnostics.observe_query_duration(&name, QueryType::Write, Duration::from_millis(10));
    diagnostics.observe_transaction_duration(&name, LoadKind::WriteTransactions, Duration::from_millis(50));
    diagnostics.observe_queries_per_transaction(&name, 3);
    diagnostics.increment_load_count(ClientEndpoint::Grpc, &name, LoadKind::WriteTransactions);
    diagnostics.submit_action_success(ClientEndpoint::Grpc, Some(&name), ActionKind::TransactionOpen);
    diagnostics.submit_error(ClientEndpoint::Http, Some(&name), "ERR-1".into());

    diagnostics
}

#[test]
fn posthog_full_report_carries_hash_not_name() {
    let diagnostics = build_populated_diagnostics(/* is_reporting_enabled */ true);
    let payload = diagnostics.to_posthog_reporting_json_against_snapshot("test-key");
    let serialized = serde_json::to_string(&payload).unwrap();

    assert!(!serialized.contains(SECRET_NAME), "Posthog payload leaked the database name. Full payload:\n{serialized}");

    let expected_hash = hash_string_consistently(SECRET_NAME).to_string();
    assert!(
        serialized.contains(&expected_hash),
        "Posthog payload did not include the database hash — test setup may not have populated the database. \
         Full payload:\n{serialized}"
    );
}

#[test]
fn posthog_minimal_report_carries_hash_not_name() {
    let diagnostics = build_populated_diagnostics(/* is_reporting_enabled */ false);
    let payload = diagnostics.to_posthog_reporting_json_against_snapshot("test-key");
    let serialized = serde_json::to_string(&payload).unwrap();

    assert!(!serialized.contains(SECRET_NAME), "Minimal Posthog payload leaked the database name:\n{serialized}");
}
