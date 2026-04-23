/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Integration test that drives DuckDB's PostgreSQL extension against a
//! live TypeDB pgwire listener backed by a real materialized catalog.

mod real_pgwire_fixture;

use real_pgwire_fixture::{run_duckdb_command, run_with_real_pgwire_fixture, split_sql_statements_preserving_text};

struct DuckdbRunResult {
    listener_port: u16,
    status_code: Option<i32>,
    stdout: String,
    stderr: String,
    database_file_created: bool,
    temp_dir_removed: bool,
    server_errors: String,
    query_batches: Vec<String>,
    query_statements: Vec<String>,
}

async fn run_duckdb_attach_and_capture() -> DuckdbRunResult {
    run_duckdb_sql_and_capture("SHOW ALL TABLES;").await
}

async fn run_duckdb_sql_and_capture(tail_sql: &str) -> DuckdbRunResult {
    let run = run_with_real_pgwire_fixture(|addr| {
        let listener_port = addr.rsplit_once(':').unwrap().1.parse::<u16>().unwrap();
        (run_duckdb_command(addr, tail_sql), listener_port)
    })
    .await;
    let query_statements =
        run.query_batches.iter().flat_map(|batch| split_sql_statements_preserving_text(batch).into_iter()).collect();

    DuckdbRunResult {
        listener_port: run.value.1,
        status_code: run.value.0.output.status.code(),
        stdout: String::from_utf8_lossy(&run.value.0.output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&run.value.0.output.stderr).into_owned(),
        database_file_created: run.value.0.database_file_created,
        temp_dir_removed: run.value.0.temp_dir_removed,
        server_errors: run.server_errors,
        query_batches: run.query_batches,
        query_statements,
    }
}

const EXPECTED_DUCKDB_ATTACH_STATEMENTS: &[&str] = &[
    "SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')",
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    r#"SELECT oid, nspname
FROM pg_namespace

ORDER BY oid"#,
    r#"SELECT pg_namespace.oid AS namespace_id, relname, relpages, attname,
    pg_type.typname type_name, atttypmod type_modifier, pg_attribute.attndims ndim,
    attnum, pg_attribute.attnotnull AS notnull, NULL constraint_id,
    NULL constraint_type, NULL constraint_key
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
JOIN pg_type ON atttypid=pg_type.oid
WHERE attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p') 
UNION ALL
SELECT pg_namespace.oid AS namespace_id, relname, NULL relpages, NULL attname, NULL type_name,
    NULL type_modifier, NULL ndim, NULL attnum, NULL AS notnull,
    pg_constraint.oid AS constraint_id, contype AS constraint_type,
    conkey AS constraint_key
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_constraint ON (pg_class.oid=pg_constraint.conrelid)
WHERE relkind IN ('r', 'v', 'm', 'f', 'p') AND contype IN ('p', 'u') 
ORDER BY namespace_id, relname, attnum, constraint_id"#,
    r#"SELECT 0 AS oid, 0 AS enumtypid, '' AS typname, '' AS enumlabel
LIMIT 0"#,
    r#"SELECT n.oid, t.typrelid AS id, t.typname as type, pg_attribute.attname, sub_type.typname
FROM pg_type t
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
JOIN pg_class ON pg_class.oid = t.typrelid
JOIN pg_attribute ON attrelid=t.typrelid
JOIN pg_type sub_type ON (pg_attribute.atttypid=sub_type.oid)
WHERE pg_class.relkind = 'c'
AND t.typtype='c'

ORDER BY n.oid, t.oid, attrelid, attnum"#,
    r#"SELECT pg_namespace.oid, tablename, indexname
FROM pg_indexes
JOIN pg_namespace ON (schemaname=nspname)

ORDER BY pg_namespace.oid"#,
    "COMMIT",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_attach_and_discover_projection_tables() {
    let run = run_duckdb_attach_and_capture().await;

    assert!(run.database_file_created, "DuckDB test fixture should create a file-backed temporary database.");
    assert!(run.temp_dir_removed, "DuckDB test fixture should remove its temporary database directory after the run.");
    assert!(
        run.status_code == Some(0),
        "DuckDB should attach to the TypeDB pgwire endpoint and discover projection tables.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
    );
    assert!(
        run.stdout.lines().any(|line| line.contains("people")),
        "DuckDB table discovery should include the seeded projection.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}",
        run.stdout,
        run.stderr,
        run.server_errors,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_attach_emits_expected_introspection_statements() {
    let run = run_duckdb_attach_and_capture().await;

    assert!(
        run.status_code == Some(0),
        "DuckDB attach must succeed before introspection SQL can be asserted.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
    );

    let expected_statements: Vec<String> =
        EXPECTED_DUCKDB_ATTACH_STATEMENTS.iter().map(|statement| statement.to_string()).collect();
    let actual_statements = run.query_statements;
    let actual_batches = run.query_batches;

    assert_eq!(
        &actual_statements,
        &expected_statements,
        "DuckDB attach SQL should be captured as the exact statement sequence DuckDB emits.\nactual_statements:\n{:#?}\nactual_batches:\n{:#?}",
        actual_statements,
        actual_batches,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_filter_order_and_limit() {
    let run =
        run_duckdb_sql_and_capture("SELECT name FROM typedb.public.people WHERE id = 2 ORDER BY name LIMIT 1;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query attached projections with filter, ordering, and limit.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
    assert_eq!(
        run.stdout.trim(),
        "Bob",
        "DuckDB query result should contain the filtered row only.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_ordering_and_limit_without_filter() {
    let run = run_duckdb_sql_and_capture("SELECT name FROM typedb.public.people ORDER BY name DESC LIMIT 1;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query attached projections with ordering and limit even when the full scan is executed remotely.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
    assert_eq!(
        run.stdout.trim(),
        "Bob",
        "DuckDB ordering and limit should return the descending top row.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_scan_multiple_projection_rows() {
    let run = run_duckdb_sql_and_capture("SELECT name FROM typedb.public.people;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to scan multiple rows from an attached projection.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    let actual_rows: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        actual_rows,
        vec!["Alice", "Bob"],
        "DuckDB scan should return both seeded rows in scan order.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_scan_mixed_projection_columns() {
    let run = run_duckdb_sql_and_capture("SELECT id, name FROM typedb.public.people ORDER BY id;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to scan mixed integer and text projection columns from an attached projection.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    let actual_rows: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        actual_rows,
        vec!["1,Alice", "2,Bob"],
        "DuckDB scan should return both seeded rows with mixed projection column types.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_cast_projection() {
    let run =
        run_duckdb_sql_and_capture("SELECT CAST(id AS VARCHAR) AS id_text FROM typedb.public.people ORDER BY id;")
            .await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query attached projections with cast projections.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    let actual_rows: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        actual_rows,
        vec!["1", "2"],
        "DuckDB cast projection should return the cast integer values as text.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_row_count() {
    let run = run_duckdb_sql_and_capture("SELECT COUNT(*) FROM typedb.public.people;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query aggregate row counts over attached projections.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert_eq!(
        run.stdout.trim(),
        "2",
        "DuckDB aggregate row count should match the seeded projection row count.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_schema_probe_with_constant_false_filter() {
    let run = run_duckdb_sql_and_capture("SELECT * FROM typedb.public.people WHERE 1 = 0;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to run schema-probe style constant-false filters over attached projections.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert!(
        run.stdout.trim().is_empty(),
        "DuckDB schema-probe constant-false filters should return no rows.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_row_count_with_constant_false_filter() {
    let run = run_duckdb_sql_and_capture("SELECT COUNT(*) FROM typedb.public.people WHERE 1 = 0;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query aggregate row counts over attached projections with schema-probe style constant-false filters.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert_eq!(
        run.stdout.trim(),
        "0",
        "DuckDB aggregate row count with a constant-false filter should be zero.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_row_count_with_is_not_null_filter() {
    let run = run_duckdb_sql_and_capture("SELECT COUNT(*) FROM typedb.public.people WHERE name IS NOT NULL;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query aggregate row counts over attached projections with IS NOT NULL filters.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert_eq!(
        run.stdout.trim(),
        "2",
        "DuckDB aggregate row count with an IS NOT NULL filter should count both seeded rows.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_quoted_identifiers() {
    let run =
        run_duckdb_sql_and_capture("SELECT \"name\" FROM typedb.public.people ORDER BY \"name\" DESC LIMIT 1;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query attached projections with quoted identifiers.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert_eq!(
        run.stdout.trim(),
        "Bob",
        "DuckDB quoted-identifier queries should return the expected row.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_alias_ordering() {
    let run = run_duckdb_sql_and_capture(
        "SELECT name AS full_name FROM typedb.public.people ORDER BY full_name DESC LIMIT 1;",
    )
    .await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query attached projections with alias ordering.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert_eq!(
        run.stdout.trim(),
        "Bob",
        "DuckDB alias-ordering queries should return the descending top row.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_projection_rows_with_limit_zero() {
    let run = run_duckdb_sql_and_capture("SELECT * FROM typedb.public.people LIMIT 0;").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to run LIMIT 0 metadata-style queries over attached projections.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );

    assert!(
        run.stdout.trim().is_empty(),
        "DuckDB LIMIT 0 queries should return no rows.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_active_session_row_through_pg_stat_activity() {
    let run =
        run_duckdb_sql_and_capture("SELECT datname, usename, pid FROM typedb.pg_catalog.pg_stat_activity LIMIT 1;")
            .await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query active session metadata through pg_stat_activity on the attached TypeDB pgwire endpoint.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
    assert_eq!(
        run.stdout.trim(),
        "typedb,admin,1",
        "DuckDB pg_stat_activity queries should expose the active database, user, and backend PID for the attached session.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_can_query_active_server_port_through_pg_settings() {
    let run = run_duckdb_sql_and_capture("SELECT setting FROM typedb.pg_catalog.pg_settings WHERE name = 'port';").await;

    assert!(
        run.status_code == Some(0),
        "DuckDB should be able to query server-port metadata through pg_settings on the attached TypeDB pgwire endpoint.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
    );
    assert_eq!(
        run.stdout.trim(),
        run.listener_port.to_string(),
        "DuckDB pg_settings queries should expose the active listener port for the attached session.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_statements:\n{:#?}\nlistener_port: {}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_statements,
        run.listener_port,
    );
}
