/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(dead_code)]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use encoding::value::value_type::ValueTypeCategory;
use projection::{
    catalog::MaterializedCatalog,
    definition::{ColumnDefinition, ProjectionDefinition},
    pgwire::{
        connection::{PgConnection, QueryHandler, QueryOutcome, ServerParams, SessionContext},
        query_executor::{execute_raw_query_batch_with_session, execute_raw_query_with_session},
    },
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};

#[derive(Debug)]
struct RealCatalogHandler {
    catalog: MaterializedCatalog,
    query_batches: Arc<Mutex<Vec<String>>>,
}

impl RealCatalogHandler {
    fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog, query_batches: Arc::new(Mutex::new(Vec::new())) }
    }

    fn query_batches_handle(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.query_batches)
    }
}

impl QueryHandler for RealCatalogHandler {
    fn handle_query(&self, sql: &str) -> QueryOutcome {
        execute_raw_query_with_session(&self.catalog, sql, &SessionContext::default())
    }

    fn handle_query_with_session(&self, sql: &str, session: &SessionContext) -> QueryOutcome {
        execute_raw_query_with_session(&self.catalog, sql, session)
    }

    fn handle_query_batch_with_session(&self, sql: &str, session: &SessionContext) -> Vec<QueryOutcome> {
        self.query_batches.lock().unwrap().push(sql.to_string());
        execute_raw_query_batch_with_session(&self.catalog, sql, session)
    }
}

pub struct FixtureRunResult<T> {
    pub value: T,
    pub server_errors: String,
    pub query_batches: Vec<String>,
}

pub struct DuckdbCommandResult {
    pub output: Output,
    pub database_file_created: bool,
    pub temp_dir_removed: bool,
}

fn seeded_catalog() -> MaterializedCatalog {
    let catalog = MaterializedCatalog::new();
    let definition = ProjectionDefinition::new(
        "people",
        vec![
            ColumnDefinition::new("id", ValueTypeCategory::Integer),
            ColumnDefinition::new("name", ValueTypeCategory::String),
        ],
        "match $p isa person; fetch { $p.id; $p.name; };",
    )
    .unwrap();
    catalog.register(definition);
    assert!(catalog.replace_rows(
        "people",
        vec![
            vec![Some("1".to_string()), Some("Alice".to_string())],
            vec![Some("2".to_string()), Some("Bob".to_string())],
        ]
    ));
    catalog
}

pub async fn run_with_real_pgwire_fixture<T, F>(runner: F) -> FixtureRunResult<T>
where
    F: FnOnce(&str) -> T,
{
    let handler = Arc::new(RealCatalogHandler::new(seeded_catalog()));
    let query_batches = handler.query_batches_handle();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (error_tx, mut error_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut shutdown_rx = shutdown_rx;
        loop {
            tokio::select! {
                biased;

                result = shutdown_rx.changed() => {
                    let _ = result;
                    break;
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            let handler = Arc::clone(&handler);
                            let error_tx = error_tx.clone();
                            let server_port = stream.local_addr().unwrap().port();
                            tokio::spawn(async move {
                                let mut conn = PgConnection::new(stream, handler, ServerParams::default())
                                    .with_backend_key(1, 0)
                                    .with_server_port(server_port);
                                if let Err(err) = conn.run().await {
                                    let _ = error_tx.send(err.to_string());
                                }
                            });
                        }
                        Err(err) => {
                            let _ = error_tx.send(err.to_string());
                            break;
                        }
                    }
                }
            }
        }
    });

    let value = runner(&addr);

    let _ = shutdown_tx.send(true);
    let _ = server.await;

    let mut server_errors = Vec::new();
    while let Ok(err) = error_rx.try_recv() {
        if err != "Connection closed" {
            server_errors.push(err);
        }
    }

    let query_batches = query_batches.lock().unwrap().clone();

    FixtureRunResult { value, server_errors: server_errors.join("\n"), query_batches }
}

#[allow(dead_code)]
pub fn split_sql_statements_preserving_text(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = sql.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '\'' if !in_double_quote => {
                if in_single_quote {
                    if matches!(chars.peek(), Some((_, '\''))) {
                        let _ = chars.next();
                    } else {
                        in_single_quote = false;
                    }
                } else {
                    in_single_quote = true;
                }
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            ';' if !in_single_quote && !in_double_quote => {
                let statement = sql[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    let trailing_statement = sql[start..].trim();
    if !trailing_statement.is_empty() {
        statements.push(trailing_statement.to_string());
    }

    statements
}

pub fn duckdb_sql(addr: &str, tail_sql: &str) -> String {
    let (_host, port) = addr.rsplit_once(':').expect("listener address should include a port");
    format!(
        "INSTALL postgres; \
         LOAD postgres; \
         ATTACH 'host=127.0.0.1 port={port} dbname=typedb user=admin password=password' AS typedb (TYPE postgres); \
         {tail_sql}"
    )
}

pub fn run_duckdb_command(addr: &str, tail_sql: &str) -> DuckdbCommandResult {
    let duckdb_dir = TempDir::new("typedb-duckdb");
    let duckdb_path = duckdb_dir.path().join("integration.duckdb");

    let output = Command::new("duckdb")
        .args(["-unsigned", "-batch", "-csv", "-noheader", "-no-stdin", "-c", &duckdb_sql(addr, tail_sql)])
        .arg(&duckdb_path)
        .output()
        .expect("duckdb CLI must be available on PATH for this integration test");
    let database_file_created = duckdb_path.is_file();
    drop(duckdb_dir);
    let temp_dir_removed = duckdb_path.parent().is_some_and(|path| !path.exists());

    DuckdbCommandResult { output, database_file_created, temp_dir_removed }
}

#[derive(Debug)]
enum DbtInvoker {
    Executable(PathBuf),
    Uv(PathBuf),
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            candidate.is_file().then_some(candidate)
        })
    })
}

fn find_dbt_invoker() -> Option<DbtInvoker> {
    let repo_root = repo_root();
    let local_windows = repo_root.join("tools").join("dbt-typedb").join(".venv").join("Scripts").join("dbt.exe");
    if local_windows.is_file() {
        return Some(DbtInvoker::Executable(local_windows));
    }

    let local_unix = repo_root.join("tools").join("dbt-typedb").join(".venv").join("bin").join("dbt");
    if local_unix.is_file() {
        return Some(DbtInvoker::Executable(local_unix));
    }

    if let Some(dbt) = find_on_path(if cfg!(windows) { "dbt.exe" } else { "dbt" }) {
        return Some(DbtInvoker::Executable(dbt));
    }

    find_on_path(if cfg!(windows) { "uv.exe" } else { "uv" }).map(DbtInvoker::Uv)
}

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn write_dbt_project(project_dir: &Path) {
    fs::write(
        project_dir.join("dbt_project.yml"),
        "name: typedb_pgwire_smoke\nversion: '1.0'\nconfig-version: 2\nprofile: typedb\nmodel-paths: ['models']\n",
    )
    .unwrap();
    fs::create_dir_all(project_dir.join("models")).unwrap();
}

fn write_dbt_profiles(profiles_dir: &Path, host: &str, port: u16) {
    fs::write(
        profiles_dir.join("profiles.yml"),
        format!(
            "typedb:\n  target: dev\n  outputs:\n    dev:\n      type: typedb\n      host: {host}\n      port: {port}\n      user: admin\n      pass: password\n      dbname: typedb\n      schema: public\n      threads: 1\n"
        ),
    )
    .unwrap();
}

fn dbt_tool_dir() -> PathBuf {
    repo_root().join("tools").join("dbt-typedb")
}

fn build_dbt_command(
    invoker: &DbtInvoker,
    project_dir: &Path,
    profiles_dir: &Path,
    subcommand: &str,
    subcommand_args: &[&str],
) -> Command {
    let mut command = match invoker {
        DbtInvoker::Executable(path) => Command::new(path),
        DbtInvoker::Uv(path) => {
            let mut command = Command::new(path);
            command.arg("run").arg("--directory").arg(dbt_tool_dir()).arg("dbt");
            command
        }
    };

    command
        .arg("--no-use-colors")
        .arg(subcommand)
        .arg("--project-dir")
        .arg(project_dir)
        .arg("--profiles-dir")
        .arg(profiles_dir)
        .args(subcommand_args);
    command
}

pub fn run_dbt_command(addr: &str, subcommand: &str, subcommand_args: &[&str]) -> Output {
    let invoker = find_dbt_invoker().expect("dbt must be available via tools/dbt-typedb/.venv, PATH, or uv");
    let project_dir = TempDir::new("typedb-dbt-project");
    let profiles_dir = TempDir::new("typedb-dbt-profiles");
    write_dbt_project(project_dir.path());

    let (_host, port) = addr.rsplit_once(':').expect("listener address should include a port");
    let port = port.parse::<u16>().expect("listener address should include a numeric port");
    write_dbt_profiles(profiles_dir.path(), "127.0.0.1", port);

    build_dbt_command(&invoker, project_dir.path(), profiles_dir.path(), subcommand, subcommand_args)
        .current_dir(repo_root())
        .output()
        .expect("dbt command must start successfully for this integration test")
}
