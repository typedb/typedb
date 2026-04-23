/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Integration tests that drive the real dbt TypeDB adapter against a live
//! TypeDB pgwire listener backed by a seeded materialized catalog.

mod real_pgwire_fixture;

use real_pgwire_fixture::{run_dbt_command as run_dbt_command_with_addr, run_with_real_pgwire_fixture};

#[derive(Debug)]
struct DbtRunResult {
    status_code: Option<i32>,
    stdout: String,
    stderr: String,
    server_errors: String,
    query_batches: Vec<String>,
}

async fn run_dbt_command(subcommand: &str, subcommand_args: &[&str]) -> DbtRunResult {
    let run = run_with_real_pgwire_fixture(|addr| run_dbt_command_with_addr(addr, subcommand, subcommand_args)).await;

    DbtRunResult {
        status_code: run.value.status.code(),
        stdout: String::from_utf8_lossy(&run.value.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&run.value.stderr).into_owned(),
        server_errors: run.server_errors,
        query_batches: run.query_batches,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dbt_debug_can_connect_to_projection_pgwire_endpoint() {
    let run = run_dbt_command("debug", &[]).await;

    assert!(
        run.status_code == Some(0),
        "dbt debug should connect to the TypeDB pgwire endpoint.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_batches,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dbt_show_can_query_projection_rows() {
    let run = run_dbt_command("show", &["--inline", "select name from public.people order by id"]).await;

    assert!(
        run.status_code == Some(0),
        "dbt show should query the seeded projection rows through the TypeDB pgwire endpoint.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        run.status_code,
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_batches,
    );
    assert!(
        run.stdout.contains("Alice") && run.stdout.contains("Bob"),
        "dbt show output should contain the seeded projection rows.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        run.stdout,
        run.stderr,
        run.server_errors,
        run.query_batches,
    );
}
