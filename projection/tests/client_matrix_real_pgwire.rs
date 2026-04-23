/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Combined live-client pgwire smoke coverage for raw PostgreSQL protocol,
//! dbt, and DuckDB against the same seeded projection fixture.

mod real_pgwire_fixture;

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use real_pgwire_fixture::{run_dbt_command, run_duckdb_command, run_with_real_pgwire_fixture};

use projection::pgwire::messages::{
    MSG_AUTHENTICATION, MSG_BACKEND_KEY_DATA, MSG_COMMAND_COMPLETE, MSG_DATA_ROW, MSG_PASSWORD, MSG_QUERY,
    MSG_READY_FOR_QUERY, MSG_ROW_DESCRIPTION, PROTOCOL_VERSION_30, TX_IDLE,
};

struct SimpleQueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    command_tag: Option<String>,
}

fn build_startup_packet(user: &str, database: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.push(0);

    let total_len = (4 + payload.len()) as i32;
    let mut packet = Vec::new();
    packet.extend_from_slice(&total_len.to_be_bytes());
    packet.extend_from_slice(&payload);
    packet
}

fn build_query_packet(sql: &str) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(MSG_QUERY);
    let len = (4 + sql.len() + 1) as i32;
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(sql.as_bytes());
    packet.push(0);
    packet
}

fn build_terminate_packet() -> Vec<u8> {
    vec![b'X', 0, 0, 0, 4]
}

fn read_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf) as usize;
    let payload_len = len - 4;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload)?;
    }

    Ok((msg_type, payload))
}

fn authenticate(addr: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(&build_startup_packet("admin", "typedb"))?;
    stream.flush()?;

    loop {
        let (msg_type, payload) = read_message(&mut stream)?;
        match msg_type {
            MSG_AUTHENTICATION => {
                let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if auth_type == 3 {
                    let mut pwd_msg = Vec::new();
                    pwd_msg.push(MSG_PASSWORD);
                    let pwd = b"password\0";
                    let len = (4 + pwd.len()) as i32;
                    pwd_msg.extend_from_slice(&len.to_be_bytes());
                    pwd_msg.extend_from_slice(pwd);
                    stream.write_all(&pwd_msg)?;
                    stream.flush()?;
                }
            }
            MSG_BACKEND_KEY_DATA => {}
            MSG_READY_FOR_QUERY => {
                assert_eq!(payload, vec![TX_IDLE], "expected ready-for-query idle after startup");
                break;
            }
            _ => {}
        }
    }

    Ok(stream)
}

fn decode_row_description(payload: &[u8]) -> Vec<String> {
    let field_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut fields = Vec::with_capacity(field_count);
    let mut pos = 2usize;

    for _ in 0..field_count {
        let end = pos + payload[pos..].iter().position(|&b| b == 0).unwrap();
        fields.push(String::from_utf8(payload[pos..end].to_vec()).unwrap());
        pos = end + 1 + 18;
    }

    fields
}

fn decode_data_row(payload: &[u8]) -> Vec<Option<String>> {
    let field_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut pos = 2usize;
    let mut fields = Vec::with_capacity(field_count);

    for _ in 0..field_count {
        let len = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        if len == -1 {
            fields.push(None);
        } else {
            let len = len as usize;
            fields.push(Some(String::from_utf8(payload[pos..pos + len].to_vec()).unwrap()));
            pos += len;
        }
    }

    fields
}

fn run_simple_query(addr: &str, sql: &str) -> std::io::Result<SimpleQueryResult> {
    let mut stream = authenticate(addr)?;
    stream.write_all(&build_query_packet(sql))?;
    stream.flush()?;

    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut command_tag = None;

    loop {
        let (msg_type, payload) = read_message(&mut stream)?;
        match msg_type {
            MSG_ROW_DESCRIPTION => columns = decode_row_description(&payload),
            MSG_DATA_ROW => rows.push(decode_data_row(&payload)),
            MSG_COMMAND_COMPLETE => {
                command_tag = Some(String::from_utf8(payload[..payload.len() - 1].to_vec()).unwrap());
            }
            MSG_READY_FOR_QUERY => {
                assert_eq!(payload, vec![TX_IDLE], "expected ready-for-query idle after simple query");
                break;
            }
            _ => {}
        }
    }

    stream.write_all(&build_terminate_packet())?;
    stream.flush()?;

    Ok(SimpleQueryResult { columns, rows, command_tag })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_pgwire_session_context_is_truthful() {
    let run = run_with_real_pgwire_fixture(|addr| {
        let server_port = addr.rsplit_once(':').unwrap().1.parse::<u16>().unwrap();
        let query = run_simple_query(
            addr,
            "SELECT current_database(), current_schema(), current_user, pg_backend_pid(), inet_server_port()",
        )
        .expect("raw pgwire query should succeed");
        (query, server_port)
    })
    .await;

    let (query, server_port) = run.value;
    assert!(run.server_errors.is_empty(), "server should not emit connection errors: {}", run.server_errors);
    assert_eq!(
        query.columns,
        vec!["current_database", "current_schema", "current_user", "pg_backend_pid", "inet_server_port"],
        "session function query should expose the expected columns"
    );
    assert_eq!(query.command_tag.as_deref(), Some("SELECT 1"), "simple query should complete with SELECT 1");
    assert_eq!(query.rows.len(), 1, "session function query should return exactly one row");
    assert_eq!(
        query.rows[0],
        vec![
            Some("typedb".to_string()),
            Some("public".to_string()),
            Some("admin".to_string()),
            Some("1".to_string()),
            Some(server_port.to_string()),
        ],
        "session function values should reflect the active connection context"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dbt_debug_and_show_succeed_against_seeded_projection() {
    let debug_run = run_with_real_pgwire_fixture(|addr| run_dbt_command(addr, "debug", &[])).await;
    assert!(
        debug_run.value.status.code() == Some(0),
        "dbt debug should succeed in the combined client matrix target.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        debug_run.value.status.code(),
        String::from_utf8_lossy(&debug_run.value.stdout),
        String::from_utf8_lossy(&debug_run.value.stderr),
        debug_run.server_errors,
        debug_run.query_batches,
    );

    let show_run = run_with_real_pgwire_fixture(|addr| {
        run_dbt_command(addr, "show", &["--inline", "select name from public.people order by id"])
    })
    .await;
    let show_stdout = String::from_utf8_lossy(&show_run.value.stdout).into_owned();
    let show_stderr = String::from_utf8_lossy(&show_run.value.stderr).into_owned();

    assert!(
        show_run.value.status.code() == Some(0),
        "dbt show should succeed in the combined client matrix target.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        show_run.value.status.code(),
        show_stdout,
        show_stderr,
        show_run.server_errors,
        show_run.query_batches,
    );
    assert!(
        show_stdout.contains("Alice") && show_stdout.contains("Bob"),
        "dbt show output should contain the seeded projection rows.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        show_stdout,
        show_stderr,
        show_run.server_errors,
        show_run.query_batches,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_attach_scan_filter_and_detach_succeeds() {
    let run = run_with_real_pgwire_fixture(|addr| {
        run_duckdb_command(
            addr,
            "SELECT name FROM typedb.public.people ORDER BY id; SELECT name FROM typedb.public.people WHERE id = 2; DETACH typedb;",
        )
    })
    .await;

    let stdout = String::from_utf8_lossy(&run.value.output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&run.value.output.stderr).into_owned();

    assert!(run.value.database_file_created, "DuckDB combined smoke should run against a file-backed temporary database.");
    assert!(run.value.temp_dir_removed, "DuckDB combined smoke should remove its temporary database directory after the run.");
    assert!(
        run.value.output.status.code() == Some(0),
        "DuckDB attach/scan/filter/detach should succeed in the combined client matrix target.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        run.value.output.status.code(),
        stdout,
        stderr,
        run.server_errors,
        run.query_batches,
    );
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["Alice", "Bob", "Bob"],
        "DuckDB combined smoke output should cover the full scan and filtered row before detach.\nstdout:\n{}\nstderr:\n{}\nserver_errors:\n{}\nquery_batches:\n{:#?}",
        stdout,
        stderr,
        run.server_errors,
        run.query_batches,
    );
}
