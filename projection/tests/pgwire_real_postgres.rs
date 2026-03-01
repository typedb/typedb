/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Integration tests that connect to a real Postgres instance via raw TCP
//! and compare the wire bytes it sends against our encoding functions.
//!
//! Prerequisites:
//!   wsl -- podman run -d --name pgwire-test -e POSTGRES_PASSWORD=test -p 5433:5432 postgres:16-alpine
//!
//! Run with:
//!   cargo test -p projection --test pgwire_real_postgres -- --nocapture

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use projection::pgwire::messages::*;

// ── Helpers ────────────────────────────────────────────────────────

/// Default address; override with PG_TEST_ADDR env var.
fn pg_addr() -> String {
    std::env::var("PG_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:5433".to_string())
}

/// Build a raw StartupMessage to send to Postgres.
/// Format: [4-byte length] [4-byte protocol version] [key\0value\0...] [\0]
fn build_startup_packet(user: &str, database: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.push(0); // terminator

    let total_len = (4 + payload.len()) as i32; // length includes itself
    let mut packet = Vec::new();
    packet.extend_from_slice(&total_len.to_be_bytes());
    packet.extend_from_slice(&payload);
    packet
}

/// Build a raw Query message: 'Q' + 4-byte length + SQL\0
fn build_query_packet(sql: &str) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(MSG_QUERY);
    let len = (4 + sql.len() + 1) as i32; // length includes self + sql + null
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(sql.as_bytes());
    packet.push(0);
    packet
}

/// Build a raw Terminate message: 'X' + 4-byte length (4)
fn build_terminate_packet() -> Vec<u8> {
    vec![MSG_TERMINATE, 0, 0, 0, 4]
}

/// Read exactly one wire protocol message from the stream.
/// Returns (type_byte, payload) where payload does NOT include the type byte or length.
fn read_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf) as usize;

    // len includes itself (4 bytes) but not the type byte
    let payload_len = len - 4;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload)?;
    }

    Ok((msg_type, payload))
}

/// Reconstruct the full wire message (type + length + payload) from read_message output.
fn reconstruct_message(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(msg_type);
    let len = (4 + payload.len()) as i32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(payload);
    msg
}

/// Connect to Postgres, send startup, handle auth, return stream positioned after ReadyForQuery.
fn connect_and_authenticate() -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(&pg_addr())?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Send startup
    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup)?;
    stream.flush()?;

    // Read messages until ReadyForQuery
    loop {
        let (msg_type, payload) = read_message(&mut stream)?;
        match msg_type {
            MSG_AUTHENTICATION => {
                let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if auth_type == AUTH_CLEARTEXT_PASSWORD {
                    // Send password
                    let mut pwd_msg = Vec::new();
                    pwd_msg.push(MSG_PASSWORD);
                    let pwd = b"test\0";
                    let len = (4 + pwd.len()) as i32;
                    pwd_msg.extend_from_slice(&len.to_be_bytes());
                    pwd_msg.extend_from_slice(pwd);
                    stream.write_all(&pwd_msg)?;
                    stream.flush()?;
                } else if auth_type == AUTH_MD5_PASSWORD {
                    // For MD5, we'd need to compute the hash. For trust auth, auth_type = 0.
                    // Skip — if we see this, the test environment needs adjustment.
                    panic!("MD5 auth not supported in this test; configure pg_hba.conf to use trust or password");
                }
                // AUTH_OK => continue reading
            }
            MSG_READY_FOR_QUERY => {
                break;
            }
            _ => {
                // ParameterStatus, BackendKeyData, etc. — continue
            }
        }
    }

    Ok(stream)
}

// ── Tests ──────────────────────────────────────────────────────────

#[test]
fn startup_handshake_message_types_match() {
    let addr = pg_addr();
    let mut stream = TcpStream::connect(&addr)
        .unwrap_or_else(|e| panic!("Cannot connect to Postgres on {} — is the container running? {}", addr, e));
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup).unwrap();
    stream.flush().unwrap();

    // Collect all messages until ReadyForQuery
    let mut message_types = Vec::new();
    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();
        message_types.push(msg_type);

        if msg_type == MSG_AUTHENTICATION {
            let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if auth_type == AUTH_CLEARTEXT_PASSWORD || auth_type == AUTH_MD5_PASSWORD {
                // Send password for cleartext
                let mut pwd_msg = Vec::new();
                pwd_msg.push(MSG_PASSWORD);
                let pwd = b"test\0";
                let len = (4 + pwd.len()) as i32;
                pwd_msg.extend_from_slice(&len.to_be_bytes());
                pwd_msg.extend_from_slice(pwd);
                stream.write_all(&pwd_msg).unwrap();
                stream.flush().unwrap();
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    println!("Startup handshake message types: {:?}", message_types.iter().map(|&b| b as char).collect::<Vec<_>>());

    // Must contain at least: R (auth), S (param status), K (backend key), Z (ready)
    assert!(message_types.contains(&MSG_AUTHENTICATION), "Missing Authentication message");
    assert!(message_types.contains(&MSG_PARAMETER_STATUS), "Missing ParameterStatus message");
    assert!(message_types.contains(&MSG_BACKEND_KEY_DATA), "Missing BackendKeyData message");
    assert!(message_types.contains(&MSG_READY_FOR_QUERY), "Missing ReadyForQuery message");

    // ReadyForQuery must be last
    assert_eq!(*message_types.last().unwrap(), MSG_READY_FOR_QUERY);
}

#[test]
fn auth_ok_bytes_match_postgres() {
    let mut stream = TcpStream::connect(&pg_addr()).expect("Cannot connect to Postgres");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup).unwrap();
    stream.flush().unwrap();

    // Find the AuthenticationOk message (auth_type = 0)
    let mut found_auth_ok = false;
    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_AUTHENTICATION {
            let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if auth_type == AUTH_CLEARTEXT_PASSWORD || auth_type == AUTH_MD5_PASSWORD {
                let mut pwd_msg = Vec::new();
                pwd_msg.push(MSG_PASSWORD);
                let pwd = b"test\0";
                let len = (4 + pwd.len()) as i32;
                pwd_msg.extend_from_slice(&len.to_be_bytes());
                pwd_msg.extend_from_slice(pwd);
                stream.write_all(&pwd_msg).unwrap();
                stream.flush().unwrap();
            } else if auth_type == AUTH_OK {
                // Compare against our encoding
                let our_auth_ok = encode_auth_ok();
                let pg_auth_ok = reconstruct_message(msg_type, &payload);
                assert_eq!(
                    our_auth_ok, pg_auth_ok,
                    "AuthenticationOk mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
                    our_auth_ok, pg_auth_ok
                );
                found_auth_ok = true;
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    assert!(found_auth_ok, "Never received AuthenticationOk from Postgres");
}

#[test]
fn parameter_status_structure_matches_postgres() {
    let mut stream = TcpStream::connect(&pg_addr()).expect("Cannot connect to Postgres");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup).unwrap();
    stream.flush().unwrap();

    let mut param_statuses: Vec<(String, String, Vec<u8>)> = Vec::new();
    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_AUTHENTICATION {
            let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if auth_type == AUTH_CLEARTEXT_PASSWORD || auth_type == AUTH_MD5_PASSWORD {
                let mut pwd_msg = Vec::new();
                pwd_msg.push(MSG_PASSWORD);
                let pwd = b"test\0";
                let len = (4 + pwd.len()) as i32;
                pwd_msg.extend_from_slice(&len.to_be_bytes());
                pwd_msg.extend_from_slice(pwd);
                stream.write_all(&pwd_msg).unwrap();
                stream.flush().unwrap();
            }
        }

        if msg_type == MSG_PARAMETER_STATUS {
            // Parse key\0value\0 from payload
            let key_end = payload.iter().position(|&b| b == 0).unwrap();
            let key = String::from_utf8(payload[..key_end].to_vec()).unwrap();
            let value_start = key_end + 1;
            let value_end = value_start + payload[value_start..].iter().position(|&b| b == 0).unwrap();
            let value = String::from_utf8(payload[value_start..value_end].to_vec()).unwrap();

            let pg_raw = reconstruct_message(msg_type, &payload);
            param_statuses.push((key, value, pg_raw));
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    assert!(!param_statuses.is_empty(), "No ParameterStatus messages received");
    println!("Received {} ParameterStatus messages:", param_statuses.len());

    // For each param status from Postgres, encode our version and compare byte-for-byte
    for (key, value, pg_raw) in &param_statuses {
        let our_msg = encode_parameter_status(key, value);
        println!("  {}={} => match={}", key, value, our_msg == *pg_raw);
        assert_eq!(
            our_msg, *pg_raw,
            "ParameterStatus mismatch for {}={}!\n  ours: {:02x?}\n  pg:   {:02x?}",
            key, value, our_msg, pg_raw
        );
    }
}

#[test]
fn backend_key_data_structure_matches_postgres() {
    let mut stream = TcpStream::connect(&pg_addr()).expect("Cannot connect to Postgres");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup).unwrap();
    stream.flush().unwrap();

    let mut found_key_data = false;
    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_AUTHENTICATION {
            let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if auth_type == AUTH_CLEARTEXT_PASSWORD || auth_type == AUTH_MD5_PASSWORD {
                let mut pwd_msg = Vec::new();
                pwd_msg.push(MSG_PASSWORD);
                let pwd = b"test\0";
                let len = (4 + pwd.len()) as i32;
                pwd_msg.extend_from_slice(&len.to_be_bytes());
                pwd_msg.extend_from_slice(pwd);
                stream.write_all(&pwd_msg).unwrap();
                stream.flush().unwrap();
            }
        }

        if msg_type == MSG_BACKEND_KEY_DATA {
            // Payload is 8 bytes: 4-byte PID + 4-byte secret key
            assert_eq!(payload.len(), 8, "BackendKeyData payload should be 8 bytes");
            let pid = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let secret = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            println!("BackendKeyData: pid={}, secret={}", pid, secret);

            // Re-encode with same values and compare
            let our_msg = encode_backend_key_data(pid, secret);
            let pg_raw = reconstruct_message(msg_type, &payload);
            assert_eq!(our_msg, pg_raw, "BackendKeyData mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_msg, pg_raw);
            found_key_data = true;
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    assert!(found_key_data, "Never received BackendKeyData from Postgres");
}

#[test]
fn ready_for_query_bytes_match_postgres() {
    let mut stream = TcpStream::connect(&pg_addr()).expect("Cannot connect to Postgres");
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let startup = build_startup_packet("postgres", "postgres");
    stream.write_all(&startup).unwrap();
    stream.flush().unwrap();

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_AUTHENTICATION {
            let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if auth_type == AUTH_CLEARTEXT_PASSWORD || auth_type == AUTH_MD5_PASSWORD {
                let mut pwd_msg = Vec::new();
                pwd_msg.push(MSG_PASSWORD);
                let pwd = b"test\0";
                let len = (4 + pwd.len()) as i32;
                pwd_msg.extend_from_slice(&len.to_be_bytes());
                pwd_msg.extend_from_slice(pwd);
                stream.write_all(&pwd_msg).unwrap();
                stream.flush().unwrap();
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            assert_eq!(payload.len(), 1, "ReadyForQuery payload should be 1 byte");
            assert_eq!(payload[0], TX_IDLE, "Initial ReadyForQuery should be Idle ('I')");

            let our_msg = encode_ready_for_query(TX_IDLE);
            let pg_raw = reconstruct_message(msg_type, &payload);
            assert_eq!(our_msg, pg_raw, "ReadyForQuery mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_msg, pg_raw);
            break;
        }
    }
}

#[test]
fn query_select_1_row_description_matches() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    // Send: SELECT 1 AS num, 'hello'::text AS greeting
    let query = build_query_packet("SELECT 1 AS num, 'hello'::text AS greeting");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    // Read messages: expect T (RowDescription), D (DataRow), C (CommandComplete), Z (ReadyForQuery)
    let mut message_types = Vec::new();
    let mut row_desc_raw: Option<Vec<u8>> = None;
    let mut row_desc_columns: Vec<(String, u32, i16, u32, i16, i32, i16)> = Vec::new();

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();
        message_types.push(msg_type);

        if msg_type == MSG_ROW_DESCRIPTION {
            row_desc_raw = Some(reconstruct_message(msg_type, &payload));

            // Parse column count
            let col_count = i16::from_be_bytes([payload[0], payload[1]]);
            let mut pos = 2;
            for _ in 0..col_count {
                // Column name (null-terminated)
                let name_end = payload[pos..].iter().position(|&b| b == 0).unwrap();
                let name = String::from_utf8(payload[pos..pos + name_end].to_vec()).unwrap();
                pos += name_end + 1;

                let table_oid =
                    u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                let col_idx = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
                pos += 2;
                let type_oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                let type_size = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
                pos += 2;
                let type_mod = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                let fmt_code = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
                pos += 2;

                println!(
                    "Column: name={}, table_oid={}, col_idx={}, type_oid={}, type_size={}, type_mod={}, fmt={}",
                    name, table_oid, col_idx, type_oid, type_size, type_mod, fmt_code
                );
                row_desc_columns.push((name, table_oid, col_idx, type_oid, type_size, type_mod, fmt_code));
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    println!("Query response message types: {:?}", message_types.iter().map(|&b| b as char).collect::<Vec<_>>());

    // Verify message sequence: T, D, C, Z
    assert!(message_types.contains(&MSG_ROW_DESCRIPTION), "Missing RowDescription");
    assert!(message_types.contains(&MSG_DATA_ROW), "Missing DataRow");
    assert!(message_types.contains(&MSG_COMMAND_COMPLETE), "Missing CommandComplete");
    assert_eq!(*message_types.last().unwrap(), MSG_READY_FOR_QUERY);

    // Re-encode RowDescription with the values Postgres sent and compare byte-for-byte
    let columns: Vec<ColumnDescription> = row_desc_columns
        .iter()
        .map(|(name, table_oid, col_idx, type_oid, type_size, type_mod, fmt)| ColumnDescription {
            name: name.clone(),
            table_oid: *table_oid,
            column_index: *col_idx,
            type_oid: *type_oid,
            type_size: *type_size,
            type_modifier: *type_mod,
            format_code: *fmt,
        })
        .collect();

    let our_row_desc = encode_row_description(&columns);
    let pg_row_desc = row_desc_raw.unwrap();
    assert_eq!(
        our_row_desc,
        pg_row_desc,
        "RowDescription mismatch!\n  ours ({} bytes): {:02x?}\n  pg   ({} bytes): {:02x?}",
        our_row_desc.len(),
        our_row_desc,
        pg_row_desc.len(),
        pg_row_desc
    );
}

#[test]
fn query_select_1_data_row_matches() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    let query = build_query_packet("SELECT 42 AS answer");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    let mut data_row_raw: Option<Vec<u8>> = None;
    let mut data_row_values: Vec<Option<String>> = Vec::new();

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_DATA_ROW {
            data_row_raw = Some(reconstruct_message(msg_type, &payload));

            let field_count = i16::from_be_bytes([payload[0], payload[1]]);
            let mut pos = 2;
            for _ in 0..field_count {
                let field_len =
                    i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                if field_len == -1 {
                    data_row_values.push(None);
                } else {
                    let val = String::from_utf8(payload[pos..pos + field_len as usize].to_vec()).unwrap();
                    data_row_values.push(Some(val));
                    pos += field_len as usize;
                }
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    println!("DataRow values: {:?}", data_row_values);
    assert_eq!(data_row_values, vec![Some("42".into())]);

    // Re-encode and compare
    let values_refs: Vec<Option<&str>> = data_row_values.iter().map(|v| v.as_deref()).collect();
    let our_data_row = encode_data_row(&values_refs);
    let pg_data_row = data_row_raw.unwrap();
    assert_eq!(
        our_data_row, pg_data_row,
        "DataRow mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_data_row, pg_data_row
    );
}

#[test]
fn query_select_null_data_row_matches() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    let query = build_query_packet("SELECT NULL::text AS empty");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    let mut data_row_raw: Option<Vec<u8>> = None;
    let mut data_row_values: Vec<Option<String>> = Vec::new();

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_DATA_ROW {
            data_row_raw = Some(reconstruct_message(msg_type, &payload));

            let field_count = i16::from_be_bytes([payload[0], payload[1]]);
            let mut pos = 2;
            for _ in 0..field_count {
                let field_len =
                    i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                if field_len == -1 {
                    data_row_values.push(None);
                } else {
                    let val = String::from_utf8(payload[pos..pos + field_len as usize].to_vec()).unwrap();
                    data_row_values.push(Some(val));
                    pos += field_len as usize;
                }
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    assert_eq!(data_row_values, vec![None]);

    let values_refs: Vec<Option<&str>> = data_row_values.iter().map(|v| v.as_deref()).collect();
    let our_data_row = encode_data_row(&values_refs);
    let pg_data_row = data_row_raw.unwrap();
    assert_eq!(our_data_row, pg_data_row, "DataRow NULL mismatch");
}

#[test]
fn command_complete_bytes_match_postgres() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    let query = build_query_packet("SELECT 1");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    let mut cmd_complete_raw: Option<Vec<u8>> = None;
    let mut cmd_tag: Option<String> = None;

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_COMMAND_COMPLETE {
            cmd_complete_raw = Some(reconstruct_message(msg_type, &payload));
            let tag_end = payload.iter().position(|&b| b == 0).unwrap();
            let tag = String::from_utf8(payload[..tag_end].to_vec()).unwrap();
            println!("CommandComplete tag: {:?}", tag);
            cmd_tag = Some(tag);
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    let tag = cmd_tag.unwrap();
    assert_eq!(tag, "SELECT 1"); // Postgres returns "SELECT <rowcount>"

    let our_msg = encode_command_complete(&tag);
    let pg_msg = cmd_complete_raw.unwrap();
    assert_eq!(our_msg, pg_msg, "CommandComplete mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_msg, pg_msg);
}

#[test]
fn error_response_structure_from_bad_query() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    // Send an intentionally bad query
    let query = build_query_packet("SELEKT bad_syntax");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    let mut error_payload: Option<Vec<u8>> = None;

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_ERROR_RESPONSE {
            error_payload = Some(payload.clone());

            // Parse fields from Postgres error
            let mut pos = 0;
            while pos < payload.len() && payload[pos] != 0 {
                let field_type = payload[pos] as char;
                pos += 1;
                let str_end = payload[pos..].iter().position(|&b| b == 0).unwrap();
                let value = String::from_utf8(payload[pos..pos + str_end].to_vec()).unwrap();
                pos += str_end + 1;
                println!("ErrorResponse field '{}': {}", field_type, value);
            }
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    let payload = error_payload.expect("No ErrorResponse received for bad query");

    // Verify structure: fields are [type_byte][string\0]... terminated by \0
    // Our encode_error_response only emits S, C, M fields — Postgres sends more (V, P, F, L, R, etc.)
    // So we can't compare byte-for-byte, but we can verify the *structure* is valid:

    // Extract S, C, M from Postgres error
    let mut severity = None;
    let mut code = None;
    let mut message = None;
    let mut pos = 0;
    while pos < payload.len() && payload[pos] != 0 {
        let field_type = payload[pos];
        pos += 1;
        let str_end = payload[pos..].iter().position(|&b| b == 0).unwrap();
        let value = String::from_utf8(payload[pos..pos + str_end].to_vec()).unwrap();
        pos += str_end + 1;
        match field_type {
            b'S' => severity = Some(value),
            b'C' => code = Some(value),
            b'M' => message = Some(value),
            _ => {}
        }
    }

    let sev = severity.expect("No severity field");
    let cod = code.expect("No code field");
    let msg = message.expect("No message field");

    // Re-encode with just S, C, M and verify our structure is parseable the same way
    let our_error = encode_error_response(&sev, &cod, &msg);

    // Verify our encoded message can be parsed back
    let our_payload = &our_error[5..]; // skip type + length
    let mut our_fields = Vec::new();
    let mut p = 0;
    while p < our_payload.len() && our_payload[p] != 0 {
        let ft = our_payload[p] as char;
        p += 1;
        let se = our_payload[p..].iter().position(|&b| b == 0).unwrap();
        let v = String::from_utf8(our_payload[p..p + se].to_vec()).unwrap();
        p += se + 1;
        our_fields.push((ft, v));
    }
    assert_eq!(our_fields.len(), 3, "Expected 3 fields (S, C, M)");
    assert_eq!(our_fields[0].0, 'S');
    assert_eq!(our_fields[0].1, sev);
    assert_eq!(our_fields[1].0, 'C');
    assert_eq!(our_fields[1].1, cod);
    assert_eq!(our_fields[2].0, 'M');
    assert_eq!(our_fields[2].1, msg);
}

#[test]
fn decode_startup_roundtrip_matches_what_we_send() {
    // Verify our decode_startup_message can parse the same bytes we build
    let startup = build_startup_packet("postgres", "postgres");
    // StartupMessage raw bytes: [4-byte total length][payload...]
    // Our decode_startup_message expects just the payload (after length)
    let payload = &startup[4..];

    let parsed = decode_startup_message(payload).unwrap();
    assert_eq!(parsed.protocol_version, PROTOCOL_VERSION_30);
    assert!(parsed.parameters.iter().any(|(k, v)| k == "user" && v == "postgres"));
    assert!(parsed.parameters.iter().any(|(k, v)| k == "database" && v == "postgres"));
}

#[test]
fn multiple_rows_data_row_matches() {
    let mut stream = connect_and_authenticate().expect("Cannot connect to Postgres");

    let query = build_query_packet("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS t(id, name)");
    stream.write_all(&query).unwrap();
    stream.flush().unwrap();

    let mut pg_data_rows: Vec<Vec<u8>> = Vec::new();
    let mut parsed_rows: Vec<Vec<Option<String>>> = Vec::new();

    loop {
        let (msg_type, payload) = read_message(&mut stream).unwrap();

        if msg_type == MSG_DATA_ROW {
            pg_data_rows.push(reconstruct_message(msg_type, &payload));

            let field_count = i16::from_be_bytes([payload[0], payload[1]]);
            let mut pos = 2;
            let mut row = Vec::new();
            for _ in 0..field_count {
                let field_len =
                    i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
                pos += 4;
                if field_len == -1 {
                    row.push(None);
                } else {
                    let val = String::from_utf8(payload[pos..pos + field_len as usize].to_vec()).unwrap();
                    row.push(Some(val));
                    pos += field_len as usize;
                }
            }
            parsed_rows.push(row);
        }

        if msg_type == MSG_READY_FOR_QUERY {
            break;
        }
    }

    assert_eq!(parsed_rows.len(), 3, "Expected 3 data rows");
    println!("Rows: {:?}", parsed_rows);

    // Re-encode each row and compare byte-for-byte
    for (i, (parsed, pg_raw)) in parsed_rows.iter().zip(pg_data_rows.iter()).enumerate() {
        let refs: Vec<Option<&str>> = parsed.iter().map(|v| v.as_deref()).collect();
        let our_row = encode_data_row(&refs);
        assert_eq!(our_row, *pg_raw, "DataRow {} mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", i, our_row, pg_raw);
    }
}
