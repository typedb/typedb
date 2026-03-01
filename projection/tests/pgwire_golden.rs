/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Integration tests that compare our wire protocol encoding against golden bytes
//! captured from a real Postgres 16.13 instance.
//!
//! Golden bytes were captured by `capture_pg_bytes.py` connecting to:
//!   podman run -d --name pgwire-test -e POSTGRES_PASSWORD=test -p 5433:5432 postgres:16-alpine
//!
//! To re-capture: wsl -- python3 projection/tests/capture_pg_bytes.py > projection/tests/pg_golden_bytes.json

use projection::pgwire::messages::*;

/// Parse hex string to bytes.
fn hex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

// ══════════════════════════════════════════════════════════════
// Golden bytes from Postgres 16.13 (captured via capture_pg_bytes.py)
// ══════════════════════════════════════════════════════════════

// AuthenticationOk: R [len=8] [auth_type=0]
const PG_AUTH_OK: &str = "520000000800000000";

// ParameterStatus examples from the handshake
const PG_PARAM_TIMEZONE: &str = "530000001154696d655a6f6e650055544300";
const PG_PARAM_CLIENT_ENCODING: &str = "5300000019636c69656e745f656e636f64696e67005554463800";
const PG_PARAM_SERVER_VERSION: &str = "53000000197365727665725f76657273696f6e0031362e313300";
const PG_PARAM_SERVER_ENCODING: &str = "53000000197365727665725f656e636f64696e67005554463800";
const PG_PARAM_DATESTYLE: &str = "5300000017446174655374796c650049534f2c204d445900";
const PG_PARAM_INT_DATETIMES: &str = "5300000019696e74656765725f6461746574696d6573006f6e00";
const PG_PARAM_STD_STRINGS: &str = "53000000237374616e646172645f636f6e666f726d696e675f737472696e6773006f6e00";

// BackendKeyData: K [len=12] [pid=149] [secret=-1947857984]
const PG_BACKEND_KEY_DATA: &str = "4b0000000c000000958be60bc0";

// ReadyForQuery: Z [len=5] [status=I]
const PG_READY_FOR_QUERY_IDLE: &str = "5a0000000549";

// RowDescription for "SELECT 42 AS answer": T [len=31] [1 col: "answer" int4]
const PG_ROW_DESC_SELECT_42: &str = "540000001f0001616e7377657200000000000000000000170004ffffffff0000";

// DataRow for "SELECT 42": D [len=12] [1 field: "42"]
const PG_DATA_ROW_42: &str = "440000000c0001000000023432";

// DataRow for "SELECT NULL::text": D [len=10] [1 field: NULL(-1)]
const PG_DATA_ROW_NULL: &str = "440000000a0001ffffffff";

// CommandComplete for SELECT 1 row: C [len=13] "SELECT 1\0"
const PG_CMD_COMPLETE_SELECT_1: &str = "430000000d53454c454354203100";

// CommandComplete for SELECT 3 rows: C [len=13] "SELECT 3\0"
const PG_CMD_COMPLETE_SELECT_3: &str = "430000000d53454c454354203300";

// Multi-row DataRows: VALUES (1,'a'), (2,'b'), (3,NULL)
const PG_DATA_ROW_MULTI_0: &str = "4400000010000200000001310000000161";
const PG_DATA_ROW_MULTI_1: &str = "4400000010000200000001320000000162";
const PG_DATA_ROW_MULTI_2: &str = "440000000f00020000000133ffffffff";

// ErrorResponse for "SELEKT bad_syntax"
const PG_ERROR_RESPONSE_SYNTAX: &str = "450000005e534552524f5200564552524f52004334323630\
31004d73796e746178206572726f72206174206f72206e656172202253454c454b542200503100467363616e\
2e6c004c3132343400527363616e6e65725f79796572726f720000";

// ── Tests ──────────────────────────────────────────────────────────

#[test]
fn auth_ok_matches_postgres_golden_bytes() {
    let pg_bytes = hex(PG_AUTH_OK);
    let our_bytes = encode_auth_ok();
    assert_eq!(
        our_bytes, pg_bytes,
        "AuthenticationOk mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}

#[test]
fn parameter_status_timezone_matches() {
    let pg_bytes = hex(PG_PARAM_TIMEZONE);
    let our_bytes = encode_parameter_status("TimeZone", "UTC");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus TimeZone=UTC mismatch");
}

#[test]
fn parameter_status_client_encoding_matches() {
    let pg_bytes = hex(PG_PARAM_CLIENT_ENCODING);
    let our_bytes = encode_parameter_status("client_encoding", "UTF8");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus client_encoding=UTF8 mismatch");
}

#[test]
fn parameter_status_server_version_matches() {
    let pg_bytes = hex(PG_PARAM_SERVER_VERSION);
    let our_bytes = encode_parameter_status("server_version", "16.13");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus server_version=16.13 mismatch");
}

#[test]
fn parameter_status_server_encoding_matches() {
    let pg_bytes = hex(PG_PARAM_SERVER_ENCODING);
    let our_bytes = encode_parameter_status("server_encoding", "UTF8");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus server_encoding=UTF8 mismatch");
}

#[test]
fn parameter_status_datestyle_matches() {
    let pg_bytes = hex(PG_PARAM_DATESTYLE);
    let our_bytes = encode_parameter_status("DateStyle", "ISO, MDY");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus DateStyle=ISO,MDY mismatch");
}

#[test]
fn parameter_status_integer_datetimes_matches() {
    let pg_bytes = hex(PG_PARAM_INT_DATETIMES);
    let our_bytes = encode_parameter_status("integer_datetimes", "on");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus integer_datetimes=on mismatch");
}

#[test]
fn parameter_status_standard_conforming_strings_matches() {
    let pg_bytes = hex(PG_PARAM_STD_STRINGS);
    let our_bytes = encode_parameter_status("standard_conforming_strings", "on");
    assert_eq!(our_bytes, pg_bytes, "ParameterStatus standard_conforming_strings=on mismatch");
}

#[test]
fn backend_key_data_matches_postgres_structure() {
    let pg_bytes = hex(PG_BACKEND_KEY_DATA);

    // Extract PID and secret from golden bytes to re-encode
    let pid = i32::from_be_bytes([pg_bytes[5], pg_bytes[6], pg_bytes[7], pg_bytes[8]]);
    let secret = i32::from_be_bytes([pg_bytes[9], pg_bytes[10], pg_bytes[11], pg_bytes[12]]);

    let our_bytes = encode_backend_key_data(pid, secret);
    assert_eq!(our_bytes, pg_bytes, "BackendKeyData mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_bytes, pg_bytes);
}

#[test]
fn ready_for_query_idle_matches_postgres() {
    let pg_bytes = hex(PG_READY_FOR_QUERY_IDLE);
    let our_bytes = encode_ready_for_query(TX_IDLE);
    assert_eq!(
        our_bytes, pg_bytes,
        "ReadyForQuery(Idle) mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}

#[test]
fn row_description_select_42_matches_postgres() {
    // Parse the golden RowDescription to extract column metadata
    let pg_bytes = hex(PG_ROW_DESC_SELECT_42);

    // Parse: type(1) + len(4) + count(2) + column fields
    let payload = &pg_bytes[5..]; // skip type byte + length
    let col_count = i16::from_be_bytes([payload[0], payload[1]]);
    assert_eq!(col_count, 1);

    let mut pos = 2;
    // Column name
    let name_end = payload[pos..].iter().position(|&b| b == 0).unwrap();
    let name = String::from_utf8(payload[pos..pos + name_end].to_vec()).unwrap();
    pos += name_end + 1;

    let table_oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
    pos += 4;
    let col_idx = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
    pos += 2;
    let type_oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
    pos += 4;
    let type_size = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
    pos += 2;
    let type_mod = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
    pos += 4;
    let fmt = i16::from_be_bytes([payload[pos], payload[pos + 1]]);

    let cols = vec![ColumnDescription {
        name,
        table_oid,
        column_index: col_idx,
        type_oid,
        type_size,
        type_modifier: type_mod,
        format_code: fmt,
    }];

    let our_bytes = encode_row_description(&cols);
    assert_eq!(
        our_bytes, pg_bytes,
        "RowDescription(SELECT 42) mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}

#[test]
fn data_row_select_42_matches_postgres() {
    let pg_bytes = hex(PG_DATA_ROW_42);
    let our_bytes = encode_data_row(&[Some("42")]);
    assert_eq!(our_bytes, pg_bytes, "DataRow('42') mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_bytes, pg_bytes);
}

#[test]
fn data_row_null_matches_postgres() {
    let pg_bytes = hex(PG_DATA_ROW_NULL);
    let our_bytes = encode_data_row(&[None]);
    assert_eq!(our_bytes, pg_bytes, "DataRow(NULL) mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}", our_bytes, pg_bytes);
}

#[test]
fn command_complete_select_1_matches_postgres() {
    let pg_bytes = hex(PG_CMD_COMPLETE_SELECT_1);
    let our_bytes = encode_command_complete("SELECT 1");
    assert_eq!(
        our_bytes, pg_bytes,
        "CommandComplete('SELECT 1') mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}

#[test]
fn command_complete_select_3_matches_postgres() {
    let pg_bytes = hex(PG_CMD_COMPLETE_SELECT_3);
    let our_bytes = encode_command_complete("SELECT 3");
    assert_eq!(
        our_bytes, pg_bytes,
        "CommandComplete('SELECT 3') mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}

#[test]
fn multi_row_data_rows_match_postgres() {
    // Row 0: (1, 'a')
    let pg0 = hex(PG_DATA_ROW_MULTI_0);
    let our0 = encode_data_row(&[Some("1"), Some("a")]);
    assert_eq!(our0, pg0, "Multi DataRow[0] mismatch");

    // Row 1: (2, 'b')
    let pg1 = hex(PG_DATA_ROW_MULTI_1);
    let our1 = encode_data_row(&[Some("2"), Some("b")]);
    assert_eq!(our1, pg1, "Multi DataRow[1] mismatch");

    // Row 2: (3, NULL)
    let pg2 = hex(PG_DATA_ROW_MULTI_2);
    let our2 = encode_data_row(&[Some("3"), None]);
    assert_eq!(our2, pg2, "Multi DataRow[2] mismatch");
}

#[test]
fn error_response_contains_correct_fields() {
    let pg_bytes = hex(PG_ERROR_RESPONSE_SYNTAX);

    // Parse Postgres error to extract S, C, M fields
    let payload = &pg_bytes[5..]; // skip type + length
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
            b'S' => {
                if severity.is_none() {
                    severity = Some(value);
                }
            }
            b'C' => code = Some(value),
            b'M' => message = Some(value),
            _ => {}
        }
    }

    let sev = severity.unwrap();
    let cod = code.unwrap();
    let msg = message.unwrap();

    assert_eq!(sev, "ERROR");
    assert_eq!(cod, "42601");
    assert!(msg.contains("SELEKT"), "Error message should reference the bad keyword");

    // Our encode_error_response produces only S, C, M fields (not V, P, F, L, R like Postgres)
    // so we can't compare byte-for-byte. Instead, verify our output is structurally valid:
    let our_error = encode_error_response(&sev, &cod, &msg);

    // Verify structure: type byte
    assert_eq!(our_error[0], b'E');

    // Verify length field
    let len = i32::from_be_bytes([our_error[1], our_error[2], our_error[3], our_error[4]]);
    assert_eq!(len as usize, our_error.len() - 1);

    // Parse our fields back
    let our_payload = &our_error[5..];
    let mut fields = Vec::new();
    let mut p = 0;
    while p < our_payload.len() && our_payload[p] != 0 {
        let ft = our_payload[p] as char;
        p += 1;
        let se = our_payload[p..].iter().position(|&b| b == 0).unwrap();
        let v = String::from_utf8(our_payload[p..p + se].to_vec()).unwrap();
        p += se + 1;
        fields.push((ft, v));
    }

    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0], ('S', sev));
    assert_eq!(fields[1], ('C', cod));
    assert_eq!(fields[2], ('M', msg));

    // Verify terminal null
    assert_eq!(*our_payload.last().unwrap(), 0);
}

#[test]
fn handshake_sequence_starts_with_auth_ends_with_ready() {
    // Golden handshake sequence from Postgres 16.13:
    // R, S, S, S, S, S, S, S, S, S, S, S, S, S, S, K, Z
    let expected_first = 'R'; // Authentication
    let expected_last = 'Z'; // ReadyForQuery

    // This verifies that our message type constants match what Postgres sends
    assert_eq!(MSG_AUTHENTICATION, b'R');
    assert_eq!(MSG_PARAMETER_STATUS, b'S');
    assert_eq!(MSG_BACKEND_KEY_DATA, b'K');
    assert_eq!(MSG_READY_FOR_QUERY, b'Z');

    assert_eq!(expected_first, MSG_AUTHENTICATION as char);
    assert_eq!(expected_last, MSG_READY_FOR_QUERY as char);
}

// ── RowDescription for multi-column query ──────────────────────────

#[test]
fn row_description_multi_column_matches_postgres() {
    // Golden: "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS t(id, name)"
    // RowDescription has 2 columns: id (int4 oid=23) and name (text oid=25)
    let pg_bytes =
        hex("54000000320002696400000000000000000000170004ffffffff00006e616d650000000000000000000019ffffffffffff0000");

    // Parse columns from golden bytes
    let payload = &pg_bytes[5..];
    let col_count = i16::from_be_bytes([payload[0], payload[1]]);
    assert_eq!(col_count, 2);

    let mut cols = Vec::new();
    let mut pos = 2;
    for _ in 0..col_count {
        let name_end = payload[pos..].iter().position(|&b| b == 0).unwrap();
        let name = String::from_utf8(payload[pos..pos + name_end].to_vec()).unwrap();
        pos += name_end + 1;
        let table_oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        let col_idx = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
        pos += 2;
        let type_oid = u32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        let type_size = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
        pos += 2;
        let type_mod = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        let fmt = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
        pos += 2;
        cols.push(ColumnDescription {
            name,
            table_oid,
            column_index: col_idx,
            type_oid,
            type_size,
            type_modifier: type_mod,
            format_code: fmt,
        });
    }

    let our_bytes = encode_row_description(&cols);
    assert_eq!(
        our_bytes, pg_bytes,
        "RowDescription(multi) mismatch!\n  ours: {:02x?}\n  pg:   {:02x?}",
        our_bytes, pg_bytes
    );
}
