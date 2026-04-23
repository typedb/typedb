/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Postgres v3 wire protocol message encoding and decoding.
//!
//! Reference: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
//!
//! All messages (except StartupMessage and SSLRequest) have the format:
//!   [1 byte: message type] [4 bytes: length including self] [payload...]
//!
//! StartupMessage has:
//!   [4 bytes: length including self] [4 bytes: protocol version 196608] [key\0value\0...] [\0]

use crate::type_mapping::PgOid;

// ── Message type bytes (frontend → backend) ────────────────────────

/// Simple Query message from client.
pub const MSG_QUERY: u8 = b'Q';
/// Password message from client.
pub const MSG_PASSWORD: u8 = b'p';
/// Terminate connection from client.
pub const MSG_TERMINATE: u8 = b'X';

// ── Extended Query sub-protocol (frontend → backend) ───────────────

/// Parse message (prepare a named or unnamed statement).
pub const MSG_PARSE: u8 = b'P';
/// Bind message (bind parameters to a prepared statement).
pub const MSG_BIND: u8 = b'B';
/// Describe message (request description of a statement or portal).
pub const MSG_DESCRIBE: u8 = b'D';
/// Execute message (execute a portal).
pub const MSG_EXECUTE: u8 = b'E';
/// Sync message (marks the end of an extended-query cycle).
pub const MSG_SYNC: u8 = b'S';
/// Close message (close a statement or portal).
pub const MSG_CLOSE: u8 = b'C';

// ── Message type bytes (backend → frontend) ────────────────────────

/// Authentication request/response.
pub const MSG_AUTHENTICATION: u8 = b'R';
/// Parameter status (e.g. server_version).
pub const MSG_PARAMETER_STATUS: u8 = b'S';
/// Backend key data (process ID + secret key).
pub const MSG_BACKEND_KEY_DATA: u8 = b'K';
/// Ready for query.
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
/// Row description (column metadata).
pub const MSG_ROW_DESCRIPTION: u8 = b'T';
/// CopyOutResponse.
pub const MSG_COPY_OUT_RESPONSE: u8 = b'H';
/// CopyData.
pub const MSG_COPY_DATA: u8 = b'd';
/// CopyDone.
pub const MSG_COPY_DONE: u8 = b'c';
/// Data row.
pub const MSG_DATA_ROW: u8 = b'D';
/// Command complete.
pub const MSG_COMMAND_COMPLETE: u8 = b'C';
/// Error response.
pub const MSG_ERROR_RESPONSE: u8 = b'E';

// ── Extended Query sub-protocol (backend → frontend) ───────────────

/// ParseComplete — response to Parse.
pub const MSG_PARSE_COMPLETE: u8 = b'1';
/// BindComplete — response to Bind.
pub const MSG_BIND_COMPLETE: u8 = b'2';
/// CloseComplete — response to Close.
pub const MSG_CLOSE_COMPLETE: u8 = b'3';
/// NoData — response to Describe when the statement produces no rows.
pub const MSG_NO_DATA: u8 = b'n';
/// ParameterDescription — describes parameters of a prepared statement.
pub const MSG_PARAMETER_DESCRIPTION: u8 = b't';

// ── Protocol constants ─────────────────────────────────────────────

/// Postgres protocol version 3.0 = 196608 (0x00030000).
pub const PROTOCOL_VERSION_30: i32 = 196608;
/// Frontend SSL negotiation request code.
pub const SSL_REQUEST_CODE: i32 = 80877103;
/// Frontend GSS encryption negotiation request code.
pub const GSSENC_REQUEST_CODE: i32 = 80877104;
/// Single-byte response indicating encryption is not supported.
pub const ENCRYPTION_NOT_SUPPORTED: u8 = b'N';

/// Transaction status indicators for ReadyForQuery.
pub const TX_IDLE: u8 = b'I';
pub const TX_IN_TRANSACTION: u8 = b'T';
pub const TX_FAILED: u8 = b'E';

// ── Authentication sub-types ───────────────────────────────────────

pub const AUTH_OK: i32 = 0;
pub const AUTH_CLEARTEXT_PASSWORD: i32 = 3;
pub const AUTH_MD5_PASSWORD: i32 = 5;

// ── Column description for RowDescription ──────────────────────────

/// Metadata for a single column in a RowDescription message.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescription {
    /// Column name.
    pub name: String,
    /// OID of the table (0 if not from a table).
    pub table_oid: u32,
    /// Column index in the table (0 if not from a table).
    pub column_index: i16,
    /// OID of the column's data type.
    pub type_oid: PgOid,
    /// Type size (pg_type.typlen). Negative for variable-length.
    pub type_size: i16,
    /// Type modifier (e.g. precision for numeric). -1 if not applicable.
    pub type_modifier: i32,
    /// Format code: 0 = text, 1 = binary.
    pub format_code: i16,
}

// ── Parsed frontend (client) messages ──────────────────────────────

/// A startup message from the client.
#[derive(Debug, Clone, PartialEq)]
pub struct StartupMessage {
    pub protocol_version: i32,
    pub parameters: Vec<(String, String)>,
}

/// A simple query message from the client.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryMessage {
    pub query: String,
}

// ── Internal helpers ────────────────────────────────────────────────

/// Start building a message with the given type byte.
/// Returns a buffer with type byte + 4 placeholder bytes for the length field.
fn begin_message(msg_type: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.push(msg_type);
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    buf
}

/// Backpatch the length field at bytes [1..5].
/// Length = total message size minus the 1-byte type tag.
fn finish_message(buf: &mut [u8]) {
    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());
}

/// Encode a simple authentication message with only a 4-byte auth sub-type.
fn encode_auth(auth_type: i32) -> Vec<u8> {
    let mut buf = begin_message(MSG_AUTHENTICATION);
    buf.extend_from_slice(&auth_type.to_be_bytes());
    finish_message(&mut buf);
    buf
}

// ── Encoding functions (backend → frontend) ────────────────────────

/// Encode an AuthenticationOk message.
pub fn encode_auth_ok() -> Vec<u8> {
    encode_auth(AUTH_OK)
}

/// Encode an AuthenticationCleartextPassword message.
pub fn encode_auth_cleartext_password() -> Vec<u8> {
    encode_auth(AUTH_CLEARTEXT_PASSWORD)
}

/// Encode an AuthenticationMD5Password message with a 4-byte salt.
pub fn encode_auth_md5_password(salt: [u8; 4]) -> Vec<u8> {
    let mut buf = begin_message(MSG_AUTHENTICATION);
    buf.extend_from_slice(&AUTH_MD5_PASSWORD.to_be_bytes());
    buf.extend_from_slice(&salt);
    finish_message(&mut buf);
    buf
}

/// Encode a ParameterStatus message (key=value pair).
pub fn encode_parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut buf = begin_message(MSG_PARAMETER_STATUS);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0);
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
    finish_message(&mut buf);
    buf
}

/// Encode a BackendKeyData message.
pub fn encode_backend_key_data(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut buf = begin_message(MSG_BACKEND_KEY_DATA);
    buf.extend_from_slice(&process_id.to_be_bytes());
    buf.extend_from_slice(&secret_key.to_be_bytes());
    finish_message(&mut buf);
    buf
}

/// Encode a ReadyForQuery message.
pub fn encode_ready_for_query(tx_status: u8) -> Vec<u8> {
    let mut buf = begin_message(MSG_READY_FOR_QUERY);
    buf.push(tx_status);
    finish_message(&mut buf);
    buf
}

/// Encode a RowDescription message from column descriptors.
pub fn encode_row_description(columns: &[ColumnDescription]) -> Vec<u8> {
    let mut buf = begin_message(MSG_ROW_DESCRIPTION);
    buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        buf.extend_from_slice(col.name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&col.table_oid.to_be_bytes());
        buf.extend_from_slice(&col.column_index.to_be_bytes());
        buf.extend_from_slice(&col.type_oid.to_be_bytes());
        buf.extend_from_slice(&col.type_size.to_be_bytes());
        buf.extend_from_slice(&col.type_modifier.to_be_bytes());
        buf.extend_from_slice(&col.format_code.to_be_bytes());
    }
    finish_message(&mut buf);
    buf
}

/// Encode a CopyOutResponse message.
pub fn encode_copy_out_response(column_formats: &[i16]) -> Vec<u8> {
    let mut buf = begin_message(MSG_COPY_OUT_RESPONSE);
    buf.push(1); // binary overall format
    buf.extend_from_slice(&(column_formats.len() as i16).to_be_bytes());
    for format in column_formats {
        buf.extend_from_slice(&format.to_be_bytes());
    }
    finish_message(&mut buf);
    buf
}

/// Encode a CopyData message.
pub fn encode_copy_data(payload: &[u8]) -> Vec<u8> {
    let mut buf = begin_message(MSG_COPY_DATA);
    buf.extend_from_slice(payload);
    finish_message(&mut buf);
    buf
}

/// Encode a CopyDone message.
pub fn encode_copy_done() -> Vec<u8> {
    let mut buf = begin_message(MSG_COPY_DONE);
    finish_message(&mut buf);
    buf
}

/// Encode a DataRow message from text-format field values.
/// `None` values represent SQL NULL.
pub fn encode_data_row(values: &[Option<&str>]) -> Vec<u8> {
    let mut buf = begin_message(MSG_DATA_ROW);
    buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for value in values {
        match value {
            Some(s) => {
                buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            None => {
                buf.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
    }
    finish_message(&mut buf);
    buf
}

/// Encode a CommandComplete message (e.g. "SELECT 42").
pub fn encode_command_complete(tag: &str) -> Vec<u8> {
    let mut buf = begin_message(MSG_COMMAND_COMPLETE);
    buf.extend_from_slice(tag.as_bytes());
    buf.push(0);
    finish_message(&mut buf);
    buf
}

/// Encode an ErrorResponse message.
pub fn encode_error_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
    let mut buf = begin_message(MSG_ERROR_RESPONSE);
    // Severity field
    buf.push(b'S');
    buf.extend_from_slice(severity.as_bytes());
    buf.push(0);
    // SQLSTATE code field
    buf.push(b'C');
    buf.extend_from_slice(code.as_bytes());
    buf.push(0);
    // Message field
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    // Terminator
    buf.push(0);
    finish_message(&mut buf);
    buf
}

// ── Extended Query protocol encoders ───────────────────────────────

/// Encode a ParseComplete ('1') message — empty body.
pub fn encode_parse_complete() -> Vec<u8> {
    let mut buf = begin_message(MSG_PARSE_COMPLETE);
    finish_message(&mut buf);
    buf
}

/// Encode a BindComplete ('2') message — empty body.
pub fn encode_bind_complete() -> Vec<u8> {
    let mut buf = begin_message(MSG_BIND_COMPLETE);
    finish_message(&mut buf);
    buf
}

/// Encode a CloseComplete ('3') message — empty body.
pub fn encode_close_complete() -> Vec<u8> {
    let mut buf = begin_message(MSG_CLOSE_COMPLETE);
    finish_message(&mut buf);
    buf
}

/// Encode a NoData ('n') message — empty body.
pub fn encode_no_data() -> Vec<u8> {
    let mut buf = begin_message(MSG_NO_DATA);
    finish_message(&mut buf);
    buf
}

/// Encode a ParameterDescription ('t') message with zero parameters.
pub fn encode_parameter_description_empty() -> Vec<u8> {
    let mut buf = begin_message(MSG_PARAMETER_DESCRIPTION);
    buf.extend_from_slice(&0i16.to_be_bytes()); // zero parameters
    finish_message(&mut buf);
    buf
}

// ── Decoding functions (frontend → backend) ────────────────────────

/// Read a null-terminated string from `data` starting at `pos`.
/// Returns the string and the position after the terminating null byte.
fn read_cstring(data: &[u8], pos: usize) -> Result<(String, usize), String> {
    let end = data[pos..].iter().position(|&b| b == 0).ok_or_else(|| "missing null terminator".to_owned())?;
    let s = String::from_utf8(data[pos..pos + end].to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))?;
    Ok((s, pos + end + 1))
}

/// Decode a StartupMessage from raw bytes (after the length prefix has been read).
/// `data` is the full message content including the 4-byte protocol version.
pub fn decode_startup_message(data: &[u8]) -> Result<StartupMessage, String> {
    if data.len() < 4 {
        return Err("startup message too short".into());
    }
    let protocol_version = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let mut parameters = Vec::new();
    let mut pos = 4;
    // Parse key-value pairs terminated by a lone \0
    while pos < data.len() && data[pos] != 0 {
        let (key, next) = read_cstring(data, pos)?;
        let (value, next) = read_cstring(data, next)?;
        parameters.push((key, value));
        pos = next;
    }
    Ok(StartupMessage { protocol_version, parameters })
}

/// Decode a simple Query message payload (the SQL string, after type byte + length).
pub fn decode_query_message(data: &[u8]) -> Result<QueryMessage, String> {
    let (query, _) = read_cstring(data, 0)?;
    Ok(QueryMessage { query })
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── AuthenticationOk ───────────────────────────────────────

    #[test]
    fn auth_ok_starts_with_type_byte_r() {
        let msg = encode_auth_ok();
        assert_eq!(msg[0], MSG_AUTHENTICATION);
    }

    #[test]
    fn auth_ok_has_correct_length() {
        // 'R' + 4 bytes length (8) + 4 bytes auth type (0) = 9 bytes total
        let msg = encode_auth_ok();
        assert_eq!(msg.len(), 9);
        // length field = 8 (includes self but not the type byte)
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(len, 8);
    }

    #[test]
    fn auth_ok_has_auth_type_zero() {
        let msg = encode_auth_ok();
        let auth_type = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(auth_type, AUTH_OK);
    }

    // ── AuthenticationCleartextPassword ─────────────────────────

    #[test]
    fn auth_cleartext_has_correct_structure() {
        let msg = encode_auth_cleartext_password();
        assert_eq!(msg[0], MSG_AUTHENTICATION);
        assert_eq!(msg.len(), 9);
        let auth_type = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(auth_type, AUTH_CLEARTEXT_PASSWORD);
    }

    // ── AuthenticationMD5Password ──────────────────────────────

    #[test]
    fn auth_md5_has_correct_structure() {
        let salt = [0xDE, 0xAD, 0xBE, 0xEF];
        let msg = encode_auth_md5_password(salt);
        assert_eq!(msg[0], MSG_AUTHENTICATION);
        // 'R' + 4 (len) + 4 (auth type) + 4 (salt) = 13
        assert_eq!(msg.len(), 13);
        let auth_type = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(auth_type, AUTH_MD5_PASSWORD);
        assert_eq!(&msg[9..13], &salt);
    }

    // ── ParameterStatus ────────────────────────────────────────

    #[test]
    fn parameter_status_type_byte() {
        let msg = encode_parameter_status("server_version", "15.0");
        assert_eq!(msg[0], MSG_PARAMETER_STATUS);
    }

    #[test]
    fn parameter_status_contains_key_and_value() {
        let msg = encode_parameter_status("server_version", "15.0");
        // Payload should contain "server_version\0" + "15.0\0"
        let payload = &msg[5..]; // skip type byte + 4 length bytes
        let key_end = payload.iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[..key_end], b"server_version");
        let value_start = key_end + 1;
        let value_end = value_start + payload[value_start..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[value_start..value_end], b"15.0");
    }

    #[test]
    fn parameter_status_length_field_is_correct() {
        let msg = encode_parameter_status("client_encoding", "UTF8");
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        // length = 4 (self) + "client_encoding\0".len() + "UTF8\0".len()
        let expected = 4 + 16 + 5; // "client_encoding" = 15 + \0, "UTF8" = 4 + \0
        assert_eq!(len, expected);
    }

    // ── BackendKeyData ─────────────────────────────────────────

    #[test]
    fn backend_key_data_structure() {
        let msg = encode_backend_key_data(12345, 67890);
        assert_eq!(msg[0], MSG_BACKEND_KEY_DATA);
        // 'K' + 4 (len=12) + 4 (pid) + 4 (secret) = 13
        assert_eq!(msg.len(), 13);
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(len, 12);
        let pid = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(pid, 12345);
        let secret = i32::from_be_bytes([msg[9], msg[10], msg[11], msg[12]]);
        assert_eq!(secret, 67890);
    }

    // ── ReadyForQuery ──────────────────────────────────────────

    #[test]
    fn ready_for_query_idle() {
        let msg = encode_ready_for_query(TX_IDLE);
        assert_eq!(msg[0], MSG_READY_FOR_QUERY);
        // 'Z' + 4 (len=5) + 1 (status) = 6
        assert_eq!(msg.len(), 6);
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(len, 5);
        assert_eq!(msg[5], TX_IDLE);
    }

    #[test]
    fn ready_for_query_in_transaction() {
        let msg = encode_ready_for_query(TX_IN_TRANSACTION);
        assert_eq!(msg[5], TX_IN_TRANSACTION);
    }

    // ── RowDescription ─────────────────────────────────────────

    #[test]
    fn row_description_type_byte() {
        let cols = vec![ColumnDescription {
            name: "id".into(),
            table_oid: 0,
            column_index: 0,
            type_oid: 20,
            type_size: 8,
            type_modifier: -1,
            format_code: 0,
        }];
        let msg = encode_row_description(&cols);
        assert_eq!(msg[0], MSG_ROW_DESCRIPTION);
    }

    #[test]
    fn row_description_column_count() {
        let cols = vec![
            ColumnDescription {
                name: "id".into(),
                table_oid: 0,
                column_index: 0,
                type_oid: 20,
                type_size: 8,
                type_modifier: -1,
                format_code: 0,
            },
            ColumnDescription {
                name: "name".into(),
                table_oid: 0,
                column_index: 0,
                type_oid: 25,
                type_size: -1,
                type_modifier: -1,
                format_code: 0,
            },
        ];
        let msg = encode_row_description(&cols);
        // After type byte + 4 length bytes, next 2 bytes = column count
        let count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(count, 2);
    }

    #[test]
    fn row_description_single_column_field_layout() {
        let cols = vec![ColumnDescription {
            name: "age".into(),
            table_oid: 0,
            column_index: 0,
            type_oid: 20, // int8
            type_size: 8,
            type_modifier: -1,
            format_code: 0,
        }];
        let msg = encode_row_description(&cols);

        // After type(1) + len(4) + count(2) = offset 7, column fields start
        let mut pos = 7;

        // name: "age\0"
        assert_eq!(&msg[pos..pos + 3], b"age");
        assert_eq!(msg[pos + 3], 0);
        pos += 4;

        // table OID (4 bytes)
        let table_oid = u32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(table_oid, 0);
        pos += 4;

        // column index (2 bytes)
        let col_idx = i16::from_be_bytes([msg[pos], msg[pos + 1]]);
        assert_eq!(col_idx, 0);
        pos += 2;

        // type OID (4 bytes)
        let type_oid = u32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(type_oid, 20);
        pos += 4;

        // type size (2 bytes)
        let type_size = i16::from_be_bytes([msg[pos], msg[pos + 1]]);
        assert_eq!(type_size, 8);
        pos += 2;

        // type modifier (4 bytes)
        let type_mod = i32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(type_mod, -1);
        pos += 4;

        // format code (2 bytes)
        let fmt = i16::from_be_bytes([msg[pos], msg[pos + 1]]);
        assert_eq!(fmt, 0);
    }

    // ── DataRow ────────────────────────────────────────────────

    #[test]
    fn data_row_type_byte() {
        let msg = encode_data_row(&[Some("hello")]);
        assert_eq!(msg[0], MSG_DATA_ROW);
    }

    #[test]
    fn data_row_field_count() {
        let msg = encode_data_row(&[Some("a"), Some("b"), None]);
        let count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(count, 3);
    }

    #[test]
    fn data_row_text_value() {
        let msg = encode_data_row(&[Some("hello")]);
        // After type(1) + len(4) + count(2) = offset 7
        // field length (4 bytes)
        let field_len = i32::from_be_bytes([msg[7], msg[8], msg[9], msg[10]]);
        assert_eq!(field_len, 5); // "hello".len()
        assert_eq!(&msg[11..16], b"hello");
    }

    #[test]
    fn data_row_null_value() {
        let msg = encode_data_row(&[None]);
        // After type(1) + len(4) + count(2) = offset 7
        // NULL is encoded as -1 in the 4-byte length field
        let field_len = i32::from_be_bytes([msg[7], msg[8], msg[9], msg[10]]);
        assert_eq!(field_len, -1);
    }

    #[test]
    fn data_row_mixed_values() {
        let msg = encode_data_row(&[Some("42"), None, Some("hi")]);
        let mut pos = 7; // past type + len + count

        // First field: "42"
        let len1 = i32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(len1, 2);
        pos += 4;
        assert_eq!(&msg[pos..pos + 2], b"42");
        pos += 2;

        // Second field: NULL
        let len2 = i32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(len2, -1);
        pos += 4;

        // Third field: "hi"
        let len3 = i32::from_be_bytes([msg[pos], msg[pos + 1], msg[pos + 2], msg[pos + 3]]);
        assert_eq!(len3, 2);
        pos += 4;
        assert_eq!(&msg[pos..pos + 2], b"hi");
    }

    // ── CommandComplete ────────────────────────────────────────

    #[test]
    fn command_complete_structure() {
        let msg = encode_command_complete("SELECT 5");
        assert_eq!(msg[0], MSG_COMMAND_COMPLETE);
        let payload = &msg[5..];
        // "SELECT 5\0"
        let tag_end = payload.iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[..tag_end], b"SELECT 5");
    }

    #[test]
    fn command_complete_length() {
        let msg = encode_command_complete("SELECT 5");
        let len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        // 4 (self) + "SELECT 5\0".len() = 4 + 9 = 13
        assert_eq!(len, 13);
    }

    // ── ErrorResponse ──────────────────────────────────────────

    #[test]
    fn error_response_type_byte() {
        let msg = encode_error_response("ERROR", "42601", "syntax error");
        assert_eq!(msg[0], MSG_ERROR_RESPONSE);
    }

    #[test]
    fn error_response_contains_fields() {
        let msg = encode_error_response("ERROR", "42601", "syntax error");
        let payload = &msg[5..]; // skip type + length

        // Should contain 'S' field (severity), 'C' field (code), 'M' field (message)
        // Each field: [1 byte field type] [string\0]
        // Terminated by a final \0

        // Find severity field
        let s_pos = payload.iter().position(|&b| b == b'S').unwrap();
        let s_end = s_pos + 1 + payload[s_pos + 1..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[s_pos + 1..s_end], b"ERROR");

        // Find code field
        let c_pos = payload.iter().position(|&b| b == b'C').unwrap();
        let c_end = c_pos + 1 + payload[c_pos + 1..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[c_pos + 1..c_end], b"42601");

        // Find message field
        let m_pos = payload.iter().position(|&b| b == b'M').unwrap();
        let m_end = m_pos + 1 + payload[m_pos + 1..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&payload[m_pos + 1..m_end], b"syntax error");

        // Terminal \0
        assert_eq!(*payload.last().unwrap(), 0u8);
    }

    // ── StartupMessage decoding ────────────────────────────────

    #[test]
    fn decode_startup_v3() {
        // Build a raw startup payload: protocol version + "user\0postgres\0database\0mydb\0\0"
        let mut data = Vec::new();
        data.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
        data.extend_from_slice(b"user\0postgres\0");
        data.extend_from_slice(b"database\0mydb\0");
        data.push(0); // terminator

        let msg = decode_startup_message(&data).unwrap();
        assert_eq!(msg.protocol_version, PROTOCOL_VERSION_30);
        assert_eq!(msg.parameters.len(), 2);
        assert_eq!(msg.parameters[0], ("user".into(), "postgres".into()));
        assert_eq!(msg.parameters[1], ("database".into(), "mydb".into()));
    }

    #[test]
    fn decode_startup_single_param() {
        let mut data = Vec::new();
        data.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
        data.extend_from_slice(b"user\0admin\0");
        data.push(0);

        let msg = decode_startup_message(&data).unwrap();
        assert_eq!(msg.parameters.len(), 1);
        assert_eq!(msg.parameters[0], ("user".into(), "admin".into()));
    }

    #[test]
    fn decode_startup_no_params() {
        let mut data = Vec::new();
        data.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
        data.push(0);

        let msg = decode_startup_message(&data).unwrap();
        assert_eq!(msg.protocol_version, PROTOCOL_VERSION_30);
        assert!(msg.parameters.is_empty());
    }

    // ── QueryMessage decoding ──────────────────────────────────

    #[test]
    fn decode_query_simple() {
        // Query payload is just the SQL string followed by \0
        let data = b"SELECT * FROM users\0";
        let msg = decode_query_message(data).unwrap();
        assert_eq!(msg.query, "SELECT * FROM users");
    }

    #[test]
    fn decode_query_empty_string() {
        let data = b"\0";
        let msg = decode_query_message(data).unwrap();
        assert_eq!(msg.query, "");
    }

    // ── Empty row description ──────────────────────────────────

    #[test]
    fn row_description_zero_columns() {
        let msg = encode_row_description(&[]);
        assert_eq!(msg[0], MSG_ROW_DESCRIPTION);
        let count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(count, 0);
        // type(1) + len(4) + count(2) = 7 bytes total
        assert_eq!(msg.len(), 7);
    }

    // ── Empty data row ─────────────────────────────────────────

    #[test]
    fn data_row_zero_fields() {
        let msg = encode_data_row(&[]);
        assert_eq!(msg[0], MSG_DATA_ROW);
        let count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(count, 0);
        assert_eq!(msg.len(), 7);
    }

    // ── Length field consistency ────────────────────────────────

    #[test]
    fn all_encoded_messages_have_consistent_length_field() {
        let messages: Vec<Vec<u8>> = vec![
            encode_auth_ok(),
            encode_auth_cleartext_password(),
            encode_auth_md5_password([1, 2, 3, 4]),
            encode_parameter_status("k", "v"),
            encode_backend_key_data(1, 2),
            encode_ready_for_query(TX_IDLE),
            encode_row_description(&[]),
            encode_data_row(&[]),
            encode_command_complete("SELECT 0"),
            encode_error_response("ERROR", "00000", "test"),
            encode_parse_complete(),
            encode_bind_complete(),
            encode_close_complete(),
            encode_no_data(),
            encode_parameter_description_empty(),
        ];

        for msg in &messages {
            assert!(msg.len() >= 5, "Message too short: {:?}", msg);
            let declared_len = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
            // declared length includes 4 bytes for itself, plus payload, but NOT the type byte
            assert_eq!(
                declared_len,
                msg.len() - 1,
                "Length field mismatch for message type '{}': declared={}, actual={}",
                msg[0] as char,
                declared_len,
                msg.len() - 1
            );
        }
    }

    // ── Extended Query protocol encoders ───────────────────────

    #[test]
    fn parse_complete_type_byte() {
        let msg = encode_parse_complete();
        assert_eq!(msg[0], MSG_PARSE_COMPLETE);
        assert_eq!(msg.len(), 5); // type + 4-byte length (just self)
    }

    #[test]
    fn bind_complete_type_byte() {
        let msg = encode_bind_complete();
        assert_eq!(msg[0], MSG_BIND_COMPLETE);
        assert_eq!(msg.len(), 5);
    }

    #[test]
    fn close_complete_type_byte() {
        let msg = encode_close_complete();
        assert_eq!(msg[0], MSG_CLOSE_COMPLETE);
        assert_eq!(msg.len(), 5);
    }

    #[test]
    fn no_data_type_byte() {
        let msg = encode_no_data();
        assert_eq!(msg[0], MSG_NO_DATA);
        assert_eq!(msg.len(), 5);
    }

    #[test]
    fn parameter_description_empty_type_byte() {
        let msg = encode_parameter_description_empty();
        assert_eq!(msg[0], MSG_PARAMETER_DESCRIPTION);
        // type(1) + length(4) + int16(2) = 7 bytes
        assert_eq!(msg.len(), 7);
        // The int16 should be 0 (zero parameters)
        let num_params = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(num_params, 0);
    }
}
