/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Per-connection state machine for the Postgres wire protocol.
//!
//! `PgConnection` is generic over `AsyncRead + AsyncWrite` so it can be
//! tested with in-memory streams (e.g. `tokio::io::duplex()`).

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::type_mapping::{
    PG_OID_BOOL, PG_OID_FLOAT8, PG_OID_INT4, PG_OID_INT8, PG_OID_OID, PG_OID_TEXT, PG_OID_VARCHAR,
};

use crate::pgwire::{
    authenticator::{decode_password_message, extract_database, extract_username, AuthMode},
    messages::{
        encode_auth_cleartext_password, encode_auth_ok, encode_backend_key_data, encode_bind_complete,
        encode_close_complete, encode_command_complete, encode_copy_data, encode_copy_done, encode_copy_out_response,
        encode_data_row, encode_error_response, encode_no_data, encode_parameter_description_empty,
        encode_parameter_status, encode_parse_complete, encode_ready_for_query, encode_row_description,
        ColumnDescription, ENCRYPTION_NOT_SUPPORTED, GSSENC_REQUEST_CODE, MSG_BIND, MSG_CLOSE, MSG_DESCRIBE,
        MSG_EXECUTE, MSG_PARSE, MSG_PASSWORD, MSG_QUERY, MSG_SYNC, MSG_TERMINATE, PROTOCOL_VERSION_30,
        SSL_REQUEST_CODE, TX_IDLE,
    },
};

// ── Query handler trait ────────────────────────────────────────────

/// Result of executing a SQL query against a projection catalog.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnDescription>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Custom command tag (e.g. "SET", "BEGIN"). If `None`, defaults to "SELECT {row_count}".
    pub command_tag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BinaryCopyOutResult {
    pub columns: Vec<ColumnDescription>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: Option<String>,
}

/// Outcome of a query — either a result set or an error.
#[derive(Debug, Clone)]
pub enum QueryOutcome {
    /// Successful result with columns and rows.
    Result(QueryResult),
    /// Successful COPY TO STDOUT result using PostgreSQL binary format.
    CopyOut(BinaryCopyOutResult),
    /// Error with severity, SQLSTATE code, and message.
    Error { severity: String, code: String, message: String },
}

/// Session state captured during startup and reused for session-scoped queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionContext {
    pub current_user: String,
    pub current_database: String,
    pub current_schema: String,
    pub server_port: u16,
    pub backend_pid: i32,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            current_user: "typedb".into(),
            current_database: "typedb".into(),
            current_schema: "public".into(),
            server_port: 5432,
            backend_pid: 1,
        }
    }
}

/// Trait for dispatching SQL queries. Implementations can look up projection
/// catalogs, parse SQL, filter/sort materialized rows, etc.
pub trait QueryHandler: Send + Sync {
    fn handle_query(&self, sql: &str) -> QueryOutcome;

    fn handle_query_with_session(&self, sql: &str, session: &SessionContext) -> QueryOutcome {
        let _ = session;
        self.handle_query(sql)
    }

    fn handle_query_batch_with_session(&self, sql: &str, session: &SessionContext) -> Vec<QueryOutcome> {
        vec![self.handle_query_with_session(sql, session)]
    }
}

// ── Connection parameters sent during handshake ────────────────────

/// Server parameters sent to the client during the startup handshake.
#[derive(Debug, Clone)]
pub struct ServerParams {
    pub server_version: String,
    pub server_encoding: String,
    pub client_encoding: String,
    pub date_style: String,
    pub timezone: String,
}

impl Default for ServerParams {
    fn default() -> Self {
        Self {
            server_version: "16.0 (TypeDB)".into(),
            server_encoding: "UTF8".into(),
            client_encoding: "UTF8".into(),
            date_style: "ISO, MDY".into(),
            timezone: "UTC".into(),
        }
    }
}

// ── PgConnection ───────────────────────────────────────────────────

/// A single Postgres wire protocol connection.
///
/// Generic over `S` so it works with real `TcpStream` or test `DuplexStream`.
pub struct PgConnection<S, H>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: QueryHandler,
{
    stream: S,
    handler: Arc<H>,
    params: ServerParams,
    auth_mode: AuthMode,
    process_id: i32,
    secret_key: i32,
    /// SQL stored from a Parse message, consumed by Execute.
    pending_sql: Option<String>,
    session: SessionContext,
}

impl<S, H> PgConnection<S, H>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: QueryHandler,
{
    /// Create a new connection with the given stream and query handler.
    ///
    /// Defaults to `AuthMode::Trust` (no authentication).
    pub fn new(stream: S, handler: Arc<H>, params: ServerParams) -> Self {
        Self {
            stream,
            handler,
            params,
            auth_mode: AuthMode::Trust,
            process_id: 1,
            secret_key: 0,
            pending_sql: None,
            session: SessionContext::default(),
        }
    }

    /// Set the authentication mode for this connection.
    pub fn with_auth_mode(mut self, auth_mode: AuthMode) -> Self {
        self.auth_mode = auth_mode;
        self
    }

    /// Set the process ID and secret key for BackendKeyData.
    pub fn with_backend_key(mut self, process_id: i32, secret_key: i32) -> Self {
        self.process_id = process_id;
        self.secret_key = secret_key;
        self.session.backend_pid = process_id;
        self
    }

    /// Set the server port exposed to session functions.
    pub fn with_server_port(mut self, server_port: u16) -> Self {
        self.session.server_port = server_port;
        self
    }

    /// Run the connection to completion: startup handshake → query loop → termination.
    pub async fn run(&mut self) -> Result<(), ConnectionError> {
        self.handle_startup().await?;
        self.query_loop().await
    }

    /// Perform the startup handshake: read StartupMessage, send auth + params + ReadyForQuery.
    async fn handle_startup(&mut self) -> Result<(), ConnectionError> {
        let startup_params = loop {
            // Read the startup message (no type byte — just length + payload).
            let payload = self.read_startup_message().await?;

            // Validate protocol version (first 4 bytes of payload).
            if payload.len() < 4 {
                return Err(ConnectionError::Protocol("startup message too short".into()));
            }
            let version = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            match version {
                PROTOCOL_VERSION_30 => {
                    break crate::pgwire::messages::decode_startup_message(&payload)
                        .map_err(|e| ConnectionError::Protocol(format!("bad startup message: {e}")))?;
                }
                SSL_REQUEST_CODE | GSSENC_REQUEST_CODE => {
                    self.write_all(&[ENCRYPTION_NOT_SUPPORTED]).await?;
                    self.stream.flush().await?;
                }
                _ => {
                    return Err(ConnectionError::Protocol(format!("unsupported protocol version: {version}")));
                }
            }
        };
        if let Some(username) = extract_username(&startup_params.parameters) {
            self.session.current_user = username.to_string();
        }
        if let Some(database) = extract_database(&startup_params.parameters) {
            self.session.current_database = database.to_string();
        }

        // Authenticate based on the configured auth mode.
        // Clone auth_mode to release the immutable borrow on self, allowing
        // mutable stream access for write_all / read_message.
        let auth_mode = self.auth_mode.clone();
        match auth_mode {
            AuthMode::Trust => {
                // No authentication required.
                self.write_all(&encode_auth_ok()).await?;
            }
            AuthMode::CleartextPassword(authenticator) => {
                let username = extract_username(&startup_params.parameters)
                    .ok_or_else(|| ConnectionError::Protocol("missing 'user' in startup parameters".into()))?
                    .to_string();

                // Request cleartext password from client.
                self.write_all(&encode_auth_cleartext_password()).await?;
                self.stream.flush().await?;

                // Read the PasswordMessage response.
                let (msg_type, pw_payload) = self.read_message().await?;
                if msg_type != MSG_PASSWORD {
                    return Err(ConnectionError::Protocol(format!(
                        "expected PasswordMessage ('p'), got '{}'",
                        msg_type as char
                    )));
                }
                let password = decode_password_message(&pw_payload)
                    .map_err(|e| ConnectionError::Protocol(format!("bad password message: {e}")))?;

                // Verify credentials.
                if let Err(err_msg) = authenticator.verify_password(&username, &password) {
                    self.write_all(&encode_error_response("FATAL", "28P01", &err_msg)).await?;
                    self.stream.flush().await?;
                    return Err(ConnectionError::Protocol(format!("authentication failed: {err_msg}")));
                }

                self.write_all(&encode_auth_ok()).await?;
            }
        }

        // Encode ParameterStatus messages upfront to avoid borrow conflict.
        let param_messages = vec![
            encode_parameter_status("server_version", &self.params.server_version),
            encode_parameter_status("server_encoding", &self.params.server_encoding),
            encode_parameter_status("client_encoding", &self.params.client_encoding),
            encode_parameter_status("DateStyle", &self.params.date_style),
            encode_parameter_status("TimeZone", &self.params.timezone),
        ];

        for msg in &param_messages {
            self.write_all(msg).await?;
        }

        // Send BackendKeyData
        self.write_all(&encode_backend_key_data(self.process_id, self.secret_key)).await?;

        // Send ReadyForQuery (idle)
        self.write_all(&encode_ready_for_query(TX_IDLE)).await?;

        self.stream.flush().await?;
        Ok(())
    }

    /// Run the query loop: read messages, dispatch queries, handle terminate.
    async fn query_loop(&mut self) -> Result<(), ConnectionError> {
        loop {
            let (msg_type, payload) = self.read_message().await?;
            match msg_type {
                MSG_QUERY => {
                    // Extract the SQL string (null-terminated C string in payload).
                    let sql = read_cstring_from_payload(&payload)?;
                    self.handle_simple_query(&sql).await?;
                }
                // Extended Query sub-protocol —————————————————————
                MSG_PARSE => {
                    // Parse: store the SQL for later Execute.
                    // Format: stmt_name\0 query\0 int16(num_params) [int32(oid)...]
                    let (_stmt_name, pos) = read_cstring_pair_first(&payload)?;
                    let (sql, _) = read_cstring_pair_at(&payload, pos)?;
                    self.pending_sql = Some(sql);
                    self.write_all(&encode_parse_complete()).await?;
                }
                MSG_BIND => {
                    // Bind: we don't support parameters, just acknowledge.
                    self.write_all(&encode_bind_complete()).await?;
                }
                MSG_DESCRIBE => {
                    // Describe: send ParameterDescription (0 params) + RowDescription or NoData.
                    self.write_all(&encode_parameter_description_empty()).await?;
                    if let Some(sql) = &self.pending_sql {
                        let outcome = self.handler.handle_query_with_session(sql, &self.session);
                        if let QueryOutcome::Result(ref result) = outcome {
                            if result.columns.is_empty() {
                                self.write_all(&encode_no_data()).await?;
                            } else {
                                self.write_all(&encode_row_description(&result.columns)).await?;
                            }
                        } else {
                            self.write_all(&encode_no_data()).await?;
                        }
                    } else {
                        self.write_all(&encode_no_data()).await?;
                    }
                }
                MSG_EXECUTE => {
                    // Execute: run the pending SQL.
                    if let Some(sql) = self.pending_sql.take() {
                        self.handle_execute(&sql).await?;
                    } else {
                        // No pending statement — send an empty CommandComplete.
                        self.write_all(&encode_command_complete("SELECT 0")).await?;
                    }
                }
                MSG_SYNC => {
                    // Sync: flush + ReadyForQuery.
                    self.write_all(&encode_ready_for_query(TX_IDLE)).await?;
                    self.stream.flush().await?;
                }
                MSG_CLOSE => {
                    // Close a statement or portal — just acknowledge.
                    self.pending_sql = None;
                    self.write_all(&encode_close_complete()).await?;
                }
                MSG_TERMINATE => {
                    return Err(ConnectionError::Closed);
                }
                other => {
                    // Unknown/unsupported message type — send error and continue.
                    let err_msg = format!("unsupported message type: {}", other as char);
                    self.write_all(&encode_error_response("ERROR", "0A000", &err_msg)).await?;
                    self.write_all(&encode_ready_for_query(TX_IDLE)).await?;
                    self.stream.flush().await?;
                }
            }
        }
    }

    /// Handle a simple Query message, including multi-statement batches.
    async fn handle_simple_query(&mut self, sql: &str) -> Result<(), ConnectionError> {
        let outcomes = self.handler.handle_query_batch_with_session(sql, &self.session);
        for outcome in outcomes {
            let should_stop = matches!(outcome, QueryOutcome::Error { .. });
            self.write_query_outcome(outcome, true).await?;
            if should_stop {
                break;
            }
        }

        self.write_all(&encode_ready_for_query(TX_IDLE)).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_query_outcome(
        &mut self,
        outcome: QueryOutcome,
        include_row_description: bool,
    ) -> Result<(), ConnectionError> {
        match outcome {
            QueryOutcome::Result(result) => {
                // Only send RowDescription + DataRows if there are columns.
                if include_row_description && !result.columns.is_empty() {
                    self.write_all(&encode_row_description(&result.columns)).await?;
                }
                for row in &result.rows {
                    let refs: Vec<Option<&str>> = row.iter().map(|v| v.as_deref()).collect();
                    self.write_all(&encode_data_row(&refs)).await?;
                }

                // CommandComplete with custom tag or default SELECT tag.
                let tag = result.command_tag.unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
                self.write_all(&encode_command_complete(&tag)).await?;
            }
            QueryOutcome::CopyOut(result) => {
                let column_formats = vec![1i16; result.columns.len()];
                self.write_all(&encode_copy_out_response(&column_formats)).await?;

                let payload_chunks =
                    encode_copy_binary_chunks(&result.columns, &result.rows).map_err(ConnectionError::Protocol)?;
                for chunk in payload_chunks {
                    self.write_all(&encode_copy_data(&chunk)).await?;
                }
                self.write_all(&encode_copy_done()).await?;

                let tag = result.command_tag.unwrap_or_else(|| format!("COPY {}", result.rows.len()));
                self.write_all(&encode_command_complete(&tag)).await?;
            }
            QueryOutcome::Error { severity, code, message } => {
                self.write_all(&encode_error_response(&severity, &code, &message)).await?;
            }
        }
        Ok(())
    }

    /// Handle an Execute in the extended-query sub-protocol.
    ///
    /// Like `handle_query` but does NOT send RowDescription (already sent by Describe)
    /// and does NOT send ReadyForQuery (that comes with Sync).
    async fn handle_execute(&mut self, sql: &str) -> Result<(), ConnectionError> {
        let outcome = self.handler.handle_query_with_session(sql, &self.session);
        self.write_query_outcome(outcome, false).await
    }

    /// Read a raw message from the stream: returns (type_byte, payload).
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), ConnectionError> {
        let msg_type = self.stream.read_u8().await?;
        let length = self.stream.read_i32().await? as usize;
        if length < 4 {
            return Err(ConnectionError::Protocol("message length < 4".into()));
        }
        let payload_len = length - 4;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }
        Ok((msg_type, payload))
    }

    /// Read the startup message (no type byte, just length + payload).
    async fn read_startup_message(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let length = self.stream.read_i32().await? as usize;
        if length < 4 {
            return Err(ConnectionError::Protocol("startup message length < 4".into()));
        }
        let payload_len = length - 4;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }
        Ok(payload)
    }

    /// Write all bytes to the stream.
    async fn write_all(&mut self, data: &[u8]) -> Result<(), ConnectionError> {
        self.stream.write_all(data).await?;
        Ok(())
    }
}

// ── Error type ─────────────────────────────────────────────────────

/// Errors that can occur during connection handling.
#[derive(Debug)]
pub enum ConnectionError {
    /// I/O error on the underlying stream.
    Io(std::io::Error),
    /// Protocol violation (e.g. unexpected message type, bad startup).
    Protocol(String),
    /// Connection closed by client (graceful terminate).
    Closed,
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "I/O error: {}", e),
            ConnectionError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            ConnectionError::Closed => write!(f, "Connection closed"),
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Extract a null-terminated C string from a message payload.
fn read_cstring_from_payload(payload: &[u8]) -> Result<String, ConnectionError> {
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ConnectionError::Protocol("missing null terminator in query".into()))?;
    String::from_utf8(payload[..end].to_vec())
        .map_err(|e| ConnectionError::Protocol(format!("invalid UTF-8 in query: {e}")))
}

/// Read the first C-string from `payload` (e.g. the statement name in a Parse message).
/// Returns (string, position after the null terminator).
fn read_cstring_pair_first(payload: &[u8]) -> Result<(String, usize), ConnectionError> {
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ConnectionError::Protocol("missing null terminator".into()))?;
    let s = String::from_utf8(payload[..end].to_vec())
        .map_err(|e| ConnectionError::Protocol(format!("invalid UTF-8: {e}")))?;
    Ok((s, end + 1))
}

/// Read a C-string from `payload` starting at `pos`.
fn read_cstring_pair_at(payload: &[u8], pos: usize) -> Result<(String, usize), ConnectionError> {
    if pos >= payload.len() {
        return Err(ConnectionError::Protocol("unexpected end of payload".into()));
    }
    let end = payload[pos..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| ConnectionError::Protocol("missing null terminator".into()))?;
    let s = String::from_utf8(payload[pos..pos + end].to_vec())
        .map_err(|e| ConnectionError::Protocol(format!("invalid UTF-8: {e}")))?;
    Ok((s, pos + end + 1))
}

const COPY_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";

fn encode_copy_binary_chunks(
    columns: &[ColumnDescription],
    rows: &[Vec<Option<String>>],
) -> Result<Vec<Vec<u8>>, String> {
    let mut chunks = Vec::with_capacity(rows.len().saturating_add(1));
    let mut header_sent = false;

    for row in rows {
        if row.len() != columns.len() {
            return Err("copy row length does not match column length".into());
        }

        let mut payload = Vec::new();
        if !header_sent {
            payload.extend_from_slice(COPY_BINARY_SIGNATURE);
            payload.extend_from_slice(&0i32.to_be_bytes());
            payload.extend_from_slice(&0i32.to_be_bytes());
            header_sent = true;
        }

        payload.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (column, value) in columns.iter().zip(row.iter()) {
            match value {
                Some(value) => {
                    let encoded = encode_copy_binary_value(column.type_oid, value)?;
                    payload.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                    payload.extend_from_slice(&encoded);
                }
                None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }

        chunks.push(payload);
    }

    let mut trailer = Vec::new();
    if !header_sent {
        trailer.extend_from_slice(COPY_BINARY_SIGNATURE);
        trailer.extend_from_slice(&0i32.to_be_bytes());
        trailer.extend_from_slice(&0i32.to_be_bytes());
    }
    trailer.extend_from_slice(&(-1i16).to_be_bytes());
    chunks.push(trailer);

    Ok(chunks)
}

fn encode_copy_binary_value(type_oid: crate::type_mapping::PgOid, value: &str) -> Result<Vec<u8>, String> {
    match type_oid {
        PG_OID_TEXT | PG_OID_VARCHAR => Ok(value.as_bytes().to_vec()),
        PG_OID_BOOL => match value {
            "t" | "true" | "1" => Ok(vec![1]),
            "f" | "false" | "0" => Ok(vec![0]),
            _ => Err(format!("cannot encode '{value}' as bool")),
        },
        PG_OID_INT8 => value
            .parse::<i64>()
            .map(|parsed| parsed.to_be_bytes().to_vec())
            .map_err(|err| format!("cannot encode '{value}' as int8: {err}")),
        PG_OID_INT4 => value
            .parse::<i32>()
            .map(|parsed| parsed.to_be_bytes().to_vec())
            .map_err(|err| format!("cannot encode '{value}' as int4: {err}")),
        PG_OID_OID => value
            .parse::<u32>()
            .map(|parsed| parsed.to_be_bytes().to_vec())
            .map_err(|err| format!("cannot encode '{value}' as oid: {err}")),
        PG_OID_FLOAT8 => value
            .parse::<f64>()
            .map(|parsed| parsed.to_be_bytes().to_vec())
            .map_err(|err| format!("cannot encode '{value}' as float8: {err}")),
        _ => Err(format!("unsupported binary COPY type oid: {type_oid}")),
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire::messages::*;

    // ── Mock QueryHandler ──────────────────────────────────────

    struct MockHandler {
        /// Pre-configured responses keyed by SQL string.
        responses: std::collections::HashMap<String, QueryOutcome>,
    }

    impl MockHandler {
        fn new() -> Self {
            Self { responses: std::collections::HashMap::new() }
        }

        fn on_query(mut self, sql: &str, outcome: QueryOutcome) -> Self {
            self.responses.insert(sql.to_string(), outcome);
            self
        }
    }

    impl QueryHandler for MockHandler {
        fn handle_query(&self, sql: &str) -> QueryOutcome {
            self.responses.get(sql).cloned().unwrap_or_else(|| QueryOutcome::Error {
                severity: "ERROR".into(),
                code: "42P01".into(),
                message: format!("unknown query: {}", sql),
            })
        }
    }

    struct SessionAwareHandler;

    impl QueryHandler for SessionAwareHandler {
        fn handle_query(&self, _sql: &str) -> QueryOutcome {
            QueryOutcome::Error {
                severity: "ERROR".into(),
                code: "0A000".into(),
                message: "session-aware path not used".into(),
            }
        }

        fn handle_query_with_session(&self, _sql: &str, session: &SessionContext) -> QueryOutcome {
            QueryOutcome::Result(QueryResult {
                columns: vec![],
                rows: vec![],
                command_tag: Some(format!(
                    "USER {} DB {} PID {} PORT {}",
                    session.current_user, session.current_database, session.backend_pid, session.server_port
                )),
            })
        }
    }

    // ── Test helpers ───────────────────────────────────────────

    /// Build a raw StartupMessage to send to the server.
    fn build_startup(user: &str, database: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
        payload.extend_from_slice(b"user\0");
        payload.extend_from_slice(user.as_bytes());
        payload.push(0);
        payload.extend_from_slice(b"database\0");
        payload.extend_from_slice(database.as_bytes());
        payload.push(0);
        payload.push(0); // terminator

        let total_len = (4 + payload.len()) as i32;
        let mut packet = Vec::new();
        packet.extend_from_slice(&total_len.to_be_bytes());
        packet.extend_from_slice(&payload);
        packet
    }

    /// Build a raw Query('Q') message.
    fn build_query(sql: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(MSG_QUERY);
        let len = (4 + sql.len() + 1) as i32;
        packet.extend_from_slice(&len.to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        packet
    }

    /// Build a raw Terminate('X') message.
    fn build_terminate() -> Vec<u8> {
        vec![MSG_TERMINATE, 0, 0, 0, 4]
    }

    /// Read one wire protocol message from a stream. Returns (type_byte, payload).
    async fn client_read_message<R: AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<(u8, Vec<u8>)> {
        let msg_type = stream.read_u8().await?;
        let length = stream.read_i32().await? as usize;
        let payload_len = length - 4;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        Ok((msg_type, payload))
    }

    /// Perform the startup handshake from the client side (send startup, read until ReadyForQuery).
    /// Returns all message types received during handshake.
    async fn client_do_handshake<S: AsyncRead + AsyncWrite + Unpin>(client: &mut S) -> Vec<u8> {
        client.write_all(&build_startup("test", "test")).await.unwrap();
        client.flush().await.unwrap();

        let mut types = Vec::new();
        loop {
            let (msg_type, _payload) = client_read_message(client).await.unwrap();
            types.push(msg_type);
            if msg_type == MSG_READY_FOR_QUERY {
                break;
            }
        }
        types
    }

    // ── RED Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn startup_handshake_sends_auth_ok() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(
            types.contains(&MSG_AUTHENTICATION),
            "Handshake must include AuthenticationOk (R): got {:?}",
            types.iter().map(|&b| b as char).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn startup_handshake_sends_parameter_statuses() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(
            types.contains(&MSG_PARAMETER_STATUS),
            "Handshake must include ParameterStatus (S): got {:?}",
            types.iter().map(|&b| b as char).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn startup_handshake_sends_backend_key_data() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(
            types.contains(&MSG_BACKEND_KEY_DATA),
            "Handshake must include BackendKeyData (K): got {:?}",
            types.iter().map(|&b| b as char).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn startup_handshake_ends_with_ready_for_query() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY, "Handshake must end with ReadyForQuery (Z)");
    }

    #[tokio::test]
    async fn startup_handshake_message_order_matches_postgres() {
        // Postgres sends: R, S..., K, Z
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );
        // First message must be Authentication
        assert_eq!(types[0], MSG_AUTHENTICATION, "First message must be Authentication (R)");
        // Last message must be ReadyForQuery
        assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY, "Last must be ReadyForQuery (Z)");
        // BackendKeyData comes before ReadyForQuery but after ParameterStatus
        let k_pos = types.iter().position(|&t| t == MSG_BACKEND_KEY_DATA).unwrap();
        let z_pos = types.iter().position(|&t| t == MSG_READY_FOR_QUERY).unwrap();
        assert!(k_pos < z_pos, "BackendKeyData must come before ReadyForQuery");
    }

    #[tokio::test]
    async fn query_returns_row_description_and_data_rows() {
        let handler = MockHandler::new().on_query(
            "SELECT 1",
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "answer".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 23, // int4
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![vec![Some("1".into())]],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (query_types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                // Handshake first
                let _ = client_do_handshake(&mut client).await;

                // Send query
                client.write_all(&build_query("SELECT 1")).await.unwrap();
                client.flush().await.unwrap();

                // Read response messages until ReadyForQuery
                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                // Send terminate
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(query_types.contains(&MSG_ROW_DESCRIPTION), "Must send RowDescription (T)");
        assert!(query_types.contains(&MSG_DATA_ROW), "Must send DataRow (D)");
        assert!(query_types.contains(&MSG_COMMAND_COMPLETE), "Must send CommandComplete (C)");
        assert_eq!(*query_types.last().unwrap(), MSG_READY_FOR_QUERY, "Must end with ReadyForQuery");
    }

    #[tokio::test]
    async fn query_error_returns_error_response() {
        let handler = MockHandler::new().on_query(
            "SELEKT bad",
            QueryOutcome::Error { severity: "ERROR".into(), code: "42601".into(), message: "syntax error".into() },
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (query_types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_query("SELEKT bad")).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(query_types.contains(&MSG_ERROR_RESPONSE), "Must send ErrorResponse (E)");
        assert_eq!(*query_types.last().unwrap(), MSG_READY_FOR_QUERY, "Must end with ReadyForQuery");
    }

    #[tokio::test]
    async fn terminate_closes_connection_gracefully() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (_, result) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
            },
            async move { conn.run().await }
        );

        // run() should return Ok(()) or Err(Closed) — not an I/O error
        match result {
            Ok(()) => {}
            Err(ConnectionError::Closed) => {}
            Err(e) => panic!("Expected Ok or Closed, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn multiple_queries_in_sequence() {
        let handler = MockHandler::new()
            .on_query(
                "SELECT 1",
                QueryOutcome::Result(QueryResult {
                    columns: vec![ColumnDescription {
                        name: "a".into(),
                        table_oid: 0,
                        column_index: 0,
                        type_oid: 23,
                        type_size: 4,
                        type_modifier: -1,
                        format_code: 0,
                    }],
                    rows: vec![vec![Some("1".into())]],
                    command_tag: None,
                }),
            )
            .on_query(
                "SELECT 2",
                QueryOutcome::Result(QueryResult {
                    columns: vec![ColumnDescription {
                        name: "b".into(),
                        table_oid: 0,
                        column_index: 0,
                        type_oid: 23,
                        type_size: 4,
                        type_modifier: -1,
                        format_code: 0,
                    }],
                    rows: vec![vec![Some("2".into())]],
                    command_tag: None,
                }),
            );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (query_responses, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                let mut all_responses = Vec::new();

                // Query 1
                client.write_all(&build_query("SELECT 1")).await.unwrap();
                client.flush().await.unwrap();
                let mut types1 = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types1.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }
                all_responses.push(types1);

                // Query 2
                client.write_all(&build_query("SELECT 2")).await.unwrap();
                client.flush().await.unwrap();
                let mut types2 = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types2.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }
                all_responses.push(types2);

                // Terminate
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                all_responses
            },
            async move { conn.run().await }
        );

        assert_eq!(query_responses.len(), 2);
        for (i, types) in query_responses.iter().enumerate() {
            assert!(types.contains(&MSG_ROW_DESCRIPTION), "Query {} missing RowDescription", i);
            assert!(types.contains(&MSG_DATA_ROW), "Query {} missing DataRow", i);
            assert!(types.contains(&MSG_COMMAND_COMPLETE), "Query {} missing CommandComplete", i);
            assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY, "Query {} must end with ReadyForQuery", i);
        }
    }

    #[tokio::test]
    async fn query_with_multiple_rows() {
        let handler = MockHandler::new().on_query(
            "SELECT * FROM t",
            QueryOutcome::Result(QueryResult {
                columns: vec![
                    ColumnDescription {
                        name: "id".into(),
                        table_oid: 0,
                        column_index: 0,
                        type_oid: 23,
                        type_size: 4,
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
                ],
                rows: vec![
                    vec![Some("1".into()), Some("alice".into())],
                    vec![Some("2".into()), Some("bob".into())],
                    vec![Some("3".into()), None],
                ],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (responses, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_query("SELECT * FROM t")).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                let mut data_row_count = 0u32;
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_DATA_ROW {
                        data_row_count += 1;
                    }
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                (types, data_row_count)
            },
            async move { conn.run().await }
        );

        assert_eq!(responses.1, 3, "Expected 3 DataRow messages");
        // Order: T, D, D, D, C, Z
        let types = &responses.0;
        assert_eq!(types[0], MSG_ROW_DESCRIPTION);
        assert_eq!(types[1], MSG_DATA_ROW);
        assert_eq!(types[2], MSG_DATA_ROW);
        assert_eq!(types[3], MSG_DATA_ROW);
        assert_eq!(types[4], MSG_COMMAND_COMPLETE);
        assert_eq!(types[5], MSG_READY_FOR_QUERY);
    }

    #[tokio::test]
    async fn empty_result_sends_zero_data_rows() {
        let handler = MockHandler::new().on_query(
            "SELECT * FROM empty",
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "x".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (responses, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_query("SELECT * FROM empty")).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        // T, C, Z — no D messages
        assert!(responses.contains(&MSG_ROW_DESCRIPTION), "Must send RowDescription");
        assert!(responses.contains(&MSG_COMMAND_COMPLETE), "Must send CommandComplete");
        assert!(!responses.contains(&MSG_DATA_ROW), "Must NOT send DataRow for empty result");
        assert_eq!(*responses.last().unwrap(), MSG_READY_FOR_QUERY);
    }

    #[tokio::test]
    async fn command_complete_tag_includes_row_count() {
        let handler = MockHandler::new().on_query(
            "SELECT 1",
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "x".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![vec![Some("1".into())]],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (cmd_tag, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_query("SELECT 1")).await.unwrap();
                client.flush().await.unwrap();

                let mut tag = String::new();
                loop {
                    let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
                    if msg_type == MSG_COMMAND_COMPLETE {
                        let end = payload.iter().position(|&b| b == 0).unwrap();
                        tag = String::from_utf8(payload[..end].to_vec()).unwrap();
                    }
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                tag
            },
            async move { conn.run().await }
        );

        assert_eq!(cmd_tag, "SELECT 1", "CommandComplete tag should be 'SELECT <rowcount>'");
    }

    #[tokio::test]
    async fn backend_key_data_contains_configured_values() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(1234, 5678);

        let (key_data, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                client.write_all(&build_startup("test", "test")).await.unwrap();
                client.flush().await.unwrap();

                let mut pid = 0i32;
                let mut secret = 0i32;
                loop {
                    let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
                    if msg_type == MSG_BACKEND_KEY_DATA {
                        pid = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        secret = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                    }
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                (pid, secret)
            },
            async move { conn.run().await }
        );

        assert_eq!(key_data.0, 1234, "PID should match configured value");
        assert_eq!(key_data.1, 5678, "Secret should match configured value");
    }

    // ── Authentication tests ───────────────────────────────────

    /// Build a raw PasswordMessage ('p') with a null-terminated password.
    fn build_password(password: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(MSG_PASSWORD);
        let len = (4 + password.len() + 1) as i32;
        packet.extend_from_slice(&len.to_be_bytes());
        packet.extend_from_slice(password.as_bytes());
        packet.push(0);
        packet
    }

    /// Perform cleartext auth handshake from client side:
    /// Send startup → read AuthCleartextPassword → send password → read until ReadyForQuery.
    async fn client_do_auth_handshake<S: AsyncRead + AsyncWrite + Unpin>(client: &mut S, password: &str) -> Vec<u8> {
        client.write_all(&build_startup("testuser", "testdb")).await.unwrap();
        client.flush().await.unwrap();

        let mut types = Vec::new();
        loop {
            let (msg_type, payload) = client_read_message(client).await.unwrap();
            types.push(msg_type);

            if msg_type == MSG_AUTHENTICATION {
                let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if auth_type == AUTH_CLEARTEXT_PASSWORD {
                    // Server requested password — send it.
                    client.write_all(&build_password(password)).await.unwrap();
                    client.flush().await.unwrap();
                }
                // AUTH_OK (0) → continue reading
            }
            if msg_type == MSG_READY_FOR_QUERY || msg_type == MSG_ERROR_RESPONSE {
                break;
            }
        }
        types
    }

    #[tokio::test]
    async fn cleartext_auth_success() {
        use crate::pgwire::authenticator::TrustAuthenticator;

        let auth_mode = AuthMode::CleartextPassword(Arc::new(TrustAuthenticator));
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default())
            .with_auth_mode(auth_mode)
            .with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_auth_handshake(&mut client, "anypassword").await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        // Should see: R(cleartext request), R(auth ok), S..., K, Z
        assert!(types.contains(&MSG_READY_FOR_QUERY), "Handshake should complete successfully");
        // First R is cleartext request, second is auth ok
        let auth_count = types.iter().filter(|&&t| t == MSG_AUTHENTICATION).count();
        assert_eq!(auth_count, 2, "Should have two Authentication messages (request + ok)");
    }

    #[tokio::test]
    async fn cleartext_auth_failure() {
        use crate::pgwire::authenticator::RejectAuthenticator;

        let auth_mode = AuthMode::CleartextPassword(Arc::new(RejectAuthenticator));
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default())
            .with_auth_mode(auth_mode)
            .with_backend_key(42, 99);

        let (types, result) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_auth_handshake(&mut client, "wrong").await;
                types
            },
            async move { conn.run().await }
        );

        // Should see: R(cleartext request), E(error) — no ReadyForQuery.
        assert!(types.contains(&MSG_ERROR_RESPONSE), "Should send ErrorResponse on auth failure");
        assert!(!types.contains(&MSG_READY_FOR_QUERY), "Should NOT send ReadyForQuery after auth failure");
        assert!(result.is_err(), "Connection should return error on auth failure");
    }

    #[tokio::test]
    async fn trust_mode_skips_password_exchange() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(MockHandler::new());
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default())
            .with_auth_mode(AuthMode::Trust)
            .with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let types = client_do_handshake(&mut client).await;
                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        // Only one Authentication message (AuthOk), no cleartext request.
        let auth_count = types.iter().filter(|&&t| t == MSG_AUTHENTICATION).count();
        assert_eq!(auth_count, 1, "Trust mode should send only AuthOk (1 R message)");
        assert!(types.contains(&MSG_READY_FOR_QUERY));
    }

    #[tokio::test]
    async fn query_handler_receives_session_context_from_startup() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(SessionAwareHandler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default())
            .with_backend_key(42, 99)
            .with_server_port(15432);

        let (command_tag, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                client.write_all(&build_startup("admin", "analytics")).await.unwrap();
                client.flush().await.unwrap();

                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_query("SELECT 1")).await.unwrap();
                client.flush().await.unwrap();

                let mut tag = String::new();
                loop {
                    let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
                    if msg_type == MSG_COMMAND_COMPLETE {
                        let end = payload.iter().position(|&b| b == 0).unwrap();
                        tag = String::from_utf8(payload[..end].to_vec()).unwrap();
                    }
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                tag
            },
            async move { conn.run().await }
        );

        assert_eq!(command_tag, "USER admin DB analytics PID 42 PORT 15432");
    }

    // ── Extended Query Protocol ─────────────────────────────────

    /// Build a Parse message: stmt_name\0 query\0 int16(0)
    fn build_parse(stmt_name: &str, query: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(stmt_name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(query.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&0i16.to_be_bytes()); // zero param types
        let len = (payload.len() + 4) as i32;
        let mut msg = Vec::new();
        msg.push(MSG_PARSE);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Build a Bind message (minimal: unnamed statement, unnamed portal, no params).
    fn build_bind() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(0); // portal name (unnamed)
        payload.push(0); // statement name (unnamed)
        payload.extend_from_slice(&0i16.to_be_bytes()); // num format codes
        payload.extend_from_slice(&0i16.to_be_bytes()); // num parameters
        payload.extend_from_slice(&0i16.to_be_bytes()); // num result format codes
        let len = (payload.len() + 4) as i32;
        let mut msg = Vec::new();
        msg.push(MSG_BIND);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Build a Describe message (portal variant).
    fn build_describe_portal() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'P'); // describe portal (not statement)
        payload.push(0); // unnamed portal
        let len = (payload.len() + 4) as i32;
        let mut msg = Vec::new();
        msg.push(MSG_DESCRIBE);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Build an Execute message (unnamed portal, fetch all).
    fn build_execute() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(0); // unnamed portal
        payload.extend_from_slice(&0i32.to_be_bytes()); // max rows = 0 (all)
        let len = (payload.len() + 4) as i32;
        let mut msg = Vec::new();
        msg.push(MSG_EXECUTE);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Build a Sync message (empty payload).
    fn build_sync() -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(MSG_SYNC);
        msg.extend_from_slice(&4i32.to_be_bytes());
        msg
    }

    /// Build a Close message (portal variant).
    fn build_close_portal() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'P'); // close portal
        payload.push(0); // unnamed portal
        let len = (payload.len() + 4) as i32;
        let mut msg = Vec::new();
        msg.push(MSG_CLOSE);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    #[tokio::test]
    async fn extended_query_parse_bind_execute_sync() {
        let handler = MockHandler::new().on_query(
            "SELECT 1",
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "one".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![vec![Some("1".into())]],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                // Send Parse → Bind → Execute → Sync
                client.write_all(&build_parse("", "SELECT 1")).await.unwrap();
                client.write_all(&build_bind()).await.unwrap();
                client.write_all(&build_execute()).await.unwrap();
                client.write_all(&build_sync()).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        // Expected: ParseComplete(1), BindComplete(2), DataRow(D), CommandComplete(C), ReadyForQuery(Z)
        assert!(types.contains(&MSG_PARSE_COMPLETE), "Should get ParseComplete");
        assert!(types.contains(&MSG_BIND_COMPLETE), "Should get BindComplete");
        assert!(types.contains(&MSG_DATA_ROW), "Should get DataRow");
        assert!(types.contains(&MSG_COMMAND_COMPLETE), "Should get CommandComplete");
        assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY);
    }

    #[tokio::test]
    async fn extended_query_close_sends_close_complete() {
        let handler = MockHandler::new();
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                client.write_all(&build_close_portal()).await.unwrap();
                client.write_all(&build_sync()).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        assert!(types.contains(&MSG_CLOSE_COMPLETE), "Should get CloseComplete");
        assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY);
    }

    #[tokio::test]
    async fn extended_query_describe_sends_parameter_description() {
        let handler = MockHandler::new().on_query(
            "SELECT 1",
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "x".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![vec![Some("1".into())]],
                command_tag: None,
            }),
        );

        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let handler = Arc::new(handler);
        let mut conn = PgConnection::new(server_stream, handler, ServerParams::default()).with_backend_key(42, 99);

        let (types, _) = tokio::join!(
            async move {
                let mut client = client_stream;
                let _ = client_do_handshake(&mut client).await;

                // Parse, then Describe, then Sync
                client.write_all(&build_parse("", "SELECT 1")).await.unwrap();
                client.write_all(&build_describe_portal()).await.unwrap();
                client.write_all(&build_sync()).await.unwrap();
                client.flush().await.unwrap();

                let mut types = Vec::new();
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    types.push(msg_type);
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                types
            },
            async move { conn.run().await }
        );

        // Should include: ParseComplete(1), ParameterDescription(t), RowDescription(T), ReadyForQuery(Z)
        assert!(types.contains(&MSG_PARSE_COMPLETE), "Should get ParseComplete");
        assert!(types.contains(&MSG_PARAMETER_DESCRIPTION), "Should get ParameterDescription");
        assert!(types.contains(&MSG_ROW_DESCRIPTION), "Should get RowDescription for SELECT");
        assert_eq!(*types.last().unwrap(), MSG_READY_FOR_QUERY);
    }
}
