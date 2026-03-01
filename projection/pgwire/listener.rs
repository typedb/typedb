/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! TCP listener for the Postgres wire protocol.
//!
//! Accepts incoming connections and spawns a [`PgConnection`] task for each.

use std::sync::Arc;

use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

use crate::pgwire::{
    authenticator::AuthMode,
    connection::{PgConnection, QueryHandler, ServerParams},
};

// ── PgWireListener ─────────────────────────────────────────────────

/// Accepts TCP connections and spawns a [`PgConnection`] for each client.
pub struct PgWireListener<H: QueryHandler + 'static> {
    listener: TcpListener,
    handler: Arc<H>,
    params: ServerParams,
    auth_mode: AuthMode,
    next_process_id: std::sync::atomic::AtomicI32,
}

impl<H: QueryHandler + 'static> PgWireListener<H> {
    /// Bind to the given address and prepare to accept connections.
    pub async fn bind(addr: &str, handler: Arc<H>, params: ServerParams) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            handler,
            params,
            auth_mode: AuthMode::Trust,
            next_process_id: std::sync::atomic::AtomicI32::new(1),
        })
    }

    /// Create a listener from an already-bound `TcpListener` (useful for tests).
    pub fn from_listener(listener: TcpListener, handler: Arc<H>, params: ServerParams) -> Self {
        Self {
            listener,
            handler,
            params,
            auth_mode: AuthMode::Trust,
            next_process_id: std::sync::atomic::AtomicI32::new(1),
        }
    }

    /// Set the authentication mode for spawned connections.
    pub fn with_auth_mode(mut self, auth_mode: AuthMode) -> Self {
        self.auth_mode = auth_mode;
        self
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Run the accept loop until the shutdown signal fires.
    ///
    /// Each accepted connection is spawned as an independent tokio task.
    /// Returns the list of spawned task handles when shutdown is received.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        loop {
            tokio::select! {
                // Bias toward shutdown so we don't accept after signal.
                biased;

                result = shutdown.changed() => {
                    // Shutdown signaled (or sender dropped).
                    let _ = result;
                    break;
                }

                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            handles.push(self.spawn_connection(stream));
                        }
                        Err(_e) => {
                            // Transient accept error — continue.
                            continue;
                        }
                    }
                }
            }
        }

        handles
    }

    /// Accept and handle a single connection (factored out for testability).
    fn spawn_connection(&self, stream: tokio::net::TcpStream) -> JoinHandle<()> {
        let handler = Arc::clone(&self.handler);
        let params = self.params.clone();
        let auth_mode = self.auth_mode.clone();
        let pid = self.next_pid();

        tokio::spawn(async move {
            let mut conn =
                PgConnection::new(stream, handler, params).with_backend_key(pid, 0).with_auth_mode(auth_mode);
            // Connection errors are expected (e.g. client disconnect).
            let _ = conn.run().await;
        })
    }

    /// Allocate a new process ID for BackendKeyData.
    fn next_pid(&self) -> i32 {
        self.next_process_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;
    use crate::pgwire::{
        authenticator::TrustAuthenticator,
        connection::{QueryOutcome, QueryResult},
        messages::*,
    };

    // ── Mock QueryHandler ──────────────────────────────────────

    struct EchoHandler;

    impl QueryHandler for EchoHandler {
        fn handle_query(&self, sql: &str) -> QueryOutcome {
            QueryOutcome::Result(QueryResult {
                columns: vec![ColumnDescription {
                    name: "sql".into(),
                    table_oid: 0,
                    column_index: 0,
                    type_oid: 25, // text
                    type_size: -1,
                    type_modifier: -1,
                    format_code: 0,
                }],
                rows: vec![vec![Some(sql.to_string())]],
                command_tag: None,
            })
        }
    }

    // ── Helpers ────────────────────────────────────────────────

    /// Build a raw StartupMessage.
    fn build_startup(user: &str, database: &str) -> Vec<u8> {
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

    /// Build a raw Query message.
    fn build_query(sql: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(MSG_QUERY);
        let len = (4 + sql.len() + 1) as i32;
        packet.extend_from_slice(&len.to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        packet
    }

    /// Build a raw Terminate message.
    fn build_terminate() -> Vec<u8> {
        vec![MSG_TERMINATE, 0, 0, 0, 4]
    }

    /// Read one wire protocol message from a stream. Returns (type_byte, payload).
    async fn client_read_message<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<(u8, Vec<u8>)> {
        let msg_type = stream.read_u8().await?;
        let length = stream.read_i32().await? as usize;
        let payload_len = length - 4;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        Ok((msg_type, payload))
    }

    /// Complete the startup handshake from client side.
    async fn client_handshake(stream: &mut TcpStream) {
        stream.write_all(&build_startup("test", "testdb")).await.unwrap();
        stream.flush().await.unwrap();

        loop {
            let (msg_type, _) = client_read_message(stream).await.unwrap();
            if msg_type == MSG_READY_FOR_QUERY {
                break;
            }
        }
    }

    /// Start a listener on a random port and return (listener, addr).
    async fn start_listener() -> (PgWireListener<EchoHandler>, String) {
        let handler = Arc::new(EchoHandler);
        let listener = PgWireListener::bind("127.0.0.1:0", handler, ServerParams::default()).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        (listener, addr)
    }

    // ── RED Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn listener_binds_to_random_port() {
        let (listener, addr) = start_listener().await;
        assert!(addr.starts_with("127.0.0.1:"), "Should bind to 127.0.0.1, got: {}", addr);
        let port: u16 = addr.split(':').last().unwrap().parse().unwrap();
        assert!(port > 0, "Port should be assigned");
        drop(listener);
    }

    #[tokio::test]
    async fn listener_accepts_single_connection() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        // Connect as a client
        let mut client = TcpStream::connect(&addr).await.unwrap();
        client_handshake(&mut client).await;

        // Send terminate
        client.write_all(&build_terminate()).await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        // Shutdown the listener
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();

        let handles = server.await.unwrap();
        // At least one connection was spawned
        assert!(!handles.is_empty(), "Should have spawned at least one connection task");
    }

    #[tokio::test]
    async fn listener_accepts_multiple_connections() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        // Connect 3 clients sequentially
        for _ in 0..3 {
            let mut client = TcpStream::connect(&addr).await.unwrap();
            client_handshake(&mut client).await;
            client.write_all(&build_terminate()).await.unwrap();
            client.flush().await.unwrap();
            drop(client);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        shutdown_tx.send(true).unwrap();

        let handles = server.await.unwrap();
        assert!(handles.len() >= 3, "Should have spawned at least 3 tasks, got {}", handles.len());
    }

    #[tokio::test]
    async fn listener_handles_query_through_connection() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        let mut client = TcpStream::connect(&addr).await.unwrap();
        client_handshake(&mut client).await;

        // Send a query
        client.write_all(&build_query("SELECT 1")).await.unwrap();
        client.flush().await.unwrap();

        // Read response: T, D, C, Z
        let mut query_types = Vec::new();
        loop {
            let (msg_type, _) = client_read_message(&mut client).await.unwrap();
            query_types.push(msg_type);
            if msg_type == MSG_READY_FOR_QUERY {
                break;
            }
        }

        assert!(query_types.contains(&MSG_ROW_DESCRIPTION), "Missing RowDescription");
        assert!(query_types.contains(&MSG_DATA_ROW), "Missing DataRow");
        assert!(query_types.contains(&MSG_COMMAND_COMPLETE), "Missing CommandComplete");

        client.write_all(&build_terminate()).await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn listener_concurrent_connections() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        // Spawn 3 concurrent clients
        let mut client_handles = Vec::new();
        for i in 0..3 {
            let addr = addr.clone();
            client_handles.push(tokio::spawn(async move {
                let mut client = TcpStream::connect(&addr).await.unwrap();
                client_handshake(&mut client).await;

                let query = format!("SELECT {}", i);
                client.write_all(&build_query(&query)).await.unwrap();
                client.flush().await.unwrap();

                // Read until ReadyForQuery
                loop {
                    let (msg_type, _) = client_read_message(&mut client).await.unwrap();
                    if msg_type == MSG_READY_FOR_QUERY {
                        break;
                    }
                }

                client.write_all(&build_terminate()).await.unwrap();
                client.flush().await.unwrap();
                true
            }));
        }

        // All clients should complete successfully
        for handle in client_handles {
            assert!(handle.await.unwrap(), "Client should complete successfully");
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        shutdown_tx.send(true).unwrap();

        let handles = server.await.unwrap();
        assert!(handles.len() >= 3, "Should have spawned at least 3 tasks, got {}", handles.len());
    }

    #[tokio::test]
    async fn listener_shutdown_stops_accept_loop() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        // Send shutdown immediately
        shutdown_tx.send(true).unwrap();

        let handles = server.await.unwrap();
        // Should return promptly (no connections to report)
        // The important thing is that run() returned, not hung.
        assert!(handles.is_empty() || handles.len() < 100, "Should exit cleanly");
    }

    #[tokio::test]
    async fn listener_assigns_unique_process_ids() {
        let (listener, addr) = start_listener().await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        let mut pids = Vec::new();
        for _ in 0..3 {
            let mut client = TcpStream::connect(&addr).await.unwrap();
            client.write_all(&build_startup("test", "testdb")).await.unwrap();
            client.flush().await.unwrap();

            // Read handshake, extract BackendKeyData PID
            let mut pid = 0i32;
            loop {
                let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
                if msg_type == MSG_BACKEND_KEY_DATA {
                    pid = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                }
                if msg_type == MSG_READY_FOR_QUERY {
                    break;
                }
            }
            pids.push(pid);

            client.write_all(&build_terminate()).await.unwrap();
            client.flush().await.unwrap();
            drop(client);
        }

        // All PIDs should be unique
        pids.sort();
        pids.dedup();
        assert_eq!(pids.len(), 3, "All 3 connections should have unique PIDs: {:?}", pids);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();
        server.await.unwrap();
    }

    // ── auth_mode propagation ──────────────────────────────────

    #[tokio::test]
    async fn with_auth_mode_builder_sets_mode() {
        let handler = Arc::new(EchoHandler);
        let listener = PgWireListener::bind("127.0.0.1:0", handler, ServerParams::default())
            .await
            .unwrap()
            .with_auth_mode(AuthMode::CleartextPassword(Arc::new(TrustAuthenticator)));
        // Just verify it builds without error and still has a valid local_addr.
        assert!(listener.local_addr().is_ok());
    }

    #[tokio::test]
    async fn auth_mode_cleartext_requires_password_exchange() {
        let handler = Arc::new(EchoHandler);
        let listener = PgWireListener::bind("127.0.0.1:0", handler, ServerParams::default())
            .await
            .unwrap()
            .with_auth_mode(AuthMode::CleartextPassword(Arc::new(TrustAuthenticator)));
        let addr = listener.local_addr().unwrap().to_string();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move { listener.run(shutdown_rx).await });

        let mut client = TcpStream::connect(&addr).await.unwrap();
        // Send startup.
        client.write_all(&build_startup("test", "testdb")).await.unwrap();
        client.flush().await.unwrap();

        // Should receive AuthenticationCleartextPassword (R with int32 = 3)
        let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
        assert_eq!(msg_type, b'R', "Expected AuthenticationCleartextPassword");
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        assert_eq!(auth_type, 3, "auth_type should be 3 for CleartextPassword");

        // Send password.
        let password = b"secret\0";
        let mut pw_msg = Vec::new();
        pw_msg.push(b'p');
        let len = (4 + password.len()) as i32;
        pw_msg.extend_from_slice(&len.to_be_bytes());
        pw_msg.extend_from_slice(password);
        client.write_all(&pw_msg).await.unwrap();
        client.flush().await.unwrap();

        // Should get AuthenticationOk then ReadyForQuery eventually.
        let mut got_auth_ok = false;
        let mut got_ready = false;
        loop {
            let (msg_type, payload) = client_read_message(&mut client).await.unwrap();
            if msg_type == b'R' {
                let t = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if t == 0 {
                    got_auth_ok = true;
                }
            }
            if msg_type == MSG_READY_FOR_QUERY {
                got_ready = true;
                break;
            }
        }
        assert!(got_auth_ok, "Should have received AuthenticationOk");
        assert!(got_ready, "Should have reached ReadyForQuery");

        client.write_all(&build_terminate()).await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();
        server.await.unwrap();
    }
}
