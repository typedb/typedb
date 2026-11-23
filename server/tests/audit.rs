/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use diagnostics::metrics::ActionKind;
use resource::constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD};
use server::{
    audit::{AuditAdmission, AuditEndpoint, AuditEvent, AuditOutcome, AuditReason, AuditStage, Auditor},
    service::QueryType,
};
use test_utils_server::TestServer;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, transport::Channel};
use typedb_protocol::{
    authentication::token::create::{
        Req as TokenCreateReq,
        req::{Credentials, Password},
    },
    transaction::{Client as TransactionClient, Req as TransactionReq, req::Req as TransactionReqKind},
    type_db_client::TypeDbClient,
};
use uuid::Uuid;

const WRONG_PASSWORD: &str = "not-the-password";
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);
const HELD_CHECK: Duration = Duration::from_millis(100);
const QUEUEING_TIME: Duration = Duration::from_secs(1);
const PIPELINED_QUERY_COUNT: usize = 8;

#[derive(Debug, Clone, PartialEq)]
struct RecordedEvent {
    action_kind: ActionKind,
    endpoint: AuditEndpoint,
    has_peer_address: bool,
    accessor: Option<String>,
    target_username: Option<String>,
    database_name: Option<String>,
    transaction_id: Option<Uuid>,
    operation_id: Uuid,
    query_type: Option<QueryType>,
    forced_by: Option<AuditReason>,
    query: Option<String>,
    has_started: bool,
    error_code: Option<String>,
    is_success: bool,
    is_abandoned: bool,
}

impl RecordedEvent {
    fn of(event: &AuditEvent<'_>, query: Option<&str>, has_started: bool) -> Self {
        Self {
            action_kind: event.action_kind,
            endpoint: event.endpoint,
            has_peer_address: event.peer_address.is_some(),
            accessor: event.accessor.map(str::to_owned),
            target_username: event.target_username.map(str::to_owned),
            database_name: event.database_name.map(str::to_owned),
            transaction_id: event.transaction_id,
            operation_id: event.operation_id,
            query_type: event.query_type,
            forced_by: event.forced_by,
            query: query.map(str::to_owned),
            has_started,
            error_code: None,
            is_success: false,
            is_abandoned: false,
        }
    }

    fn ended_with(self, outcome: AuditOutcome<'_>) -> Self {
        Self {
            error_code: match outcome {
                AuditOutcome::Failure(error) => error.code.map(str::to_owned),
                AuditOutcome::Success | AuditOutcome::Abandoned => None,
            },
            is_success: matches!(outcome, AuditOutcome::Success),
            is_abandoned: matches!(outcome, AuditOutcome::Abandoned),
            ..self
        }
    }
}

#[derive(Debug, Default)]
struct Recording {
    started: HashMap<Uuid, RecordedEvent>,
    ended: Vec<RecordedEvent>,
    violations: Vec<String>,
}

impl Recording {
    fn keep(&mut self, event: &AuditEvent<'_>) {
        if self.ended.iter().any(|ended| ended.operation_id == event.operation_id) {
            self.violations.push(format!("Reported after its end: {event:?}"));
        }
        match event.stage {
            AuditStage::Started { query } => {
                let started = RecordedEvent::of(event, query, true);
                if self.started.insert(event.operation_id, started).is_some() {
                    self.violations.push(format!("Started again: {event:?}"));
                }
            }
            AuditStage::Ended { outcome, unreported_query, .. } => match self.started.remove(&event.operation_id) {
                Some(started) => {
                    let ended_alike = started == RecordedEvent::of(event, started.query.as_deref(), true);
                    if unreported_query.is_some() || !ended_alike {
                        self.violations.push(format!("Ended differently from its start {started:?}: {event:?}"));
                    }
                    self.ended.push(started.ended_with(outcome));
                }
                None => self.ended.push(RecordedEvent::of(event, unreported_query, false).ended_with(outcome)),
            },
        }
    }

    fn ended(&self, action_kind: ActionKind) -> Vec<RecordedEvent> {
        self.ended.iter().filter(|event| event.action_kind == action_kind).cloned().collect()
    }
}

#[derive(Debug)]
struct StartGate {
    query: String,
    reached: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
}

struct HeldStart {
    reached: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl HeldStart {
    async fn reached(&mut self) {
        let reached = tokio::time::timeout(EVENT_TIMEOUT, &mut self.reached).await;
        reached.expect("Expected the held start to be recorded").unwrap();
    }

    fn release(self) {
        self.release.send(()).unwrap();
    }
}

#[derive(Debug, Default)]
struct RecordingAuditor {
    recording: Mutex<Recording>,
    start_gate: Mutex<Option<StartGate>>,
}

impl Auditor for RecordingAuditor {
    fn is_enabled(&self) -> bool {
        true
    }

    fn record(&self, event: AuditEvent<'_>) -> AuditAdmission {
        self.recording.lock().unwrap().keep(&event);
        let AuditStage::Started { query: Some(query) } = event.stage else {
            return AuditAdmission::immediate();
        };
        match self.start_gate.lock().unwrap().take_if(|gate| gate.query == query) {
            Some(gate) => {
                gate.reached.send(()).unwrap();
                AuditAdmission::after(async move {
                    let _ = gate.released.await;
                })
            }
            None => AuditAdmission::immediate(),
        }
    }
}

impl RecordingAuditor {
    fn hold_start_of(&self, query: &str) -> HeldStart {
        let (reached_sender, reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let gate = StartGate { query: query.to_owned(), reached: reached_sender, released };
        *self.start_gate.lock().unwrap() = Some(gate);
        HeldStart { reached, release }
    }

    fn assert_consistent(&self) {
        self.assert_consistent_but_abandoned(&[]);
    }

    fn assert_consistent_but_abandoned(&self, abandoned_queries: &[&str]) {
        let recording = self.recording.lock().unwrap();
        assert!(recording.violations.is_empty(), "Inconsistent audit events: {:#?}", recording.violations);
        let abandoned: Vec<_> = recording
            .ended
            .iter()
            .filter(|event| event.is_abandoned)
            .filter(|event| event.query.as_deref().is_none_or(|query| !abandoned_queries.contains(&query)))
            .collect();
        assert!(abandoned.is_empty(), "Unexpected abandoned actions: {abandoned:#?}");
    }

    fn assert_all_ended(&self) {
        let recording = self.recording.lock().unwrap();
        assert!(recording.started.is_empty(), "Actions that never ended: {:#?}", recording.started);
    }

    fn started(&self, action_kind: ActionKind) -> Vec<RecordedEvent> {
        let recording = self.recording.lock().unwrap();
        recording.started.values().filter(|event| event.action_kind == action_kind).cloned().collect()
    }

    fn events(&self, action_kind: ActionKind) -> Vec<RecordedEvent> {
        self.recording.lock().unwrap().ended(action_kind)
    }

    fn closes(&self, transaction_id: Option<Uuid>) -> Vec<RecordedEvent> {
        let closes = self.events(ActionKind::TransactionClose);
        closes.into_iter().filter(|close| close.transaction_id == transaction_id).collect()
    }

    async fn await_recorded<T>(&self, expected: &str, find: impl Fn(&Recording) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
        loop {
            let found = find(&self.recording.lock().unwrap());
            if let Some(found) = found {
                return found;
            }
            assert!(tokio::time::Instant::now() < deadline, "Expected {expected}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn await_events(&self, action_kind: ActionKind, count: usize) -> Vec<RecordedEvent> {
        let events = self
            .await_recorded(&format!("{count} {action_kind:?} events"), |recording| {
                Some(recording.ended(action_kind)).filter(|events| events.len() >= count)
            })
            .await;
        assert_eq!(events.len(), count, "Unexpected {action_kind:?} events: {events:#?}");
        events
    }

    async fn await_last(&self, action_kind: ActionKind, count: usize) -> RecordedEvent {
        self.await_events(action_kind, count).await.pop().unwrap()
    }

    async fn await_query(&self, query: &str) -> RecordedEvent {
        self.await_recorded(&format!("the end of the query '{query}'"), |recording| {
            let mut queries = recording.ended(ActionKind::TransactionQuery).into_iter();
            queries.find(|event| event.query.as_deref() == Some(query))
        })
        .await
    }

    async fn await_close(&self, transaction_id: Option<Uuid>) -> RecordedEvent {
        let mut closes = self
            .await_recorded(&format!("{transaction_id:?} to be closed"), |recording| {
                let mut closes = recording.ended(ActionKind::TransactionClose);
                closes.retain(|close| close.transaction_id == transaction_id);
                Some(closes).filter(|closes| !closes.is_empty())
            })
            .await;
        assert_eq!(closes.len(), 1, "Unexpected closes of {transaction_id:?}: {closes:#?}");
        closes.remove(0)
    }
}

struct AuditedServer {
    server: TestServer,
    http_address: SocketAddr,
    auditor: Arc<RecordingAuditor>,
}

impl AuditedServer {
    async fn start() -> Self {
        let auditor = Arc::new(RecordingAuditor::default());
        let server = TestServer::start(auditor.clone(), |config_builder, _| config_builder).await;
        Self { http_address: server.http_address, server, auditor }
    }

    async fn stop(self) {
        self.server.stop().await;
    }

    async fn send_http(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> TcpStream {
        let body = body.map(|body| body.to_string()).unwrap_or_default();
        let authorization = token.map(|token| format!("Authorization: Bearer {token}\r\n")).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\n\
             {authorization}Content-Length: {}\r\n\r\n{body}",
            self.http_address,
            body.len(),
        );
        let mut stream = TcpStream::connect(self.http_address).await.expect("Failed to connect to the HTTP endpoint");
        stream.write_all(request.as_bytes()).await.expect("Failed to send the HTTP request");
        stream
    }

    async fn http(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        HttpResponse::read(self.send_http(method, path, token, body).await).await
    }

    async fn sign_in(&self) -> String {
        let credentials = serde_json::json!({ "username": DEFAULT_USER_NAME, "password": DEFAULT_USER_PASSWORD });
        self.http("POST", "/v1/signin", None, Some(credentials)).await.field("token")
    }

    async fn create_database_of_people(&self, token: Option<&str>, database: &str) {
        assert_eq!(self.http("POST", &format!("/v1/databases/{database}"), token, None).await.status, 200);
        let define = oneshot_query(database, "schema", "define entity person;");
        let response = self.http("POST", "/v1/query", token, Some(define)).await;
        assert_eq!(response.status, 200, "{}", response.body);
    }

    async fn open_write_transaction(&self, token: Option<&str>, database: &str) -> String {
        let open = serde_json::json!({ "databaseName": database, "transactionType": "write" });
        self.http("POST", "/v1/transactions/open", token, Some(open)).await.field("transactionId")
    }

    async fn commit(&self, token: Option<&str>, transaction_id: &str) {
        let commit_path = format!("/v1/transactions/{transaction_id}/commit");
        assert_eq!(self.http("POST", &commit_path, token, None).await.status, 200);
    }

    async fn count_people(&self, token: Option<&str>, database: &str) -> usize {
        let select = oneshot_query(database, "read", "match $p isa person;");
        let response = self.http("POST", "/v1/query", token, Some(select)).await;
        assert_eq!(response.status, 200, "{}", response.body);
        response.body["answers"].as_array().expect("Expected answers").len()
    }

    async fn grpc_client(&self) -> TypeDbClient<Channel> {
        TypeDbClient::connect(format!("http://{}", self.server.grpc_address))
            .await
            .expect("Failed to connect over gRPC")
    }
}

struct HttpResponse {
    status: u16,
    body: serde_json::Value,
}

impl HttpResponse {
    async fn read(mut stream: TcpStream) -> Self {
        let mut response = String::new();
        stream.read_to_string(&mut response).await.expect("Failed to read the HTTP response");
        let (head, body) = response.split_once("\r\n\r\n").expect("Malformed HTTP response");
        let status = head.split(' ').nth(1).and_then(|status| status.parse().ok()).expect("Malformed HTTP status");
        Self { status, body: serde_json::from_str(body).unwrap_or(serde_json::Value::Null) }
    }

    fn field(&self, name: &str) -> String {
        self.body[name].as_str().unwrap_or_else(|| panic!("Expected '{name}' in {}", self.body)).to_owned()
    }
}

fn oneshot_query(database: &str, transaction_type: &str, query: &str) -> serde_json::Value {
    serde_json::json!({ "databaseName": database, "transactionType": transaction_type, "query": query })
}

fn password_credentials(password: &str) -> TokenCreateReq {
    TokenCreateReq {
        credentials: Some(Credentials::Password(Password {
            username: DEFAULT_USER_NAME.to_owned(),
            password: password.to_owned(),
        })),
    }
}

fn connection_open_req(password: &str) -> typedb_protocol::connection::open::Req {
    typedb_protocol::connection::open::Req {
        version: typedb_protocol::Version::Version as i32,
        extension_version: typedb_protocol::ExtensionVersion::Extension as i32,
        driver_lang: "test".to_owned(),
        driver_version: "0.0.0".to_owned(),
        authentication: Some(password_credentials(password)),
    }
}

fn authenticated<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

struct GrpcTransaction {
    request_sender: mpsc::Sender<TransactionClient>,
}

impl GrpcTransaction {
    async fn open(client: &mut TypeDbClient<Channel>, token: &str, database: &str, type_: i32) -> Self {
        let (request_sender, request_receiver) = mpsc::channel(16);
        let this = Self { request_sender };
        this.send(vec![TransactionReqKind::OpenReq(typedb_protocol::transaction::open::Req {
            database: database.to_owned(),
            r#type: type_,
            options: None,
            network_latency_millis: 0,
        })])
        .await;
        let mut responses = client
            .transaction(authenticated(ReceiverStream::new(request_receiver), token))
            .await
            .expect("Failed to open a transaction stream")
            .into_inner();
        tokio::spawn(async move { while let Ok(Some(_)) = responses.message().await {} });
        this
    }

    async fn send(&self, reqs: Vec<TransactionReqKind>) {
        let reqs = reqs
            .into_iter()
            .map(|req| TransactionReq {
                req_id: Uuid::new_v4().as_bytes().to_vec(),
                metadata: Default::default(),
                req: Some(req),
            })
            .collect();
        self.request_sender.send(TransactionClient { reqs }).await.expect("Transaction stream is closed");
    }

    fn query_req(query: &str) -> TransactionReqKind {
        TransactionReqKind::QueryReq(typedb_protocol::query::Req {
            options: None,
            query: query.to_owned(),
            given: None,
        })
    }

    fn commit_req() -> TransactionReqKind {
        TransactionReqKind::CommitReq(typedb_protocol::transaction::commit::Req {})
    }

    async fn query(&self, query: &str) {
        self.send(vec![Self::query_req(query)]).await;
    }

    async fn commit(&self) {
        self.send(vec![Self::commit_req()]).await;
    }

    async fn close(&self) {
        self.send(vec![TransactionReqKind::CloseReq(typedb_protocol::transaction::close::Req {})]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_actions_are_audited() {
    let server = AuditedServer::start().await;
    let auditor = server.auditor.clone();
    let database = "audit_http";
    let admin = Some(DEFAULT_USER_NAME.to_owned());

    let credentials = |password: &str| serde_json::json!({ "username": DEFAULT_USER_NAME, "password": password });
    let response = server.http("POST", "/v1/signin", None, Some(credentials(WRONG_PASSWORD))).await;
    assert_ne!(response.status, 200);
    let failed_signin = auditor.await_last(ActionKind::SignIn, 1).await;
    assert!(!failed_signin.is_success);
    assert!(failed_signin.error_code.is_some());
    assert_eq!(failed_signin.endpoint, AuditEndpoint::Http);
    assert!(failed_signin.has_peer_address);
    assert_eq!(failed_signin.target_username, admin);
    assert_eq!(failed_signin.accessor, None);

    let response = server.http("POST", "/v1/signin", None, Some(credentials(DEFAULT_USER_PASSWORD))).await;
    assert_eq!(response.status, 200);
    let token = response.field("token");
    let token = Some(token.as_str());
    let signin = auditor.await_last(ActionKind::SignIn, 2).await;
    assert!(signin.is_success);
    assert_eq!(signin.target_username, admin);

    let response = server.http("GET", "/v1/databases", Some("not-a-token"), None).await;
    assert_eq!(response.status, 401);
    let authentications = auditor.await_events(ActionKind::Authenticate, 1).await;
    assert!(!authentications[0].is_success);
    assert!(authentications[0].has_peer_address);
    assert!(auditor.events(ActionKind::DatabasesAll).is_empty());

    let response = server.http("POST", &format!("/v1/databases/{database}"), token, None).await;
    assert_eq!(response.status, 200);
    let created = auditor.await_last(ActionKind::DatabasesCreate, 1).await;
    assert!(created.is_success);
    assert_eq!(created.accessor, admin);
    assert_eq!(created.database_name.as_deref(), Some(database));
    assert!(created.has_peer_address);

    let response = server.http("POST", "/v1/users/bob", token, Some(serde_json::json!({ "password": "secret" }))).await;
    assert_eq!(response.status, 200);
    let user_created = auditor.await_last(ActionKind::UsersCreate, 1).await;
    assert!(user_created.is_success);
    assert_eq!(user_created.accessor, admin);
    assert_eq!(user_created.target_username.as_deref(), Some("bob"));

    let define = "define entity person; entity keyed, owns tag @key; attribute tag, value string;";
    let response = server.http("POST", "/v1/query", token, Some(oneshot_query(database, "schema", define))).await;
    assert_eq!(response.status, 200, "{}", response.body);
    let defined = auditor.await_last(ActionKind::OneshotQuery, 1).await;
    assert!(defined.is_success);
    assert_eq!(defined.query.as_deref(), Some(define));
    assert_eq!(defined.query_type, Some(QueryType::Schema));
    assert_eq!(defined.database_name.as_deref(), Some(database));
    assert_eq!(defined.accessor, admin);

    let invalid = "match $x isa unknown-type;";
    let response = server.http("POST", "/v1/query", token, Some(oneshot_query(database, "read", invalid))).await;
    assert_ne!(response.status, 200);
    let failed_query = auditor.await_last(ActionKind::OneshotQuery, 2).await;
    assert!(!failed_query.is_success);
    assert!(failed_query.error_code.is_some());
    assert_eq!(failed_query.query.as_deref(), Some(invalid));
    assert_eq!(failed_query.query_type, Some(QueryType::Read));
    assert!(failed_query.has_started);

    let unknown_database = oneshot_query("no-such-database", "read", invalid);
    let response = server.http("POST", "/v1/query", token, Some(unknown_database)).await;
    assert_ne!(response.status, 200);
    let failed_query = auditor.await_last(ActionKind::OneshotQuery, 3).await;
    assert!(!failed_query.is_success);
    assert_eq!(failed_query.query.as_deref(), Some(invalid));
    assert_eq!(failed_query.query_type, None);
    assert!(!failed_query.has_started);
    assert_eq!(failed_query.database_name.as_deref(), Some("no-such-database"));
    assert!(auditor.events(ActionKind::TransactionOpen).is_empty());
    assert!(auditor.events(ActionKind::TransactionCommit).is_empty());

    let open = |type_: &str| serde_json::json!({ "databaseName": database, "transactionType": type_ });
    let response = server.http("POST", "/v1/transactions/open", token, Some(open("write"))).await;
    assert_eq!(response.status, 200, "{}", response.body);
    let transaction_id = response.field("transactionId");
    let opened = auditor.await_last(ActionKind::TransactionOpen, 1).await;
    assert!(opened.is_success);
    assert_eq!(opened.transaction_id.map(|id| id.to_string()), Some(transaction_id.clone()));
    assert_eq!(opened.database_name.as_deref(), Some(database));

    let insert = "insert $p isa person;";
    let query_path = format!("/v1/transactions/{transaction_id}/query");
    let response = server.http("POST", &query_path, token, Some(serde_json::json!({ "query": insert }))).await;
    assert_eq!(response.status, 200, "{}", response.body);
    let inserted = auditor.await_last(ActionKind::TransactionQuery, 1).await;
    assert!(inserted.is_success);
    assert_eq!(inserted.query.as_deref(), Some(insert));
    assert_eq!(inserted.query_type, Some(QueryType::Write));
    assert_eq!(inserted.transaction_id, opened.transaction_id);
    assert_eq!(inserted.database_name.as_deref(), Some(database));

    let commit_path = format!("/v1/transactions/{transaction_id}/commit");
    let response = server.http("POST", &commit_path, token, None).await;
    assert_eq!(response.status, 200, "{}", response.body);
    let committed = auditor.await_last(ActionKind::TransactionCommit, 1).await;
    assert!(committed.is_success);
    assert_eq!(committed.transaction_id, opened.transaction_id);

    let response = server.http("POST", &commit_path, token, None).await;
    assert_ne!(response.status, 200);
    let failed_commit = auditor.await_last(ActionKind::TransactionCommit, 2).await;
    assert!(!failed_commit.is_success);
    assert_eq!(failed_commit.transaction_id, opened.transaction_id);

    let response = server.http("POST", "/v1/transactions/open", token, Some(open("read"))).await;
    let transaction_id = response.field("transactionId");
    let opened = auditor.await_last(ActionKind::TransactionOpen, 2).await;
    let query_path = format!("/v1/transactions/{transaction_id}/query");
    let response = server.http("POST", &query_path, token, Some(serde_json::json!({ "query": invalid }))).await;
    assert_ne!(response.status, 200);
    let failed_query = auditor.await_last(ActionKind::TransactionQuery, 2).await;
    assert!(!failed_query.is_success);
    assert!(failed_query.error_code.is_some());
    assert_eq!(failed_query.query.as_deref(), Some(invalid));
    assert_eq!(failed_query.transaction_id, opened.transaction_id);

    let response = server.http("POST", &format!("/v1/transactions/{transaction_id}/close"), token, None).await;
    assert_eq!(response.status, 200);
    let closed = auditor.await_last(ActionKind::TransactionClose, 1).await;
    assert!(closed.is_success);
    assert_eq!(closed.transaction_id, opened.transaction_id);
    assert_eq!(closed.accessor, admin);
    assert_eq!(closed.database_name.as_deref(), Some(database));
    assert_eq!(closed.forced_by, None);

    let response = server.http("POST", "/v1/transactions/open", token, Some(open("write"))).await;
    let transaction_id = response.field("transactionId");
    let opened = auditor.await_last(ActionKind::TransactionOpen, 3).await;
    let query_path = format!("/v1/transactions/{transaction_id}/query");
    let keyless = serde_json::json!({ "query": "insert $k isa keyed;" });
    let response = server.http("POST", &query_path, token, Some(keyless)).await;
    assert_eq!(response.status, 200, "{}", response.body);
    let response = server.http("POST", &format!("/v1/transactions/{transaction_id}/commit"), token, None).await;
    assert_ne!(response.status, 200);
    let failed_commit = auditor.await_last(ActionKind::TransactionCommit, 3).await;
    assert!(!failed_commit.is_success);
    assert!(failed_commit.has_started);
    let closed = auditor.await_close(opened.transaction_id).await;
    assert!(closed.is_success);
    assert!(closed.has_started);
    assert_eq!(closed.forced_by, Some(AuditReason::RequestFailure));

    let unknown_id = Uuid::new_v4();
    let response = server.http("POST", &format!("/v1/transactions/{unknown_id}/rollback"), token, None).await;
    assert_ne!(response.status, 200);
    let failed_rollback = auditor.await_last(ActionKind::TransactionRollback, 1).await;
    assert!(!failed_rollback.is_success);
    assert!(!failed_rollback.has_started);
    assert_eq!(failed_rollback.transaction_id, Some(unknown_id));

    assert_eq!(auditor.events(ActionKind::TransactionClose).len(), 2);
    assert!(auditor.events(ActionKind::Authenticate).iter().skip(1).all(|event| event.is_success));
    auditor.assert_consistent();
    server.stop().await;
    auditor.assert_all_ended();
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_actions_are_audited() {
    let server = AuditedServer::start().await;
    let auditor = server.auditor.clone();
    let database = "audit_grpc";
    let admin = Some(DEFAULT_USER_NAME.to_owned());
    let mut client = server.grpc_client().await;

    client.connection_open(connection_open_req(WRONG_PASSWORD)).await.expect_err("Expected a failed sign-in");
    let failed_open = auditor.await_last(ActionKind::ConnectionOpen, 1).await;
    assert!(!failed_open.is_success);
    assert!(failed_open.error_code.is_some());
    assert_eq!(failed_open.endpoint, AuditEndpoint::Grpc);
    assert!(failed_open.has_peer_address);
    assert_eq!(failed_open.target_username, admin);

    let response = client.connection_open(connection_open_req(DEFAULT_USER_PASSWORD)).await.unwrap().into_inner();
    let token = response.authentication.expect("Expected a token").token;
    let token = token.as_str();
    assert!(auditor.await_last(ActionKind::ConnectionOpen, 2).await.is_success);

    client.authentication_token_create(password_credentials(WRONG_PASSWORD)).await.expect_err("Expected a failure");
    let failed_signin = auditor.await_last(ActionKind::SignIn, 1).await;
    assert!(!failed_signin.is_success);
    assert_eq!(failed_signin.target_username, admin);

    client
        .databases_all(authenticated(typedb_protocol::database_manager::all::Req {}, "not-a-token"))
        .await
        .expect_err("Expected a failed authentication");
    let authentications = auditor.await_events(ActionKind::Authenticate, 1).await;
    assert!(!authentications[0].is_success);
    assert!(authentications[0].has_peer_address);
    assert!(auditor.events(ActionKind::DatabasesAll).is_empty());

    let create = typedb_protocol::database_manager::create::Req { name: database.to_owned() };
    client.databases_create(authenticated(create, token)).await.unwrap();
    let created = auditor.await_last(ActionKind::DatabasesCreate, 1).await;
    assert!(created.is_success);
    assert_eq!(created.accessor, admin);
    assert_eq!(created.database_name.as_deref(), Some(database));
    assert!(created.has_peer_address);

    let delete = typedb_protocol::database::delete::Req { name: "no-such-database".to_owned() };
    client.database_delete(authenticated(delete, token)).await.expect_err("Expected an unknown database failure");
    let failed_delete = auditor.await_last(ActionKind::DatabaseDelete, 1).await;
    assert!(!failed_delete.is_success);
    assert!(failed_delete.error_code.is_some());
    assert_eq!(failed_delete.database_name.as_deref(), Some("no-such-database"));

    let schema = typedb_protocol::transaction::Type::Schema as i32;
    let transaction = GrpcTransaction::open(&mut client, token, database, schema).await;
    let opened = auditor.await_last(ActionKind::TransactionOpen, 1).await;
    assert!(opened.is_success);
    assert_eq!(opened.accessor, admin);
    assert_eq!(opened.database_name.as_deref(), Some(database));
    assert!(opened.transaction_id.is_some());
    assert!(opened.has_peer_address);

    let define = "define entity person, owns code; attribute code, value string @regex(\"^[a-z]+$\");";
    transaction.query(define).await;
    let defined = auditor.await_last(ActionKind::TransactionQuery, 1).await;
    assert!(defined.is_success);
    assert_eq!(defined.query.as_deref(), Some(define));
    assert_eq!(defined.query_type, Some(QueryType::Schema));
    assert_eq!(defined.transaction_id, opened.transaction_id);
    assert_eq!(defined.database_name.as_deref(), Some(database));
    assert_eq!(defined.accessor, admin);

    transaction.commit().await;
    let committed = auditor.await_last(ActionKind::TransactionCommit, 1).await;
    assert!(committed.is_success);
    assert_eq!(committed.transaction_id, opened.transaction_id);
    drop(transaction);

    let write = typedb_protocol::transaction::Type::Write as i32;
    let transaction = GrpcTransaction::open(&mut client, token, database, write).await;
    let opened = auditor.await_last(ActionKind::TransactionOpen, 2).await;
    assert_ne!(opened.transaction_id, committed.transaction_id);

    let insert = "insert $p isa person;";
    transaction.query(insert).await;
    let inserted = auditor.await_last(ActionKind::TransactionQuery, 2).await;
    assert!(inserted.is_success);
    assert!(inserted.has_started);
    assert_eq!(inserted.query.as_deref(), Some(insert));
    assert_eq!(inserted.query_type, Some(QueryType::Write));
    assert_eq!(inserted.transaction_id, opened.transaction_id);

    let select = "match $p isa person;";
    transaction.query(select).await;
    let selected = auditor.await_last(ActionKind::TransactionQuery, 3).await;
    assert!(selected.is_success);
    assert_eq!(selected.query.as_deref(), Some(select));
    assert_eq!(selected.query_type, Some(QueryType::Read));

    let invalid = "match $x isa unknown-type;";
    transaction.query(invalid).await;
    let failed_query = auditor.await_last(ActionKind::TransactionQuery, 4).await;
    assert!(!failed_query.is_success);
    assert!(failed_query.error_code.is_some());
    assert_eq!(failed_query.query.as_deref(), Some(invalid));
    assert_eq!(failed_query.transaction_id, opened.transaction_id);

    let unparsable = "this is not a query";
    transaction.query(unparsable).await;
    let failed_query = auditor.await_last(ActionKind::TransactionQuery, 5).await;
    assert!(!failed_query.is_success);
    assert_eq!(failed_query.query.as_deref(), Some(unparsable));
    assert_eq!(failed_query.query_type, None);
    assert!(!failed_query.has_started);

    assert!(auditor.events(ActionKind::TransactionClose).is_empty());
    drop(transaction);
    let closed = auditor.await_last(ActionKind::TransactionClose, 1).await;
    assert!(closed.is_success);
    assert_eq!(closed.transaction_id, opened.transaction_id);
    assert_eq!(closed.accessor, admin);
    assert_eq!(closed.database_name.as_deref(), Some(database));
    assert_eq!(closed.forced_by, Some(AuditReason::ConnectionEnd));

    let transaction = GrpcTransaction::open(&mut client, token, database, write).await;
    let pipelined = auditor.await_last(ActionKind::TransactionOpen, 3).await;
    for _ in 0..PIPELINED_QUERY_COUNT {
        transaction.query(insert).await;
    }
    transaction.commit().await;
    let committed = auditor.await_last(ActionKind::TransactionCommit, 2).await;
    assert!(committed.is_success);
    assert_eq!(committed.database_name.as_deref(), Some(database));
    let queries = auditor.await_events(ActionKind::TransactionQuery, 5 + PIPELINED_QUERY_COUNT).await;
    for query in &queries[5..] {
        assert!(query.is_success, "{query:?}");
        assert_eq!(query.database_name.as_deref(), Some(database), "{query:?}");
        assert_eq!(query.transaction_id, pipelined.transaction_id, "{query:?}");
    }
    drop(transaction);

    let transaction = GrpcTransaction::open(&mut client, token, "no-such-database", write).await;
    let failed_open = auditor.await_last(ActionKind::TransactionOpen, 4).await;
    assert!(!failed_open.is_success);
    assert_eq!(failed_open.database_name.as_deref(), Some("no-such-database"));
    drop(transaction);

    let transaction = GrpcTransaction::open(&mut client, token, database, write).await;
    auditor.await_last(ActionKind::TransactionOpen, 5).await;
    let invalid_insert = "insert $p isa person, has code \"NOT-LOWERCASE\";";
    transaction
        .send(vec![
            GrpcTransaction::query_req(insert),
            GrpcTransaction::query_req(invalid_insert),
            GrpcTransaction::query_req(insert),
            GrpcTransaction::query_req(insert),
            GrpcTransaction::commit_req(),
        ])
        .await;
    let failed_commit = auditor.await_last(ActionKind::TransactionCommit, 3).await;
    assert!(!failed_commit.is_success, "{failed_commit:?}");
    let queries = auditor.await_events(ActionKind::TransactionQuery, 9 + PIPELINED_QUERY_COUNT).await;
    let queued = &queries[queries.len() - 4..];
    assert!(queued[0].is_success, "{queued:#?}");
    assert_eq!(queued[1].query.as_deref(), Some(invalid_insert));
    assert!(queued[1..].iter().all(|query| !query.is_success && !query.is_abandoned), "{queued:#?}");
    let closed = auditor.await_close(failed_commit.transaction_id).await;
    assert_eq!(closed.forced_by, Some(AuditReason::RequestFailure));
    drop(transaction);

    let read = typedb_protocol::transaction::Type::Read as i32;
    let transaction = GrpcTransaction::open(&mut client, token, database, read).await;
    let opened = auditor.await_last(ActionKind::TransactionOpen, 6).await;
    transaction.close().await;
    let closed = auditor.await_close(opened.transaction_id).await;
    assert!(closed.is_success);
    assert!(closed.has_started);
    assert_eq!(closed.accessor, admin);
    assert_eq!(closed.forced_by, None);
    drop(transaction);

    let transaction = GrpcTransaction::open(&mut client, token, database, read).await;
    let left_open = auditor.await_last(ActionKind::TransactionOpen, 7).await;

    assert!(auditor.events(ActionKind::Authenticate).iter().skip(1).all(|event| event.is_success));
    assert!(auditor.started(ActionKind::TransactionClose).is_empty());
    auditor.assert_consistent();
    server.stop().await;
    let closed = auditor.await_close(left_open.transaction_id).await;
    assert!(closed.is_success);
    assert_eq!(closed.forced_by, Some(AuditReason::Shutdown));
    assert!(auditor.closes(pipelined.transaction_id).is_empty());
    auditor.assert_consistent();
    auditor.assert_all_ended();
    drop(transaction);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_take_effect_only_once_their_start_is_recorded() {
    let server = AuditedServer::start().await;
    let auditor = server.auditor.clone();
    let database = "audit_start";
    let token = server.sign_in().await;
    let token = Some(token.as_str());
    server.create_database_of_people(token, database).await;

    let insert = "insert $p isa person;";
    let mut held_start = auditor.hold_start_of(insert);
    let inserting = server.send_http("POST", "/v1/query", token, Some(oneshot_query(database, "write", insert))).await;
    held_start.reached().await;
    assert_eq!(server.count_people(token, database).await, 0);
    let is_answered = tokio::time::timeout(HELD_CHECK, inserting.readable()).await.is_ok();
    assert!(!is_answered, "Expected the insert to wait for its start to be recorded");
    held_start.release();
    let response = HttpResponse::read(inserting).await;
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(server.count_people(token, database).await, 1);

    let queries = auditor.await_events(ActionKind::OneshotQuery, 4).await;
    let inserted = queries.iter().find(|query| query.query.as_deref() == Some(insert)).expect("Expected the insert");
    assert!(inserted.is_success);
    assert_eq!(inserted.query_type, Some(QueryType::Write));
    auditor.assert_consistent();
    server.stop().await;
    auditor.assert_all_ended();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_of_requests_that_have_ended_do_not_run() {
    let server = AuditedServer::start().await;
    let auditor = server.auditor.clone();
    let database = "audit_ended";
    let token = server.sign_in().await;
    let token = Some(token.as_str());
    server.create_database_of_people(token, database).await;
    let transaction_id = server.open_write_transaction(token, database).await;
    let query_path = format!("/v1/transactions/{transaction_id}/query");

    let held = "insert $p isa person;";
    let abandoned = "insert $q isa person;";
    let mut held_start = auditor.hold_start_of(held);
    let running_held = server.send_http("POST", &query_path, token, Some(serde_json::json!({ "query": held }))).await;
    held_start.reached().await;
    let request = server.send_http("POST", &query_path, token, Some(serde_json::json!({ "query": abandoned }))).await;
    tokio::time::sleep(QUEUEING_TIME).await;
    drop(request);
    let abandoned_query = auditor.await_query(abandoned).await;
    held_start.release();
    let response = HttpResponse::read(running_held).await;
    assert_eq!(response.status, 200, "{}", response.body);
    assert!(abandoned_query.is_abandoned, "{abandoned_query:?}");
    assert!(!abandoned_query.has_started, "{abandoned_query:?}");

    server.commit(token, &transaction_id).await;
    assert_eq!(server.count_people(token, database).await, 1);
    auditor.assert_consistent_but_abandoned(&[abandoned]);
    server.stop().await;
    auditor.assert_all_ended();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_whose_request_ends_while_their_start_is_recorded_do_not_run() {
    let server = AuditedServer::start().await;
    let auditor = server.auditor.clone();
    let database = "audit_ended_while_starting";
    let token = server.sign_in().await;
    let token = Some(token.as_str());
    server.create_database_of_people(token, database).await;
    let transaction_id = server.open_write_transaction(token, database).await;
    let query_path = format!("/v1/transactions/{transaction_id}/query");

    let abandoned = "insert $p isa person;";
    let mut held_start = auditor.hold_start_of(abandoned);
    let request = server.send_http("POST", &query_path, token, Some(serde_json::json!({ "query": abandoned }))).await;
    held_start.reached().await;
    drop(request);
    let abandoned_query = auditor.await_query(abandoned).await;
    held_start.release();
    assert!(abandoned_query.is_abandoned, "{abandoned_query:?}");
    assert!(abandoned_query.has_started, "{abandoned_query:?}");

    server.commit(token, &transaction_id).await;
    assert_eq!(server.count_people(token, database).await, 0);
    auditor.assert_consistent_but_abandoned(&[abandoned]);
    server.stop().await;
    auditor.assert_all_ended();
}
