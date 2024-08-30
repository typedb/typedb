/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
};

use database::{
    database_manager::DatabaseManager,
    transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite},
};
use error::typedb_error;
use options::TransactionOptions;
use query::{error::QueryError, query_manager::QueryManager};
use resource::constants::server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use storage::{durability_client::WALClient, snapshot::WritableSnapshot};
use tokio::{
    sync::{broadcast, mpsc::Sender},
    task::{spawn_blocking, JoinHandle},
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::transaction::{Client, Type};
use typeql::{
    parse_query,
    query::{stage::Stage, Pipeline, SchemaQuery},
    Query,
};
use uuid::Uuid;

use crate::service::error::{ProtocolError, StatusConvertible};

// TODO: where does this belong?
#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

macro_rules! with_readable_snapshot {
    ($tx: ident, |$snapshot: ident| $expr:expr) => {
        match $tx {
            Transaction::Read(transaction) => {
                let $snapshot = transaction.snapshot;
                $expr
            }
            Transaction::Write(transaction) => {
                let $snapshot = transaction.snapshot;
                $expr
            }
            Transaction::Schema(transaction) => {
                let $snapshot = transaction.snapshot;
                $expr
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct TransactionService {
    database_manager: Arc<DatabaseManager>,

    request_stream: Streaming<Client>,
    response_sender: Sender<Result<typedb_protocol::transaction::Server, Status>>,
    read_responder_interrupt_sender: broadcast::Sender<()>,
    read_responder_interrupt_receiver: broadcast::Receiver<()>,
    write_responder_interrupt_sender: broadcast::Sender<()>,
    write_responder_interrupt_receiver: broadcast::Receiver<()>,

    transaction_timeout_millis: Option<u64>,
    schema_lock_acquire_timeout_millis: Option<u64>,
    prefetch_size: Option<u64>,
    network_latency_millis: Option<u64>,

    is_open: bool,
    transaction: Option<Transaction>,
    request_queue: Vec<typedb_protocol::query::Req>,
    read_responders: HashMap<Uuid, Responder>,
    write_responders: HashMap<Uuid, Responder>,
    active_write_query: Option<(Uuid, JoinHandle<()>)>,
}

#[derive(Debug)]
enum Responder {
    Active(JoinHandle<()>),
    // a read query computing+responding, or a write query responding
    Waiting(()),
}

macro_rules! close_service {
    () => {
        return;
    };
}

impl TransactionService {
    pub(crate) fn new(
        request_stream: Streaming<Client>,
        response_sender: Sender<Result<typedb_protocol::transaction::Server, Status>>,
        database_manager: Arc<DatabaseManager>,
    ) -> Self {
        let (read_interrupt_sender, read_interrupt_receiver) = broadcast::channel(1);
        let (write_interrupt_sender, write_interrupt_receiver) = broadcast::channel(1);

        Self {
            database_manager,

            request_stream,
            response_sender,
            read_responder_interrupt_sender: read_interrupt_sender,
            read_responder_interrupt_receiver: read_interrupt_receiver,
            write_responder_interrupt_sender: write_interrupt_sender,
            write_responder_interrupt_receiver: write_interrupt_receiver,

            transaction_timeout_millis: None,
            schema_lock_acquire_timeout_millis: None,
            prefetch_size: None,
            network_latency_millis: None,

            is_open: false,
            transaction: None,
            request_queue: Vec::with_capacity(20),
            read_responders: HashMap::new(),
            write_responders: HashMap::new(),
            active_write_query: None,
        }
    }

    pub(crate) async fn listen(&mut self) {
        loop {
            let next = self.request_stream.next().await;
            match next {
                None => {
                    close_service!();
                }
                Some(Err(error)) => {
                    event!(Level::DEBUG, ?error, "GRPC error");
                    close_service!();
                }
                Some(Ok(message)) => {
                    for request in message.reqs {
                        let request_id = Uuid::from_slice(&request.req_id).unwrap();
                        let metadata = request.metadata;
                        match request.req {
                            None => {
                                self.send_err(
                                    ProtocolError::MissingField {
                                        name: "req",
                                        description: "Transaction message must contain a request.",
                                    }
                                    .into_status(),
                                )
                                .await;
                                close_service!();
                            }
                            Some(req) => {
                                // TODO:
                                //  If we get a Write query,
                                //      if the queue is non-empty, we queue request, and check if we need to execute from the queue
                                //      else if there's running read queries, we queue it only (SWAP WITH ABOVE?)
                                //      if there's no running read queries, we execute + await it immediately, and add the output iterator into the Iterators map
                                //  if we get a Read query
                                //      if the queue is non-empty, we queue the request, and check if we need to execute from the queue
                                //      else if there's running or no running read queries, we execute it and add it to the running read queries

                                let result = self.handle_request(request_id, req).await;
                                match result {
                                    Ok(Continue(())) => {}
                                    Ok(Break(())) => {
                                        close_service!();
                                    }
                                    Err(err) => {
                                        self.send_err(err).await;
                                        close_service!();
                                    }
                                }

                                // update state
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_request(
        &mut self,
        request_id: Uuid,
        req: typedb_protocol::transaction::req::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {
        match (self.is_open, req) {
            (false, typedb_protocol::transaction::req::Req::OpenReq(open_req)) => {
                // Eagerly executed in main loop
                match self.handle_open(open_req) {
                    Ok(_) => {
                        event!(Level::TRACE, "Transaction opened, request ID: {:?}", &request_id);
                        Ok(Continue(()))
                    }
                    Err(status) => Err(status),
                }
            }
            (true, typedb_protocol::transaction::req::Req::OpenReq(_)) => {
                Err(ProtocolError::TransactionAlreadyOpen {}.into_status())
            }
            (true, typedb_protocol::transaction::req::Req::QueryReq(query_req)) => {
                self.handle_query(query_req).await?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::StreamReq(stream_req)) => {
                self.handle_stream_continue(request_id, stream_req).await.map(|_| Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::CommitReq(commit_req)) => {
                // Eagerly executed in main loop
                self.handle_commit(commit_req).await?;
                Ok(Break(()))
            }
            (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {
                // Eagerly executed in main loop
                self.handle_rollback(rollback_req)?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                // Eagerly executed in main loop
                self.handle_close(close_req)?;
                Ok(Break(()))
            }
            (false, _) => Err(ProtocolError::TransactionClosed {}.into_status()),
        }
    }

    fn handle_open(&mut self, open_req: typedb_protocol::transaction::open::Req) -> Result<(), Status> {
        self.network_latency_millis = Some(open_req.network_latency_millis);
        let mut transaction_options = TransactionOptions::default();
        if let Some(options) = open_req.options {
            // transaction options
            options.parallel.map(|parallel| transaction_options.parallel = parallel);
            options
                .schema_lock_acquire_timeout_millis
                .map(|timeout| transaction_options.schema_lock_acquire_timeout_millis = timeout);

            // service options
            self.prefetch_size = options.prefetch_size.or(Some(DEFAULT_PREFETCH_SIZE));
            self.transaction_timeout_millis =
                options.transaction_timeout_millis.or(Some(DEFAULT_TRANSACTION_TIMEOUT_MILLIS));
        }

        let transaction_type = typedb_protocol::transaction::Type::try_from(open_req.r#type).map_err(|err| {
            ProtocolError::UnrecognisedTransactionType { enum_variant: open_req.r#type }.into_status()
        })?;

        let database_name = open_req.database;
        let database = self
            .database_manager
            .database(database_name.as_ref())
            .ok_or_else(|| TransactionServiceError::DatabaseNotFound { name: database_name }.into_status())?;

        let transaction = match transaction_type {
            Type::Read => Transaction::Read(TransactionRead::open(database, transaction_options)),
            Type::Write => Transaction::Write(TransactionWrite::open(database, transaction_options)),
            Type::Schema => Transaction::Schema(TransactionSchema::open(database, transaction_options)),
        };
        self.transaction = Some(transaction);
        self.is_open = true;
        Ok(())
    }

    async fn handle_commit(&mut self, commit_req: typedb_protocol::transaction::commit::Req) -> Result<(), Status> {
        // Interrupt all running read queries
        // Await all read query join handles, then clear all read responders
        // Await currently running write query, then clear all write responders
        // Drain queue: skip all reads, execute all writes
        // Execute commit

        // TODO: take mut

        match self.transaction.take().unwrap() {
            Transaction::Read(_) => Err(TransactionServiceError::CannotCommitReadTransaction {}.into_status()),
            Transaction::Write(transaction) => spawn_blocking(move || {
                transaction
                    .commit()
                    .map_err(|err| TransactionServiceError::DataCommitFailed { source: err }.into_status())
            })
            .await
            .unwrap(),
            Transaction::Schema(transaction) => transaction
                .commit()
                .map_err(|err| TransactionServiceError::SchemaCommitFailed { source: err }.into_status()),
        }
    }

    fn handle_rollback(&mut self, rollback_req: typedb_protocol::transaction::rollback::Req) -> Result<(), Status> {
        // Interrupt all running read queries and any running write query
        // Await all read query join handles, then clear all read responders
        // Await all running write responders, then clear all write responders
        // Clear queue
        // Execute rollback

        match self.transaction.take().unwrap() {
            Transaction::Read(_) => return Err(TransactionServiceError::CannotRollbackReadTransaction {}.into_status()),
            Transaction::Write(mut transaction) => transaction.rollback(),
            Transaction::Schema(mut transaction) => transaction.rollback(),
        };
        Ok(())
    }

    fn handle_close(&mut self, close_req: typedb_protocol::transaction::close::Req) -> Result<(), Status> {
        // Interrupt all running read queries and any running write query
        // Await all read query join handles, then clear all read responders
        // Await all running write responders, then clear all write responders
        // Clear queue
        // Execute close

        match self.transaction.take().unwrap() {
            Transaction::Read(transaction) => transaction.close(),
            Transaction::Write(transaction) => transaction.close(),
            Transaction::Schema(transaction) => transaction.close(),
        };
        Ok(())
    }

    async fn handle_query(&mut self, query_req: typedb_protocol::query::Req) -> Result<(), Status> {
        // TODO: compile query, create executor, respond with initial message and then await initial answers to send
        let query_string = &query_req.query;
        let query_options = &query_req.options; // TODO: pass query options
        let parsed = parse_query(query_string)
            .map_err(|err| TransactionServiceError::QueryParseFailed { source: err }.into_status())?;
        match parsed {
            Query::Schema(schema_query) => self.handle_query_schema(schema_query).await,
            Query::Pipeline(pipeline) => {
                let is_write = pipeline.stages.iter().any(Self::is_write_stage);
                if is_write {
                    if !self.request_queue.is_empty()
                        || !self.read_responders.is_empty()
                        || self.active_write_query.is_some()
                    {
                        self.request_queue.push(query_req);
                        Ok(())
                    } else {
                        self.execute_write_query(pipeline).await
                    }
                } else {
                    todo!()
                }
            }
        }
    }

    async fn handle_query_schema(&mut self, query: SchemaQuery) -> Result<(), Status> {
        if let Some(Transaction::Schema(schema_transaction)) = self.transaction.take() {
            let TransactionSchema {
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                transaction_options,
            } = schema_transaction;
            let mut snapshot = Arc::into_inner(snapshot).unwrap();
            let (snapshot, type_manager, thing_manager, result) = spawn_blocking(move || {
                let result = QueryManager::new()
                    .execute_schema(&mut snapshot, &type_manager, &thing_manager, query)
                    .map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status());
                (snapshot, type_manager, thing_manager, result)
            })
            .await
            .unwrap();
            result?;
            let transaction = TransactionSchema::from(
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                transaction_options,
            );
            self.transaction = Some(Transaction::Schema(transaction));
            Ok(())
        } else {
            Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_status())
        }
    }

    async fn execute_write_query(&mut self, pipeline: Pipeline) -> Result<(), Status> {
        debug_assert!(
            self.request_queue.is_empty()
                && self.read_responders.is_empty()
                && self.active_write_query.is_none()
                && self.transaction.is_some()
        );

        if let Some(Transaction::Schema(schema_transaction)) = self.transaction.take() {
            let (transaction, result) = spawn_blocking(move || {
                // TODO: pass in Interrupt Receiver
                let TransactionSchema {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                } = schema_transaction;
                let mut snapshot = Arc::into_inner(snapshot).unwrap();
                let result = Self::execute_write_pipeline(&mut snapshot, pipeline)
                    .map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status());
                let transaction = TransactionSchema::from(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                );
                (transaction, result)
            })
            .await
            .unwrap();
            result?;
            self.transaction = Some(Transaction::Schema(transaction));
            Ok(())
        } else if let Some(Transaction::Write(write_transaction)) = self.transaction.take() {
            let (transaction, result) = spawn_blocking(move || {
                // TODO: pass in Interrupt Receiver
                let TransactionWrite {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                } = write_transaction;
                let mut snapshot = Arc::into_inner(snapshot).unwrap();
                let result = Self::execute_write_pipeline(&mut snapshot, pipeline)
                    .map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status());
                let transaction = TransactionWrite::from(
                    Arc::new(snapshot),
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                );
                (transaction, result)
            })
            .await
            .unwrap();
            result?;
            self.transaction = Some(Transaction::Write(transaction));
            return Ok(());
        } else {
            return Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_status());
        }
    }

    // TODO move to QueryManager
    fn execute_write_pipeline(snapshot: &mut impl WritableSnapshot, pipeline: Pipeline) -> Result<(), QueryError> {
        todo!()
    }

    async fn handle_stream_continue(
        &mut self,
        request_id: Uuid,
        stream_req: typedb_protocol::transaction::stream::Req,
    ) -> Result<(), Status> {
        debug_assert!(
            self.read_responders.contains_key(&request_id) || self.write_responders.contains_key(&request_id)
        );
        let read_responder = self.read_responders.get_mut(&request_id);
        if let Some(responder) = read_responder {
            match responder {
                Responder::Active(handle) => {
                    if !handle.is_finished() {
                        event!(
                            Level::DEBUG,
                            "Responder is already active, and received a Stream Continue request.\
                                 Unexpected behaviour, but not an error state."
                        );
                    } else {
                        handle.await.unwrap();
                        // TODO: recreate a task to stream back out from the iterator and set the handle
                        // *handle = ..
                    }
                }
                Responder::Waiting(()) => {
                    todo!()
                }
            }
        } else {
            let write_responder = self.write_responders.get_mut(&request_id);
            if let Some(responder) = write_responder {
                match responder {
                    Responder::Active(handle) => {
                        if !handle.is_finished() {
                            event!(
                                Level::DEBUG,
                                "Responder is already active, and received a Stream Continue request.\
                                 Unexpected behaviour, but not an error state."
                            );
                        } else {
                            handle.await.unwrap();
                            // TODO: recreate a task to stream back out from the iterator and set the handle
                            // *handle = ..
                        }
                    }
                    Responder::Waiting(()) => {
                        todo!()
                    }
                }
            } else {
                return Err(ProtocolError::QueryStreamNotFound { query_request_id: request_id }.into_status());
            }
        }
        Ok(())
    }

    fn is_write_stage(stage: &Stage) -> bool {
        match stage {
            Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => true,
            Stage::Fetch(_) | Stage::Reduce(_) | Stage::Modifier(_) | Stage::Match(_) => false,
        }
    }

    async fn send_err(&mut self, err: Status) {
        let result = self.response_sender.send(Err(err)).await;
        if let Err(send_error) = result {
            event!(Level::DEBUG, ?send_error, "Failed to send error to client");
        }
    }
}

typedb_error!(
    pub(crate) TransactionServiceError(domain = "Service", prefix = "TSV") {
        DatabaseNotFound(1, "Database '{name}' not found.", name: String),
        CannotCommitReadTransaction(2, "Read transactions cannot be committed."),
        CannotRollbackReadTransaction(3, "Read transactions cannot be rolled back, since they never contain writes."),
        // TODO: these should be typedb_source
        DataCommitFailed(4, "Data transaction commit failed.", ( source : DataCommitError )),
        SchemaCommitFailed(5, "Schema transaction commit failed.", ( source : SchemaCommitError )),
        QueryParseFailed(6, "Query parsing failed.", ( source : typeql::Error )),
        SchemaQueryRequiresSchemaTransaction(7, "Schema modification queries require schema transactions."),
        WriteQueryRequiresSchemaOrWriteTransaction(8, "Data modification queries require either write or schema transactions."),
        QueryExecutionFailed(9, "Query execution failed.", ( source : QueryError )),
    }
);
