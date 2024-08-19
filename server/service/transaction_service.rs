/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::future::Future;
use std::ops::ControlFlow;
use std::ops::ControlFlow::{Break, Continue};
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::task::spawn_blocking;
use tokio_stream::StreamExt;
use tonic::{Code, Status, Streaming};
use tonic_types::{FieldViolation, StatusExt};
use tracing::{event, Level};
use typedb_protocol::transaction::{Client, Req, Type};
use typeql::{parse_query, Query};
use typeql::query::{Pipeline, SchemaQuery};
use typeql::query::stage::Stage;

use bytes::util::HexBytesFormatter;
use database::database_manager::DatabaseManager;
use database::transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
use error::typedb_error;
use query::error::QueryError;
use query::query_manager::QueryManager;
use resource::constants::server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use storage::durability_client::WALClient;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

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

    transaction_timeout_millis: Option<u64>,
    prefetch_size: Option<u64>,
    network_latency_millis: Option<u64>,

    transaction: Option<Transaction>,
}

macro_rules! close_service {
    () => {return;};
}

impl TransactionService {
    pub(crate) fn new(
        request_stream: Streaming<Client>,
        response_sender: Sender<Result<typedb_protocol::transaction::Server, Status>>,
        database_manager: Arc<DatabaseManager>,
    ) -> Self {
        Self {
            database_manager,

            request_stream,
            response_sender,

            transaction_timeout_millis: None,
            prefetch_size: None,
            network_latency_millis: None,

            transaction: None,
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
                        let request_id = request.req_id;
                        let metadata = request.metadata;
                        match request.req {
                            None => {
                                self.send_err(ProtocolError::MissingField {
                                    name: "req",
                                    description: "Transaction message must contain a request.",
                                }.into_status()).await;
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
                                //

                                let result = self.handle_request(&request_id, req).await;
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
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_request(&mut self, request_id: &[u8], req: typedb_protocol::transaction::req::Req) -> Result<ControlFlow<(), ()>, Status> {
        match (self.transaction.is_some(), req) {
            (false, typedb_protocol::transaction::req::Req::OpenReq(open_req)) => {
                match self.handle_open(open_req) {
                    Ok(_) => {
                        event!(Level::TRACE, "Transaction opened, request ID: {:?}", HexBytesFormatter(request_id));
                        Ok(Continue(()))
                    }
                    Err(status) => return Err(status),
                }
            }
            (true, typedb_protocol::transaction::req::Req::OpenReq(_)) => {
                return Err(ProtocolError::TransactionAlreadyOpen {}.into_status());
            }
            (true, typedb_protocol::transaction::req::Req::QueryReq(query_req)) => {
                self.handle_query(query_req)?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::StreamReq(stream_req)) => {
                // TODO: get iterator from map of request id -> iterator, and issue next set of responses.
                // todo!()
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::CommitReq(commit_req)) => {
                self.handle_commit(commit_req).await?;
                Ok(Break(()))
            }
            (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {
                self.handle_rollback(rollback_req)?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                self.handle_close(close_req)?;
                Ok(Break(()))
            }
            (false, _) => return Err(ProtocolError::TransactionClosed {}.into_status()),
        }
    }

    fn handle_open(&mut self, open_req: typedb_protocol::transaction::open::Req) -> Result<(), Status> {
        self.network_latency_millis = Some(open_req.network_latency_millis);
        if let Some(options) = open_req.options {
            self.prefetch_size = options.prefetch_size.or(Some(DEFAULT_PREFETCH_SIZE));
            self.transaction_timeout_millis = options.transaction_timeout_millis.or(Some(DEFAULT_TRANSACTION_TIMEOUT_MILLIS));
        }

        let transaction_type = typedb_protocol::transaction::Type::try_from(open_req.r#type)
            .map_err(|err| ProtocolError::UnrecognisedTransactionType { enum_variant: open_req.r#type }.into_status())?;

        let database_name = open_req.database;
        let database = self.database_manager.database(database_name.as_ref())
            .ok_or_else(|| TransactionServiceError::DatabaseNotFound { name: database_name }.into_status())?;

        let transaction = match transaction_type {
            Type::Read => Transaction::Read(TransactionRead::open(database)),
            Type::Write => Transaction::Write(TransactionWrite::open(database)),
            Type::Schema => Transaction::Schema(TransactionSchema::open(database)),
        };
        self.transaction = Some(transaction);
        Ok(())
    }

    async fn handle_commit(&mut self, commit_req: typedb_protocol::transaction::commit::Req) -> Result<(), Status> {
        // TODO: take mut
        let result = match self.transaction.take().unwrap() {
            Transaction::Read(_) => Err(TransactionServiceError::CannotCommitReadTransaction {}.into_status()),
            Transaction::Write(transaction) => {
                tokio::task::spawn_blocking(move || {
                    transaction.commit()
                        .map_err(|err| TransactionServiceError::DataCommitFailed { source: err }.into_status())
                }).await.unwrap()
            }
            Transaction::Schema(transaction) => {
                transaction.commit()
                    .map_err(|err| TransactionServiceError::SchemaCommitFailed { source: err }.into_status())
            }
        };
        result
    }

    fn handle_rollback(&mut self, rollback_req: typedb_protocol::transaction::rollback::Req) -> Result<(), Status> {
        match self.transaction.take().unwrap() {
            Transaction::Read(_) => return Err(TransactionServiceError::CannotRollbackReadTransaction {}.into_status()),
            Transaction::Write(mut transaction) => transaction.rollback(),
            Transaction::Schema(mut transaction) => transaction.rollback(),
        };
        Ok(())
    }

    fn handle_close(&mut self, close_req: typedb_protocol::transaction::close::Req) -> Result<(), Status> {
        match self.transaction.take().unwrap() {
            Transaction::Read(transaction) => transaction.close(),
            Transaction::Write(transaction) => transaction.close(),
            Transaction::Schema(transaction) => transaction.close(),
        };
        Ok(())
    }

    fn handle_query(&mut self, query_req: typedb_protocol::query::Req) -> Result<(), Status> {
        // TODO: compile query, create executor, respond with initial message and then await initial answers to send
        let query_string = query_req.query;
        let query_options = query_req.options;
        let parsed = parse_query(&query_string)
            .map_err(|err| TransactionServiceError::QueryParseFailed { source: err }.into_status())?;
        match parsed {
            Query::Schema(schema_query) => return self.handle_query_schema(schema_query),
            Query::Pipeline(pipeline) => {
                let is_write = pipeline.stages.iter().any(Self::is_write_stage);
                if is_write {

                } else {

                }
            }
        }

        let transaction = self.transaction.as_ref().unwrap();
        // transaction.readable_snapshot();
        todo!()
    }

    fn handle_query_schema(&mut self, query: SchemaQuery) -> Result<(), Status> {
        if let Some(Transaction::Schema(schema_transaction)) = self.transaction.take() {
            let TransactionSchema {
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                _schema_txn_guard,
                database
            } = schema_transaction;
            let mut snapshot = Arc::into_inner(snapshot).unwrap();
            QueryManager::new().execute_schema(
                &mut snapshot,
                &type_manager,
               query
            ).map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status())?;
            let transaction = TransactionSchema::from(
                snapshot, type_manager, thing_manager, function_manager, _schema_txn_guard, database
            );
            self.transaction = Some(Transaction::Schema(transaction));
            return Ok(());
        } else {
            return Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_status());
        }
    }

    // TODO move to QueryManager
    fn execute_write_pipeline(mut snapshot: impl WritableSnapshot, pipeline: Pipeline) {

    }

    fn is_write_stage(stage: &Stage) -> bool {
        match stage {
            Stage::Insert(_)
            | Stage::Put(_)
            | Stage::Delete(_)
            | Stage::Update(_) => true,
            Stage::Fetch(_)
            | Stage::Reduce(_)
            | Stage::Modifier(_)
            | Stage::Match(_) => false,
        }
    }

    async fn send_err(&mut self, err: Status) {
        let result = self.response_sender.send(
            Err(err)
        ).await;
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
        QueryParseFailed(6, "Query parsing failed.", ( source : typeql::common::error::Error )),
        SchemaQueryRequiresSchemaTransaction(7, "Schema modification queries require schema transactions."),
        QueryExecutionFailed(8, "Query execution failed.", ( source : QueryError )),
    }
);
