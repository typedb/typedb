/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::future::Future;
use std::ops::ControlFlow;
use std::ops::ControlFlow::{Break, Continue};
use std::sync::Arc;

use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::{Code, Status, Streaming};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::{event, Level};
use typedb_protocol::Server;
use typedb_protocol::transaction::{Client, Req, Type};

use bytes::util::HexBytesFormatter;
use database::database_manager::DatabaseManager;
use database::transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
use error::typedb_error;
use resource::constants::server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use storage::durability_client::WALClient;
use storage::snapshot::ReadableSnapshot;

use crate::service::error::ProtocolError;

// TODO: where does this belong?
#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

impl Transaction {
    pub fn readable_snapshot(&self) -> &dyn ReadableSnapshot {
        match self {
            Transaction::Read(transaction) => &transaction.snapshot,
            Transaction::Write(transaction) => &transaction.snapshot,
            Transaction::Schema(transaction) => &transaction.snapshot
        }
    }
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
                    event!(Level::DEBUG, "GRPC error", ?error);
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
                                }.into()).await;
                                close_service!()
                            }
                            Some(req) => {
                                let result = self.handle_request(&request_id, req);
                                if let Some(err) = result {
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

    fn handle_request(&mut self, request_id: &[u8], req: typedb_protocol::transaction::req::Req) -> Result<ControlFlow<(), ()>, Status> {
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
                return Err(ProtocolError::TransactionAlreadyOpen {}.into());
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
                self.handle_commit(commit_req)?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {
                self.handle_rollback(rollback_req)?;
                Ok(Continue(()))
            }
            (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                self.handle_close(close_req)?;
                Ok(Break(()))
            }
            (false, _) => return Err(ProtocolError::TransactionClosed {}.into()),
        }
    }

    fn handle_open(&mut self, open_req: typedb_protocol::transaction::open::Req) -> Result<(), Status> {
        self.network_latency_millis = Some(open_req.network_latency_millis);
        if let Some(options) = open_req.options {
            self.prefetch_size = options.prefetch_size.or(Some(DEFAULT_PREFETCH_SIZE));
            self.transaction_timeout_millis = options.transaction_timeout_millis.or(Some(DEFAULT_TRANSACTION_TIMEOUT_MILLIS));
        }

        let transaction_type = typedb_protocol::transaction::Type::try_from(open_req.r#type)
            .map_err(|err| ProtocolError::UnrecognisedTransactionType { enum_variant: open_req.r#type }.into())?;

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

    fn handle_commit(&mut self, commit_req: typedb_protocol::transaction::commit::Req) -> Result<(), Status> {
        let result = match self.transaction.take().unwrap() {
            Transaction::Read(_) => Err(TransactionServiceError::CannotCommitReadTransaction {}.into_status()),
            Transaction::Write(transaction) => {
                transaction.commit()
                    .map_err(|err| TransactionServiceError::DataCommitFailed { source: err }.into())
            }
            Transaction::Schema(transaction) => {
                transaction.commit()
                    .map_err(|err| TransactionServiceError::SchemaCommitFailed { source: err }.into())
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

        let transaction = self.transaction.as_ref().unwrap();
        // transaction.readable_snapshot();
        todo!()
    }

    async fn send_err(&mut self, err: Status) {
        let result = self.response_sender.send(
            Err(err)
        ).await;
        if let Err(send_error) = result {
            event!(Level::DEBUG, "Failed to send error to client", ?send_error);
        }
    }
}

typedb_error!(
    pub(crate) TransactionServiceError(domain = "Service", prefix = "TSV") {
        DatabaseNotFound(1, "Database '{name}' not found.", name = String),
        CannotCommitReadTransaction(2, "Read transactions cannot be committed."),
        CannotRollbackReadTransaction(3, "Read transactions cannot be rolled back, since they never contain writes."),
        DataCommitFailed(4, "Data transaction commit failed.", source = DataCommitError), // TODO: these should be typedb_source
        SchemaCommitFailed(5, "Schema transaction commit failed.", source = SchemaCommitError),
    }
);
