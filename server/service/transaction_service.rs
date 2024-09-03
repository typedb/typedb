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
use itertools::Itertools;

use tokio::{
    sync::{broadcast, mpsc::Sender},
    task::{JoinHandle, spawn_blocking},
};
use lending_iterator::LendingIterator;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::query::Res;
use typedb_protocol::transaction::{Client, Type};
use typeql::{
    parse_query,
    query::{Pipeline, SchemaQuery, stage::Stage},
    Query,
};
use uuid::Uuid;
use compiler::VariablePosition;

use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use database::{
    database_manager::DatabaseManager,
    transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite},
};
use error::typedb_error;
use executor::batch::Batch;
use executor::pipeline::{PipelineExecutionError, StageAPI, StageIterator};
use executor::pipeline::stage::{ReadPipelineStage, ReadStageIterator, WritePipelineStage, WriteStageIterator};
use executor::row::MaybeOwnedRow;
use function::function_manager::FunctionManager;
use options::TransactionOptions;
use query::{error::QueryError, query_manager::QueryManager};
use resource::constants::server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use storage::{durability_client::WALClient, snapshot::WritableSnapshot};
use storage::snapshot::{ReadableSnapshot, ReadSnapshot};

use crate::service::error::{ProtocolError, IntoProtocolErrorMessage, IntoGRPCStatus};

// TODO: where does this belong?
#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
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
    request_queue: Vec<(Uuid, Pipeline)>,
    read_responders: HashMap<Uuid, JoinHandle<()>>,
    write_responders: HashMap<Uuid, JoinHandle<()>>,
    active_write_query: Option<(Uuid, JoinHandle<(Transaction, Result<Batch, Status>)>)>,
}

macro_rules! close_service {
    () => {
        return;
    };

}

macro_rules! close_service_with_error {
    ($self: ident, $error: expr) => {
        let result = $self.response_sender.send(Err($error)).await;
        if let Err(send_error) = result {
            event!(Level::DEBUG, ?send_error, "Failed to send error to client");
        }
        return;
    }};
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
            let next = self.request_stream.next().await; // TODO: await active write query. If finished, move to responders and unblock other queries!
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
                                close_service_with_error!(
                                    self,
                                    ProtocolError::MissingField {
                                        name: "req",
                                        description: "Transaction message must contain a request.",
                                    }
                                        .into_status()
                                );
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
                                        close_service_with_error!(self,err);
                                    }
                                }
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
                // TODO: not every query error -> Status needs to abort the transaction!
                self.handle_query(request_id, query_req).await?;
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

    async fn handle_query(&mut self, req_id: Uuid, query_req: typedb_protocol::query::Req) -> Result<(), Status> {
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
                        self.request_queue.push((req_id, pipeline));
                        Ok(())
                    } else {
                        // TODO: inject interrupt signal
                        let blocking_execution = self.blocking_execute_write_query(pipeline)?;
                        self.active_write_query = Some((req_id, tokio::spawn(async move {
                            blocking_execution.await.unwrap()
                        })));
                        Ok(())
                    }
                } else {
                    if !self.request_queue.is_empty() || self.active_write_query.is_some() {
                        self.request_queue.push((req_id, pipeline));
                        Ok(())
                    } else {
                        self.blocking_execute_read_query(pipeline).await?;
                    }
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
            // TODO: this doesn't need to be fatal!
            Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_status())
        }
    }

    fn blocking_execute_write_query(&mut self, pipeline: Pipeline) -> Result<JoinHandle<(Transaction, Result<Batch, Status>)>, Status> {
        debug_assert!(
            self.request_queue.is_empty()
                && self.read_responders.is_empty()
                && self.active_write_query.is_none()
                && self.transaction.is_some()
        );
        if let Some(Transaction::Schema(schema_transaction)) = self.transaction.take() {
            Ok(spawn_blocking(move || {
                // TODO: pass in Interrupt Receiver
                let TransactionSchema {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                } = schema_transaction;

                let (snapshot, result) = Self::execute_write_query_in(
                    Arc::into_inner(snapshot).unwrap(), &type_manager, thing_manager.clone(), &function_manager, &pipeline,
                );

                let transaction = Transaction::Schema(TransactionSchema::from(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                ));
                let result = result.map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status());
                (transaction, result)
            }))
        } else if let Some(Transaction::Write(write_transaction)) = self.transaction.take() {
            Ok(spawn_blocking(move || {
                // TODO: pass in Interrupt Receiver
                let TransactionWrite {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                } = write_transaction;

                let (snapshot, result) = Self::execute_write_query_in(
                    Arc::into_inner(snapshot).unwrap(), &type_manager, thing_manager.clone(), &function_manager, &pipeline,
                );

                let transaction = Transaction::Write(TransactionWrite::from(
                    Arc::new(snapshot),
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                ));
                let result = result.map_err(|err| TransactionServiceError::QueryExecutionFailed { source: err }.into_status());
                (transaction, result)
            }))
        } else {
            return Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_status());
        }
    }

    fn execute_write_query_in<Snapshot: WritableSnapshot + 'static>(
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline,
    ) -> (Snapshot, Result<Batch, QueryError>) {
        let result = QueryManager::new().prepare_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            &function_manager,
            &pipeline,
        );
        let pipeline = match result {
            Ok(pipeline) => pipeline,
            Err((snapshot, err)) => return (snapshot, Err(err)),
        };

        let (iterator, snapshot) = match pipeline.into_iterator() {
            Ok((iterator, snapshot)) => (iterator, snapshot),
            Err((snapshot, err)) => return (Arc::into_inner(snapshot).unwrap(), Err(QueryError::WritePipelineExecutionError { source: err })),
        };

        // collect so the snapshot Arc is no longer held by the iterator
        match iterator.collect_owned() {
            Ok(batch) => (Arc::into_inner(snapshot).unwrap(), Ok(batch)),
            Err(err) => (Arc::into_inner(snapshot).unwrap(), Err(QueryError::WritePipelineExecutionError { source: err })),
        }
    }

    async fn blocking_execute_read_query(
        &self,
        pipeline: &Pipeline,
        sender: Sender<Option<Res>>,
    ) -> JoinHandle<Result<(), Status>> {
        debug_assert!(self.request_queue.is_empty() && self.active_write_query.is_none() && self.transaction.is_some());
        match self.transaction.as_ref().unwrap() {
            Transaction::Read(transaction) => {
                let executor = Self::prepare_read_query_in(
                    transaction.snapshot.clone(),
                    &transaction.type_manager,
                    transaction.thing_manager.clone(),
                    &transaction.function_manager,
                    pipeline,
                );

                let named_outputs: Vec<(String, VariablePosition)> = executor.named_selected_outputs()
                    .into_iter()
                    .map(|(position, name)| (name, position))
                    .sorted()
                    .collect();

                Self::submit_concept_rows_stream_descriptor(&sender, &named_outputs);

                let mut iterator = match executor.into_iterator() {
                    Ok((iterator, _)) => iterator,
                    Err((_, err)) => {
                        Self::submit_error_and_terminator(&sender, Err(QueryError::ReadPipelineExecutionError { source: err }));
                        return;
                    }
                };

                // TODO: send row header message

                while let Some(next) = iterator.next() {
                    match next {
                        Ok(row) => Self::submit_row(&sender, row, &named_outputs),
                        Err(err) => {
                            Self::submit_error_and_terminator(&sender, err);
                            return;
                        }
                    }
                }
                todo!()
            }
            Transaction::Write(transaction) => {
                todo!()
            }
            Transaction::Schema(transaction) => {
                todo!()
            }
        }
    }

    fn submit_concept_rows_stream_descriptor(sender: &Sender<Option<Res>>, columns: &[(String, VariablePosition)]) {
        // TODO: we could also write variable optionality and categories here?
        let mut header = typedb_protocol::query::res::ok::ConceptRowStream::default();
        header.column_variable_names.extend(columns.iter().map(|(name, _)| name));
        let message = typedb_protocol::query::res::Res::Ok(
            typedb_protocol::query::res::Ok {
                ok: Some(
                    typedb_protocol::query::res::ok::Ok::ConceptMapStream(header)
                )
            }
        );
        sender.blocking_send(Some(Res { res: Some(message) })).unwrap()
    }

    fn submit_row(sender: &Sender<Option<Res>>, row: MaybeOwnedRow, columns: &[(String, VariablePosition)]) {
        let mut row_message = typedb_protocol::ConceptRow::default();
        for (_, position) in columns {
            let concept = row.get(*position);
        }
    }

    fn submit_error_and_terminator(sender: &Sender<Option<Res>>, error: impl IntoProtocolErrorMessage) {
        let err = error.into_error_message();
        match sender.blocking_send(Some(typedb_protocol::query::Res {
            res: Some(typedb_protocol::query::res::Res::Error(err))
        })) {
            Ok(_) => if let Err(err) = sender.blocking_send(None) {
                event!(
                    Level::DEBUG,
                    "Worker failed to send final empty message after error message: {:?}", err
                )
            },
            Err(err) => event!(Level::ERROR, "Worker failed to send error message: {:?}", err),
        }
    }

    fn prepare_read_query_in<Snapshot: ReadableSnapshot + 'static>(
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline,
    ) -> ReadPipelineStage<Snapshot> {
        QueryManager::new().prepare_read_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            &function_manager,
            &pipeline,
        )?
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
        QueryExecutionFailed(9, "Query execution failed.", ( typedb_source : QueryError )),
    }
);
