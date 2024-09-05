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
    time::SystemTime,
};

use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::{
    database_manager::DatabaseManager,
    transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite},
};
use error::typedb_error;
use executor::{
    batch::Batch,
    pipeline::{
        stage::{ReadPipelineStage, WriteStageIterator},
        PipelineExecutionError, StageAPI, StageIterator,
    },
};
use function::function_manager::FunctionManager;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use options::TransactionOptions;
use query::{error::QueryError, query_manager::QueryManager};
use resource::constants::server::{DEFAULT_PREFETCH_SIZE, DEFAULT_TRANSACTION_TIMEOUT_MILLIS};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use tokio::{
    sync::{
        broadcast,
        mpsc::{channel, Receiver, Sender},
    },
    task::{spawn_blocking, JoinHandle},
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::transaction::stream_signal::Req;
use typeql::{
    parse_query,
    query::{stage::Stage, Pipeline, SchemaQuery},
    Query,
};
use uuid::Uuid;

use crate::service::{
    answer::encode_row,
    error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError},
};

// TODO: where does this belong?
#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

macro_rules! with_readable_transaction {
    ($match_:expr, |$transaction: ident| $block: block ) => {{
        match $match_ {
            Transaction::Read($transaction) => $block
            Transaction::Write($transaction) => $block
            Transaction::Schema($transaction) => $block
        }
    }}
}

#[derive(Debug)]
pub(crate) struct TransactionService {
    database_manager: Arc<DatabaseManager>,

    request_stream: Streaming<typedb_protocol::transaction::Client>,
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
    read_responders:
        HashMap<Uuid, (JoinHandle<()>, JoinHandle<ControlFlow<(), (Uuid, Receiver<Option<QueryResponse>>)>>)>,
    write_responders: HashMap<Uuid, JoinHandle<()>>,
    active_write_query: Option<(Uuid, JoinHandle<(Transaction, Result<Batch, QueryError>)>)>,
}

macro_rules! close_service {
    () => {
        return;
    };
}

macro_rules! close_service_with_error {
    ($self: ident, $error: expr) => {{
        let result = $self.response_sender.send(Err($error)).await;
        if let Err(send_error) = result {
            event!(Level::DEBUG, ?send_error, "Failed to send error to client");
        }
        return;
    }};
}

macro_rules! unwrap_or_submit_error_and_return {
    ($match_: expr, $sender: expr, |$err:ident| $err_mapper: expr) => {{
        match $match_ {
            Ok(inner) => inner,
            Err($err) => {
                Self::submit_error_and_terminator($sender, $err_mapper);
                return;
            }
        }
    }};
}

macro_rules! query_res_ok {
    ($message: expr) => {{
        typedb_protocol::query::res::Ok { ok: Some($message) }
    }};
}

macro_rules! query_res_part_res {
    ($message: expr) => {{
        typedb_protocol::query::res_part::Res { res: Some($message) }
    }};
}

macro_rules! transaction_server_res_part_query_res {
    ($messages: expr) => {{
        typedb_protocol::transaction::res_part::ResPart::QueryRes(typedb_protocol::query::ResPart { res: $messages })
    }};
}

macro_rules! transaction_res_part_stream_signal_done {
    () => {{
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Done(
                    typedb_protocol::transaction::stream_signal::res_part::Done {},
                )),
            },
        )
    }};
}

macro_rules! transaction_res_part_stream_signal_continue {
    () => {{
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Continue(
                    typedb_protocol::transaction::stream_signal::res_part::Continue {},
                )),
            },
        )
    }};
}

macro_rules! transaction_res_part_stream_signal_error {
    ($error_message: expr) => {{
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Error($error_message)),
            },
        )
    }};
}

macro_rules! transaction_server_res {
    ($req_id: expr, $message: expr) => {{
        typedb_protocol::transaction::Server {
            server: Some(typedb_protocol::transaction::server::Server::Res(typedb_protocol::transaction::Res {
                req_id: $req_id.as_bytes().to_vec(),
                res: Some($message),
            })),
        }
    }};
}

macro_rules! transaction_server_res_part {
    ($req_id: expr, $message: expr) => {{
        typedb_protocol::transaction::Server {
            server: Some(typedb_protocol::transaction::server::Server::ResPart(
                typedb_protocol::transaction::ResPart { req_id: $req_id.as_bytes().to_vec(), res_part: Some($message) },
            )),
        }
    }};
}

macro_rules! send_ok_message_else_return_break {
    ($response_sender: expr, $message: expr) => {{
        if let Err(err) = $response_sender.send(Ok($message)).await {
            event!(Level::TRACE, "Submit message failed: {:?}", err);
            return ControlFlow::Break(());
        }
    }};
}

enum QueryResponse {
    ResOk(typedb_protocol::query::res::Ok),
    ResErr(typedb_protocol::Error),
    ResPartRes(typedb_protocol::query::res_part::Res),
}

enum StreamingCondition {
    Count(usize),
    Duration(SystemTime, usize),
}

impl StreamingCondition {
    fn continue_(&self, iteration: usize) -> bool {
        match self {
            StreamingCondition::Count(count) => *count < iteration,
            StreamingCondition::Duration(start_time, limit_millis) => {
                (SystemTime::now().duration_since(*start_time).unwrap().as_millis() as usize) < *limit_millis
            }
        }
    }
}

impl TransactionService {
    pub(crate) fn new(
        request_stream: Streaming<typedb_protocol::transaction::Client>,
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
                                        close_service_with_error!(self, err);
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
        let database = self.database_manager.database(database_name.as_ref()).ok_or_else(|| {
            TransactionServiceError::DatabaseNotFound { name: database_name }.into_error_message().into_status()
        })?;

        let transaction = match transaction_type {
            typedb_protocol::transaction::Type::Read => Transaction::Read(TransactionRead::open(database, transaction_options)),
            typedb_protocol::transaction::Type::Write => Transaction::Write(TransactionWrite::open(database, transaction_options)),
            typedb_protocol::transaction::Type::Schema => Transaction::Schema(TransactionSchema::open(database, transaction_options)),
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
            Transaction::Read(_) => {
                Err(TransactionServiceError::CannotCommitReadTransaction {}.into_error_message().into_status())
            }
            Transaction::Write(transaction) => spawn_blocking(move || {
                transaction.commit().map_err(|err| {
                    TransactionServiceError::DataCommitFailed { source: err }.into_error_message().into_status()
                })
            })
            .await
            .unwrap(),
            Transaction::Schema(transaction) => transaction.commit().map_err(|err| {
                TransactionServiceError::SchemaCommitFailed { source: err }.into_error_message().into_status()
            }),
        }
    }

    fn handle_rollback(&mut self, rollback_req: typedb_protocol::transaction::rollback::Req) -> Result<(), Status> {
        // Interrupt all running read queries and any running write query
        // Await all read query join handles, then clear all read responders
        // Await all running write responders, then clear all write responders
        // Clear queue
        // Execute rollback

        match self.transaction.take().unwrap() {
            Transaction::Read(_) => {
                return Err(TransactionServiceError::CannotRollbackReadTransaction {}
                    .into_error_message()
                    .into_status())
            }
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
        let parsed = parse_query(query_string).map_err(|err| {
            TransactionServiceError::QueryParseFailed { source: err }.into_error_message().into_status()
        })?;
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
                        let handle = self.blocking_execute_write_query(pipeline)?;
                        self.active_write_query = Some((req_id, tokio::spawn(async move { handle.await.unwrap() })));
                        Ok(())
                    }
                } else {
                    if !self.request_queue.is_empty() || self.active_write_query.is_some() {
                        self.request_queue.push((req_id, pipeline));
                        Ok(())
                    } else {
                        let (sender, receiver) = channel(self.prefetch_size.unwrap() as usize);
                        let worker_handle = self.blocking_execute_read_query(pipeline, sender);
                        let transmitter_handle = tokio::spawn(Self::stream_parts(
                            self.response_sender.clone(),
                            self.prefetch_size.unwrap() as usize,
                            self.network_latency_millis.unwrap() as usize,
                            req_id,
                            receiver,
                        ));
                        self.read_responders.insert(req_id, (worker_handle, transmitter_handle));
                        Ok(())
                    }
                }
            }
        }
    }

    async fn stream_parts(
        response_sender: Sender<Result<typedb_protocol::transaction::Server, Status>>,
        prefetch_size: usize,
        network_latency_millis: usize,
        req_id: Uuid,
        query_response_receiver: Receiver<Option<QueryResponse>>,
    ) -> ControlFlow<(), (Uuid, Receiver<Option<QueryResponse>>)> {
        // stream PREFETCH answers in one big message (increases message throughput. Note: tested in Java impl)
        let (req_id, query_response_receiver) = Self::stream_while_or_finish(
            &response_sender,
            req_id,
            query_response_receiver,
            StreamingCondition::Count(prefetch_size),
        )
        .await?;
        send_ok_message_else_return_break!(response_sender, Self::message_stream_signal_continue(req_id));

        // stream LATENCY number of answers
        let (req_id, query_response_receiver) = Self::stream_while_or_finish(
            &response_sender,
            req_id,
            query_response_receiver,
            StreamingCondition::Duration(SystemTime::now(), network_latency_millis),
        )
        .await?;
        ControlFlow::Continue((req_id, query_response_receiver))
    }

    async fn stream_while_or_finish(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        mut query_response_receiver: Receiver<Option<QueryResponse>>,
        streaming_condition: StreamingCondition,
    ) -> ControlFlow<(), (Uuid, Receiver<Option<QueryResponse>>)> {
        let mut res_parts_batch = Vec::new();
        let mut iteration = 0;
        while streaming_condition.continue_(iteration) {
            match query_response_receiver.recv().await {
                None => {
                    send_ok_message_else_return_break!(
                        response_sender,
                        Self::message_stream_signal_error(
                            req_id,
                            QueryError::QueryExecutionClosedEarly {}.into_error_message()
                        )
                    );
                    return ControlFlow::Break(());
                }
                Some(response) => match response {
                    None => {
                        send_ok_message_else_return_break!(response_sender, Self::message_stream_signal_done(req_id));
                        return ControlFlow::Break(());
                    }
                    Some(query_response) => match query_response {
                        QueryResponse::ResOk(_) => unreachable!("Streams should only respond ResPart or Error"),
                        QueryResponse::ResErr(res_error) => {
                            if !res_parts_batch.is_empty() {
                                send_ok_message_else_return_break!(
                                    response_sender,
                                    Self::message_stream_res_parts(req_id, res_parts_batch)
                                );
                            }
                            send_ok_message_else_return_break!(
                                response_sender,
                                Self::message_stream_signal_error(req_id, res_error)
                            );
                            return ControlFlow::Break(());
                        }
                        QueryResponse::ResPartRes(res_part) => res_parts_batch.push(res_part),
                    },
                },
            }
            iteration += 1;
        }
        if !res_parts_batch.is_empty() {
            send_ok_message_else_return_break!(
                response_sender,
                Self::message_stream_res_parts(req_id, res_parts_batch)
            );
        }
        ControlFlow::Continue((req_id, query_response_receiver))
    }

    fn message_stream_res_parts(
        req_id: Uuid,
        res_parts: Vec<typedb_protocol::query::res_part::Res>,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res_part!(req_id, transaction_server_res_part_query_res!(res_parts))
    }

    fn message_stream_signal_continue(req_id: Uuid) -> typedb_protocol::transaction::Server {
        transaction_server_res_part!(req_id, transaction_res_part_stream_signal_continue!())
    }

    fn message_stream_signal_done(req_id: Uuid) -> typedb_protocol::transaction::Server {
        transaction_server_res_part!(req_id, transaction_res_part_stream_signal_done!())
    }

    fn message_stream_signal_error(
        req_id: Uuid,
        error: typedb_protocol::Error,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res_part!(req_id, transaction_res_part_stream_signal_error!(error))
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
                    .map_err(|err| {
                        TransactionServiceError::QueryExecutionFailed { typedb_source: err }
                            .into_error_message()
                            .into_status()
                    });
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
            Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}.into_error_message().into_status())
        }
    }

    fn blocking_execute_write_query(
        &mut self,
        pipeline: Pipeline,
    ) -> Result<JoinHandle<(Transaction, Result<Batch, QueryError>)>, Status> {
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
                    Arc::into_inner(snapshot).unwrap(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &pipeline,
                );

                let transaction = Transaction::Schema(TransactionSchema::from(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                ));
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
                    Arc::into_inner(snapshot).unwrap(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &pipeline,
                );

                let transaction = Transaction::Write(TransactionWrite::from(
                    Arc::new(snapshot),
                    type_manager,
                    thing_manager,
                    function_manager,
                    database,
                    transaction_options,
                ));
                (transaction, result)
            }))
        } else {
            return Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}
                .into_error_message()
                .into_status());
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
            Err((snapshot, err)) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(QueryError::WritePipelineExecutionError { typedb_source: err }),
                )
            }
        };

        // collect so the snapshot Arc is no longer held by the iterator
        match iterator.collect_owned() {
            Ok(batch) => (Arc::into_inner(snapshot).unwrap(), Ok(batch)),
            Err(err) => (
                Arc::into_inner(snapshot).unwrap(),
                Err(QueryError::WritePipelineExecutionError { typedb_source: err }),
            ),
        }
    }

    fn blocking_execute_read_query(&self, pipeline: Pipeline, sender: Sender<Option<QueryResponse>>) -> JoinHandle<()> {
        debug_assert!(self.request_queue.is_empty() && self.active_write_query.is_none() && self.transaction.is_some());

        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let function_manager = transaction.function_manager.clone();
            spawn_blocking(move || {
                let executor = Self::prepare_read_query_in(
                    snapshot.clone(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &pipeline,
                );

                let executor = unwrap_or_submit_error_and_return!(executor, &sender, |err| err);

                let named_outputs: Vec<(String, VariablePosition)> = executor
                    .named_selected_outputs()
                    .into_iter()
                    .map(|(position, name)| (name, position))
                    .sorted()
                    .collect();

                Self::submit_stream_row_descriptor(&sender, &named_outputs);

                let (mut iterator, _) =
                    unwrap_or_submit_error_and_return!(executor.into_iterator(), &sender, |snapshot_and_err| {
                        QueryError::ReadPipelineExecutionError { typedb_source: snapshot_and_err.1 }
                    });

                while let Some(next) = iterator.next() {
                    let row = unwrap_or_submit_error_and_return!(next, &sender, |err| err);
                    let encoded = encode_row(row, &named_outputs, snapshot.as_ref(), &type_manager, &thing_manager);
                    match encoded {
                        Ok(encoded) => Self::submit_stream_row(&sender, encoded),
                        Err(err) => {
                            Self::submit_error_and_terminator(
                                &sender,
                                PipelineExecutionError::ConceptRead { source: err },
                            );
                            return;
                        }
                    }
                }
                Self::submit_terminator(&sender);
            })
        })
    }

    fn submit_stream_row_descriptor(sender: &Sender<Option<QueryResponse>>, columns: &[(String, VariablePosition)]) {
        let mut header = typedb_protocol::query::res::ok::AnswerRowStream::default();
        header.column_variable_names.extend(columns.iter().map(|(name, _)| name.to_string()));
        let response =
            QueryResponse::ResOk(query_res_ok!(typedb_protocol::query::res::ok::Ok::ConceptMapStream(header)));
        sender.blocking_send(Some(response)).unwrap()
    }

    fn submit_stream_row(sender: &Sender<Option<QueryResponse>>, row: typedb_protocol::AnswerRow) {
        let res_part_res_row = typedb_protocol::query::res_part::res::Res::AnswerRow(row);
        let response = QueryResponse::ResPartRes(query_res_part_res!(res_part_res_row));
        sender.blocking_send(Some(response)).unwrap()
    }

    fn submit_error_and_terminator(sender: &Sender<Option<QueryResponse>>, error: impl IntoProtocolErrorMessage) {
        let response = QueryResponse::ResErr(error.into_error_message());
        match sender.blocking_send(Some(response)) {
            Ok(_) => Self::submit_terminator(sender),
            Err(err) => event!(Level::ERROR, "Worker failed to send error message: {:?}", err),
        }
    }

    fn submit_terminator(sender: &Sender<Option<QueryResponse>>) {
        if let Err(err) = sender.blocking_send(None) {
            event!(Level::DEBUG, "Worker failed to send final empty message after error message: {:?}", err)
        }
    }

    fn prepare_read_query_in<Snapshot: ReadableSnapshot + 'static>(
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline,
    ) -> Result<ReadPipelineStage<Snapshot>, QueryError> {
        QueryManager::new().prepare_read_pipeline(snapshot, type_manager, thing_manager, &function_manager, &pipeline)
    }

    async fn handle_stream_continue(&mut self, request_id: Uuid, stream_req: Req) -> Result<(), Status> {
        todo!()
        // debug_assert!(
        //     self.read_responders.contains_key(&request_id) || self.write_responders.contains_key(&request_id)
        // );
        // let read_responder = self.read_responders.get_mut(&request_id);
        // if let Some(responder) = read_responder {
        //     match responder {
        //         Responder::Active(handle) => {
        //             if !handle.is_finished() {
        //                 event!(
        //                     Level::DEBUG,
        //                     "Responder is already active, and received a Stream Continue request.\
        //                          Unexpected behaviour, but not an error state."
        //                 );
        //             } else {
        //                 handle.await.unwrap();
        //                 // TODO: recreate a task to stream back out from the iterator and set the handle
        //                 // *handle = ..
        //             }
        //         }
        //         Responder::Waiting(()) => {
        //             todo!()
        //         }
        //     }
        // } else {
        //     let write_responder = self.write_responders.get_mut(&request_id);
        //     if let Some(responder) = write_responder {
        //         match responder {
        //             Responder::Active(handle) => {
        //                 if !handle.is_finished() {
        //                     event!(
        //                         Level::DEBUG,
        //                         "Responder is already active, and received a Stream Continue request.\
        //                          Unexpected behaviour, but not an error state."
        //                     );
        //                 } else {
        //                     handle.await.unwrap();
        //                     // TODO: recreate a task to stream back out from the iterator and set the handle
        //                     // *handle = ..
        //                 }
        //             }
        //             Responder::Waiting(()) => {
        //                 todo!()
        //             }
        //         }
        //     } else {
        //         return Err(ProtocolError::QueryStreamNotFound { query_request_id: request_id }.into_status());
        //     }
        // }
        // Ok(())
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
