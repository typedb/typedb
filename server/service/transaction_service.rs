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
    time::{Instant, SystemTime},
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
        stage::{ExecutionContext, ReadPipelineStage, StageAPI, StageIterator},
        PipelineExecutionError,
    },
    ExecutionInterrupt,
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
use typedb_protocol::transaction::{stream_signal::Req, Server};
use typeql::{
    parse_query,
    query::{stage::Stage, Pipeline, SchemaQuery},
    Query,
};
use uuid::Uuid;

use crate::service::{
    answer::encode_row,
    error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError},
    response_builders::transaction::{
        query_initial_res_from_error, query_initial_res_from_query_res_ok, query_initial_res_ok_from_query_res_ok_ok,
        query_res_ok_concept_row_stream, query_res_ok_empty, query_res_part_from_concept_rows, transaction_open_res,
        transaction_server_res_part_stream_signal_continue, transaction_server_res_part_stream_signal_done,
        transaction_server_res_part_stream_signal_error, transaction_server_res_parts_query_part,
        transaction_server_res_query_res,
    },
};

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
    query_interrupt_sender: broadcast::Sender<()>,
    query_interrupt_receiver: ExecutionInterrupt,

    transaction_timeout_millis: Option<u64>,
    schema_lock_acquire_timeout_millis: Option<u64>,
    prefetch_size: Option<u64>,
    network_latency_millis: Option<u64>,

    is_open: bool,
    transaction: Option<Transaction>,
    request_queue: Vec<(Uuid, Pipeline)>,
    read_responders: HashMap<Uuid, (JoinHandle<()>, QueryStreamTransmitter)>,
    write_responders: HashMap<Uuid, (JoinHandle<()>, QueryStreamTransmitter)>,
    running_write_query:
        Option<(Uuid, JoinHandle<(Transaction, Result<(StreamQueryOutputDescriptor, Batch), QueryError>)>)>,
}

macro_rules! unwrap_or_execute_and_return {
    ($match_: expr, |$err:pat_param| $err_mapper: block) => {{
        match $match_ {
            Ok(inner) => inner,
            Err($err) => {
                $err_mapper
                return;
            }
        }
    }};
}

macro_rules! send_ok_message_else_return_break {
    ($response_sender: expr, $message: expr) => {{
        if let Err(err) = $response_sender.send(Ok($message)).await {
            event!(Level::TRACE, "Submit message failed: {:?}", err);
            return Break(());
        }
    }};
}

enum ImmediateQueryResponse {
    NonFatalErr(typedb_protocol::Error),
    ResOk(typedb_protocol::query::initial_res::Ok),
}

impl ImmediateQueryResponse {
    fn non_fatal_err(error: impl IntoProtocolErrorMessage) -> Self {
        Self::NonFatalErr(error.into_error_message())
    }

    fn ok(ok_message: typedb_protocol::query::initial_res::ok::Ok) -> Self {
        ImmediateQueryResponse::ResOk(typedb_protocol::query::initial_res::Ok { ok: Some(ok_message) })
    }
}

type StreamQueryOutputDescriptor = Vec<(String, VariablePosition)>;

enum StreamQueryResponse {
    // initial open response
    InitOk(typedb_protocol::query::initial_res::Ok),
    InitErr(typedb_protocol::Error),
    // stream responses
    StreamNextRow(typedb_protocol::ConceptRow),
    StreamDoneOk(),
    StreamDoneErr(typedb_protocol::Error),
}

impl StreamQueryResponse {
    fn init_ok(columns: &StreamQueryOutputDescriptor) -> Self {
        let columns = columns.iter().map(|(name, _)| name.to_string()).collect();
        let message = query_res_ok_concept_row_stream(columns);
        Self::InitOk(query_initial_res_ok_from_query_res_ok_ok(message))
    }

    fn init_err(error: impl IntoProtocolErrorMessage) -> Self {
        Self::InitErr(error.into_error_message())
    }

    fn next_row(row: typedb_protocol::ConceptRow) -> Self {
        Self::StreamNextRow(row)
    }

    fn done_ok() -> Self {
        Self::StreamDoneOk()
    }

    fn done_err(error: impl IntoProtocolErrorMessage) -> Self {
        Self::StreamDoneErr(error.into_error_message())
    }
}

enum StreamingCondition {
    Count(usize),
    Duration(Instant, usize),
}

impl StreamingCondition {
    fn continue_(&self, iteration: usize) -> bool {
        match self {
            StreamingCondition::Count(count) => {
                let result = iteration < *count;
                result
            }
            StreamingCondition::Duration(start_time, limit_millis) => {
                (Instant::now().duration_since(*start_time).as_millis() as usize) < *limit_millis
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
        let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);

        Self {
            database_manager,

            request_stream,
            response_sender,
            query_interrupt_sender,
            query_interrupt_receiver: ExecutionInterrupt::new(query_interrupt_receiver),

            transaction_timeout_millis: None,
            schema_lock_acquire_timeout_millis: None,
            prefetch_size: None,
            network_latency_millis: None,

            is_open: false,
            transaction: None,
            request_queue: Vec::with_capacity(20),
            read_responders: HashMap::new(),
            write_responders: HashMap::new(),
            running_write_query: None,
        }
    }

    pub(crate) async fn listen(&mut self) {
        loop {
            let result = if self.running_write_query.is_some() {
                let (req_id, write_query_worker) = self.running_write_query.as_mut().unwrap();
                tokio::select! { biased;
                    write_query_result = write_query_worker => {
                        let req_id = *req_id;
                        self.running_write_query = None;
                        let (transaction, result) = write_query_result.unwrap();
                        self.write_query_finished(req_id, transaction, result)
                            .map(|_| ControlFlow::Continue(()))
                    }
                    next = self.request_stream.next() => {
                        self.handle_next(next).await
                    }
                }
            } else {
                let next = self.request_stream.next().await;
                self.handle_next(next).await
            };

            match result {
                Ok(Continue(())) => (),
                Ok(Break(())) => {
                    event!(Level::TRACE, "Stream ended, closing transaction service.");
                    self.do_close().await;
                    return;
                }
                Err(status) => {
                    event!(Level::TRACE, "Stream ended with error, closing transaction service.");
                    let result = self.response_sender.send(Err(status)).await;
                    if let Err(send_error) = result {
                        event!(Level::DEBUG, ?send_error, "Failed to send error to client");
                    }
                    self.do_close().await;
                    return;
                }
            }
        }
    }

    // TODO: any method using `Result<ControlFlow<(), ()>, Status>` should really be `ControlFlow<Result<(), Status>, ()>`
    async fn handle_next(
        &mut self,
        next: Option<Result<typedb_protocol::transaction::Client, Status>>,
    ) -> Result<ControlFlow<(), ()>, Status> {
        match next {
            None => Ok(Break(())),
            Some(Err(error)) => {
                event!(Level::DEBUG, ?error, "GRPC error");
                Ok(Break(()))
            }
            Some(Ok(message)) => {
                for request in message.reqs {
                    let request_id = Uuid::from_slice(&request.req_id).unwrap();
                    let metadata = request.metadata;
                    match request.req {
                        None => {
                            return Err(ProtocolError::MissingField {
                                name: "req",
                                description: "Transaction message must contain a request.",
                            }
                            .into_status());
                        }
                        Some(req) => match self.handle_request(request_id, req).await {
                            Err(err) => return Err(err),
                            Ok(Break(())) => return Ok(Break(())),
                            Ok(Continue(())) => {}
                        },
                    }
                }
                Ok(Continue(()))
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
                let result = self.handle_open(request_id, open_req).await;
                match &result {
                    Ok(ControlFlow::Continue(_)) => event!(Level::TRACE, "Transaction opened successfully."),
                    Ok(ControlFlow::Break(_)) => event!(Level::TRACE, "Transaction open aborted."),
                    Err(status) => event!(Level::TRACE, "Error opening transaction: {}", status),
                }
                result
            }
            (true, typedb_protocol::transaction::req::Req::OpenReq(_)) => {
                Err(ProtocolError::TransactionAlreadyOpen {}.into_status())
            }
            (true, typedb_protocol::transaction::req::Req::QueryReq(query_req)) => {
                let query_response = self.handle_query(request_id, query_req).await?;
                if let Some(query_response) = query_response {
                    return Ok(Self::respond_query_response(&self.response_sender, request_id, query_response).await);
                } else {
                    return Ok(Continue(()));
                }
            }
            (true, typedb_protocol::transaction::req::Req::StreamReq(stream_req)) => {
                match self.handle_stream_continue(request_id, stream_req).await {
                    None => return Ok(Continue(())),
                    Some(query_response) => {
                        return Ok(
                            Self::respond_query_response(&self.response_sender, request_id, query_response).await
                        );
                    }
                }
            }
            (true, typedb_protocol::transaction::req::Req::CommitReq(commit_req)) => {
                // Eagerly executed in main loop
                self.handle_commit(commit_req).await?;
                Ok(Break(()))
            }
            (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {
                self.handle_rollback(rollback_req).await
            }
            (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                self.handle_close(close_req).await;
                Ok(Break(()))
            }
            (false, _) => Err(ProtocolError::TransactionClosed {}.into_status()),
        }
    }

    async fn respond_query_response(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        immediate_query_response: ImmediateQueryResponse,
    ) -> ControlFlow<(), ()> {
        match immediate_query_response {
            ImmediateQueryResponse::NonFatalErr(err) => {
                send_ok_message_else_return_break!(
                    response_sender,
                    transaction_server_res_query_res(req_id, query_initial_res_from_error(err))
                );
                Continue(())
            }
            ImmediateQueryResponse::ResOk(res) => {
                send_ok_message_else_return_break!(
                    response_sender,
                    transaction_server_res_query_res(req_id, query_initial_res_from_query_res_ok(res))
                );
                Continue(())
            }
        }
    }

    async fn handle_open(
        &mut self,
        req_id: Uuid,
        open_req: typedb_protocol::transaction::open::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {
        let receive_time = Instant::now();
        self.network_latency_millis = Some(open_req.network_latency_millis);
        let mut transaction_options = TransactionOptions::default();
        if let Some(options) = open_req.options {
            // transaction options
            if let Some(parallel) = options.parallel {
                transaction_options.parallel = parallel;
            }
            if let Some(timeout) = options.schema_lock_acquire_timeout_millis {
                transaction_options.schema_lock_acquire_timeout_millis = timeout
            }

            // service options
            self.prefetch_size = options.prefetch_size.or(Some(DEFAULT_PREFETCH_SIZE));
            self.transaction_timeout_millis =
                options.transaction_timeout_millis.or(Some(DEFAULT_TRANSACTION_TIMEOUT_MILLIS));
        }

        let transaction_type = typedb_protocol::transaction::Type::try_from(open_req.r#type)
            .map_err(|_| ProtocolError::UnrecognisedTransactionType { enum_variant: open_req.r#type }.into_status())?;

        let database_name = open_req.database;
        let database = self.database_manager.database(database_name.as_ref()).ok_or_else(|| {
            TransactionServiceError::DatabaseNotFound { name: database_name }.into_error_message().into_status()
        })?;

        let transaction = match transaction_type {
            typedb_protocol::transaction::Type::Read => {
                Transaction::Read(TransactionRead::open(database, transaction_options))
            }
            typedb_protocol::transaction::Type::Write => {
                Transaction::Write(TransactionWrite::open(database, transaction_options))
            }
            typedb_protocol::transaction::Type::Schema => {
                Transaction::Schema(TransactionSchema::open(database, transaction_options))
            }
        };
        self.transaction = Some(transaction);
        self.is_open = true;

        let processing_time_millis = Instant::now().duration_since(receive_time).as_millis() as u64;
        if let Err(err) = self.response_sender.send(Ok(transaction_open_res(req_id, processing_time_millis))).await {
            event!(Level::TRACE, "Submit message failed: {:?}", err);
            Ok(Break(()))
        } else {
            Ok(Continue(()))
        }
    }

    async fn handle_commit(&mut self, _commit_req: typedb_protocol::transaction::commit::Req) -> Result<(), Status> {
        // finish any running write query, interrupt running queries, clear all running/queued reads, finish all writes
        //   note: if any write query errors, the whole transaction errors
        // finish any active write query
        self.finish_running_write_query().await?;

        // interrupt active queries and close write transmitters
        self.query_interrupt_sender.send(()).unwrap();
        self.close_running_read_queries().await;
        if let Break(()) = self.cancel_queued_read_queries().await {
            return Err(TransactionServiceError::ServiceClosingFailedQueueCleanup {}
                .into_error_message()
                .into_status());
        }
        self.close_transmitting_write_queries().await;

        // finish executing any remaining writes so they make it into the commit
        self.finish_queued_write_queries().await?;

        match self.transaction.take().unwrap() {
            Transaction::Read(_) => {
                Err(TransactionServiceError::CannotCommitReadTransaction {}.into_error_message().into_status())
            }
            Transaction::Write(transaction) => spawn_blocking(move || {
                transaction.commit().map_err(|err| {
                    TransactionServiceError::DataCommitFailed { typedb_source: err }.into_error_message().into_status()
                })
            })
            .await
            .unwrap(),
            Transaction::Schema(transaction) => transaction.commit().map_err(|err| {
                TransactionServiceError::SchemaCommitFailed { source: err }.into_error_message().into_status()
            }),
        }
    }

    async fn handle_rollback(
        &mut self,
        _rollback_req: typedb_protocol::transaction::rollback::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {
        // interrupt all queries, cancel writes, then rollback
        self.query_interrupt_sender.send(()).unwrap();
        self.close_transmitting_write_queries().await;
        self.close_running_read_queries().await;
        if let Break(_) = self.cancel_queued_read_queries().await {
            return Ok(Break(()));
        }

        self.finish_running_write_query().await?;
        if let Break(()) = self.cancel_queued_write_queries().await {
            return Ok(Break(()));
        }

        match self.transaction.take().unwrap() {
            Transaction::Read(_) => {
                return Err(TransactionServiceError::CannotRollbackReadTransaction {}
                    .into_error_message()
                    .into_status());
            }
            Transaction::Write(mut transaction) => transaction.rollback(),
            Transaction::Schema(mut transaction) => transaction.rollback(),
        };
        Ok(Continue(()))
    }

    async fn handle_close(&mut self, _close_req: typedb_protocol::transaction::close::Req) {
        self.do_close().await
    }

    async fn do_close(&mut self) {
        self.query_interrupt_sender.send(()).unwrap();
        self.close_transmitting_write_queries().await;
        self.close_running_read_queries().await;
        let _ = self.cancel_queued_read_queries().await;

        let _ = self.finish_running_write_query().await;
        let _ = self.cancel_queued_write_queries().await;

        match self.transaction.take() {
            None => (),
            Some(Transaction::Read(transaction)) => transaction.close(),
            Some(Transaction::Write(transaction)) => transaction.close(),
            Some(Transaction::Schema(transaction)) => transaction.close(),
        };
    }

    async fn close_running_read_queries(&mut self) {
        for (_, (worker, transmitter)) in self.read_responders.drain() {
            if let Err(err) = worker.await {
                event!(Level::DEBUG, "Awaiting read query worker returned error: {:?}", err);
            }
            transmitter.finish_current().await
        }
    }

    async fn close_transmitting_write_queries(&mut self) {
        for (_, (worker, transmitter)) in self.write_responders.drain() {
            if let Err(err) = worker.await {
                event!(Level::DEBUG, "Awaiting dummy write worker returned error: {:?}", err);
            }
            transmitter.finish_current().await
        }
    }

    async fn cancel_queued_read_queries(&mut self) -> ControlFlow<(), ()> {
        let mut write_queries = Vec::with_capacity(self.request_queue.len());
        for (req_id, pipeline) in self.request_queue.drain(0..self.request_queue.len()) {
            if Self::is_write_pipeline(&pipeline) {
                write_queries.push((req_id, pipeline));
            }
            Self::respond_query_response(
                &self.response_sender,
                req_id,
                ImmediateQueryResponse::NonFatalErr(TransactionServiceError::QueryInterrupted {}.into_error_message()),
            )
            .await?;
        }

        self.request_queue = write_queries;
        Continue(())
    }

    async fn finish_running_write_query(&mut self) -> Result<(), Status> {
        if let Some((req_id, worker)) = self.running_write_query.take() {
            let (transaction, result) = worker.await.unwrap();
            self.write_query_finished(req_id, transaction, result)
        } else {
            Ok(())
        }
    }

    fn write_query_finished(
        &mut self,
        req_id: Uuid,
        transaction: Transaction,
        result: Result<(StreamQueryOutputDescriptor, Batch), QueryError>,
    ) -> Result<(), Status> {
        self.transaction = Some(transaction);
        match result {
            Ok((output_descriptor, batch)) => {
                self.activate_write_transmitter(req_id, output_descriptor, batch);
                Ok(())
            }
            Err(err) => {
                // we promote write errors to fatal status errors
                Err(err.into_error_message().into_status())
            }
        }
    }

    async fn cancel_queued_write_queries(&mut self) -> ControlFlow<(), ()> {
        let mut read_queries = Vec::with_capacity(self.request_queue.len());
        for (req_id, pipeline) in self.request_queue.drain(0..self.request_queue.len()) {
            if Self::is_write_pipeline(&pipeline) {
                Self::respond_query_response(
                    &self.response_sender,
                    req_id,
                    ImmediateQueryResponse::NonFatalErr(
                        TransactionServiceError::QueryInterrupted {}.into_error_message(),
                    ),
                )
                .await?;
            } else {
                read_queries.push((req_id, pipeline));
            }
        }
        self.request_queue = read_queries;
        Continue(())
    }

    async fn finish_queued_write_queries(&mut self) -> Result<(), Status> {
        self.finish_running_write_query().await?;

        let requests: Vec<_> = self.request_queue.drain(0..self.request_queue.len()).collect();
        for (req_id, pipeline) in requests.into_iter() {
            if Self::is_write_pipeline(&pipeline) {
                match self.run_write_query(req_id, pipeline) {
                    Ok(_) => self.finish_running_write_query().await?,
                    Err(err) => {
                        Self::respond_query_response(
                            &self.response_sender,
                            req_id,
                            ImmediateQueryResponse::non_fatal_err(err),
                        )
                        .await;
                    }
                }
            } else {
                self.request_queue.push((req_id, pipeline));
            }
        }
        Ok(())
    }

    async fn handle_query(
        &mut self,
        req_id: Uuid,
        query_req: typedb_protocol::query::Req,
    ) -> Result<Option<ImmediateQueryResponse>, Status> {
        let _query_options = &query_req.options; // TODO: pass query options
        let parsed = match parse_query(&query_req.query) {
            Ok(parsed) => parsed,
            Err(err) => {
                // non-fatal error
                return Ok(Some(ImmediateQueryResponse::non_fatal_err(TransactionServiceError::QueryParseFailed {
                    typedb_source: err,
                })));
            }
        };
        match parsed {
            Query::Schema(schema_query) => {
                // schema queries are handled immediately so there is a query response or a fatal Status
                self.handle_query_schema(schema_query).await.map(Some)
            }
            Query::Pipeline(pipeline) => {
                #[allow(clippy::collapsible_else_if)]
                if Self::is_write_pipeline(&pipeline) {
                    if !self.request_queue.is_empty()
                        || !self.read_responders.is_empty()
                        || self.running_write_query.is_some()
                    {
                        self.request_queue.push((req_id, pipeline));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(None)
                    } else {
                        match self.run_write_query(req_id, pipeline) {
                            Ok(_) => {
                                // running write queries have no valid response yet (until they finish) and will respond asynchronously
                                Ok(None)
                            }
                            Err(err) => Ok(Some(ImmediateQueryResponse::non_fatal_err(err))),
                        }
                    }
                } else {
                    if !self.request_queue.is_empty() || self.running_write_query.is_some() {
                        self.request_queue.push((req_id, pipeline));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(None)
                    } else {
                        self.run_and_activate_read_transmitter(req_id, pipeline);
                        // running read queries have no response on the main loop and will respond asynchronously
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn handle_query_schema(&mut self, query: SchemaQuery) -> Result<ImmediateQueryResponse, Status> {
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
                let result = QueryManager::new().execute_schema(&mut snapshot, &type_manager, &thing_manager, query);
                (snapshot, type_manager, thing_manager, result)
            })
            .await
            .unwrap();
            let message_ok_empty = result.map(|_| query_res_ok_empty()).map_err(|err| {
                TransactionServiceError::TxnAbortSchemaQueryFailed { typedb_source: err }
                    .into_error_message()
                    .into_status()
            })?;

            let transaction = TransactionSchema::from(
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                transaction_options,
            );
            self.transaction = Some(Transaction::Schema(transaction));
            Ok(ImmediateQueryResponse::ok(message_ok_empty))
        } else {
            Ok(ImmediateQueryResponse::non_fatal_err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}))
        }
    }

    fn run_write_query(&mut self, req_id: Uuid, pipeline: Pipeline) -> Result<(), TransactionServiceError> {
        debug_assert!(self.running_write_query.is_none());
        let handle = self.blocking_execute_write_query(pipeline)?;
        self.running_write_query = Some((req_id, tokio::spawn(async move { handle.await.unwrap() })));
        Ok(())
    }

    fn activate_write_transmitter(
        &mut self,
        req_id: Uuid,
        output_descriptor: StreamQueryOutputDescriptor,
        batch: Batch,
    ) {
        let (sender, receiver) = channel(self.prefetch_size.unwrap() as usize);
        let interrupt = self.query_interrupt_receiver.clone();
        let batch_reader = self.write_query_batch_reader(output_descriptor, batch, sender, interrupt);
        let stream_transmitter = QueryStreamTransmitter::start_new(
            self.response_sender.clone(),
            receiver,
            req_id,
            self.prefetch_size.unwrap() as usize,
            self.network_latency_millis.unwrap() as usize,
        );
        self.write_responders.insert(req_id, (batch_reader, stream_transmitter));
    }

    fn run_and_activate_read_transmitter(&mut self, req_id: Uuid, pipeline: Pipeline) {
        let (sender, receiver) = channel(self.prefetch_size.unwrap() as usize);
        let worker_handle = self.blocking_read_query_worker(pipeline, sender);
        let stream_transmitter = QueryStreamTransmitter::start_new(
            self.response_sender.clone(),
            receiver,
            req_id,
            self.prefetch_size.unwrap() as usize,
            self.network_latency_millis.unwrap() as usize,
        );
        self.read_responders.insert(req_id, (worker_handle, stream_transmitter));
    }

    fn blocking_execute_write_query(
        &mut self,
        pipeline: Pipeline,
    ) -> Result<
        JoinHandle<(Transaction, Result<(StreamQueryOutputDescriptor, Batch), QueryError>)>,
        TransactionServiceError,
    > {
        debug_assert!(
            self.request_queue.is_empty()
                && self.read_responders.is_empty()
                && self.running_write_query.is_none()
                && self.transaction.is_some()
        );
        let interrupt = self.query_interrupt_receiver.clone();
        match self.transaction.take() {
            Some(Transaction::Schema(schema_transaction)) => Ok(spawn_blocking(move || {
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
                    interrupt,
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
            })),
            Some(Transaction::Write(write_transaction)) => Ok(spawn_blocking(move || {
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
                    interrupt,
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
            })),
            Some(Transaction::Read(transaction)) => {
                self.transaction = Some(Transaction::Read(transaction));
                Err(TransactionServiceError::WriteQueryRequiresSchemaOrWriteTransaction {})
            }
            None => Err(TransactionServiceError::NoOpenTransaction {}),
        }
    }

    fn execute_write_query_in<Snapshot: WritableSnapshot + 'static>(
        snapshot: Snapshot,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline,
        interrupt: ExecutionInterrupt,
    ) -> (Snapshot, Result<(StreamQueryOutputDescriptor, Batch), QueryError>) {
        let result = QueryManager::new().prepare_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            pipeline,
        );
        let (query_output_descriptor, pipeline) = match result {
            Ok((executor, named_outputs)) => {
                let named_outputs: StreamQueryOutputDescriptor = named_outputs.into_iter().sorted().collect();
                (named_outputs, executor)
            }
            Err((snapshot, err)) => return (snapshot, Err(err)),
        };

        let (iterator, snapshot) = match pipeline.into_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, .. })) => (iterator, snapshot),
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(QueryError::WritePipelineExecutionError { typedb_source: err }),
                );
            }
        };

        match iterator.collect_owned() {
            Ok(batch) => (Arc::into_inner(snapshot).unwrap(), Ok((query_output_descriptor, batch))),
            Err(err) => (
                Arc::into_inner(snapshot).unwrap(),
                Err(QueryError::WritePipelineExecutionError { typedb_source: err }),
            ),
        }
    }

    // Write query is already executed, but for simplicity, we convert it to something that conform to the same API as the read path
    fn write_query_batch_reader(
        &self,
        output_descriptor: StreamQueryOutputDescriptor,
        batch: Batch,
        sender: Sender<StreamQueryResponse>,
        mut interrupt: ExecutionInterrupt,
    ) -> JoinHandle<()> {
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            tokio::spawn(async move {
                let mut as_lending_iter = batch.into_iterator();
                Self::submit_response_async(&sender, StreamQueryResponse::init_ok(&output_descriptor)).await;

                while let Some(row) = as_lending_iter.next() {
                    if interrupt.check() {
                        Self::submit_response_async(
                            &sender,
                            StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted {}),
                        )
                        .await;
                        return;
                    }

                    let encoded_row =
                        encode_row(row, &output_descriptor, snapshot.as_ref(), &type_manager, &thing_manager);
                    match encoded_row {
                        Ok(encoded_row) => {
                            Self::submit_response_async(&sender, StreamQueryResponse::next_row(encoded_row)).await;
                        }
                        Err(err) => {
                            Self::submit_response_async(
                                &sender,
                                StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { source: err }),
                            )
                            .await;
                            return;
                        }
                    }
                }
                Self::submit_response_async(&sender, StreamQueryResponse::done_ok()).await
            })
        })
    }

    fn blocking_read_query_worker(&self, pipeline: Pipeline, sender: Sender<StreamQueryResponse>) -> JoinHandle<()> {
        debug_assert!(
            self.request_queue.is_empty() && self.running_write_query.is_none() && self.transaction.is_some()
        );
        let mut interrupt = self.query_interrupt_receiver.clone();
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let function_manager = transaction.function_manager.clone();
            spawn_blocking(move || {
                let prepare_result = Self::prepare_read_query_in(
                    snapshot.clone(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &pipeline,
                );

                let (executor, named_outputs) = unwrap_or_execute_and_return!(prepare_result, |err| {
                    Self::submit_response_sync(&sender, StreamQueryResponse::done_err(err));
                });

                let descriptor: StreamQueryOutputDescriptor =
                    named_outputs.into_iter().map(|(name, position)| (name, position)).sorted().collect();
                let response = StreamQueryResponse::init_ok(&descriptor);
                Self::submit_response_sync(&sender, response);

                let (mut iterator, _) =
                    unwrap_or_execute_and_return!(executor.into_iterator(interrupt.clone()), |(err, _)| {
                        Self::submit_response_sync(
                            &sender,
                            StreamQueryResponse::done_err(QueryError::ReadPipelineExecutionError {
                                typedb_source: err,
                            }),
                        );
                    });

                while let Some(next) = iterator.next() {
                    if interrupt.check() {
                        Self::submit_response_sync(
                            &sender,
                            StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted {}),
                        );
                        return;
                    }

                    let row = unwrap_or_execute_and_return!(next, |err| {
                        Self::submit_response_sync(&sender, StreamQueryResponse::done_err(err));
                    });

                    let encoded_row = encode_row(row, &descriptor, snapshot.as_ref(), &type_manager, &thing_manager);
                    match encoded_row {
                        Ok(encoded_row) => {
                            Self::submit_response_sync(&sender, StreamQueryResponse::next_row(encoded_row))
                        }
                        Err(err) => {
                            Self::submit_response_sync(
                                &sender,
                                StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { source: err }),
                            );
                            return;
                        }
                    }
                }
                Self::submit_response_sync(&sender, StreamQueryResponse::done_ok())
            })
        })
    }

    fn prepare_read_query_in<Snapshot: ReadableSnapshot + 'static>(
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline,
    ) -> Result<(ReadPipelineStage<Snapshot>, HashMap<String, VariablePosition>), QueryError> {
        QueryManager::new().prepare_read_pipeline(snapshot, type_manager, thing_manager, function_manager, pipeline)
    }

    fn submit_response_sync(sender: &Sender<StreamQueryResponse>, response: StreamQueryResponse) {
        if let Err(err) = sender.blocking_send(response) {
            event!(Level::ERROR, "Failed to send error message: {:?}", err)
        }
    }

    async fn submit_response_async(sender: &Sender<StreamQueryResponse>, response: StreamQueryResponse) {
        if let Err(err) = sender.send(response).await {
            event!(Level::ERROR, "Failed to send error message: {:?}", err)
        }
    }

    async fn handle_stream_continue(&mut self, request_id: Uuid, _stream_req: Req) -> Option<ImmediateQueryResponse> {
        debug_assert!(
            self.read_responders.contains_key(&request_id) || self.write_responders.contains_key(&request_id)
        );
        let read_responder = self.read_responders.get_mut(&request_id);
        if let Some((worker_handle, stream_transmitter)) = read_responder {
            if stream_transmitter.check_finished_else_queue_continue().await {
                debug_assert!(worker_handle.is_finished());
                self.read_responders.remove(&request_id);
            }
            // valid query stream responses and control are reported by the transmitter directly, so no response here
            None
        } else {
            let write_responder = self.write_responders.get_mut(&request_id);
            if let Some((worker_handle, stream_transmitter)) = write_responder {
                if stream_transmitter.check_finished_else_queue_continue().await {
                    debug_assert!(worker_handle.is_finished());
                    self.write_responders.remove(&request_id);
                }
                // valid query stream responses and control are reported by the transmitter directly, so no response here
                None
            } else {
                // This could be a valid state - if the driver requests Continuing a stream that has already been
                //       finished & removed, or the user committed/rolled back/closed and we cleaned up streams eagerly
                Some(ImmediateQueryResponse::NonFatalErr(
                    TransactionServiceError::QueryStreamNotFound { query_request_id: request_id }.into_error_message(),
                ))
            }
        }
    }

    fn is_write_pipeline(pipeline: &Pipeline) -> bool {
        for stage in &pipeline.stages {
            match stage {
                Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => return true,
                Stage::Fetch(_) | Stage::Reduce(_) | Stage::Modifier(_) | Stage::Match(_) => {}
            }
        }
        false
    }
}

#[derive(Debug)]
struct QueryStreamTransmitter {
    response_sender: Sender<Result<Server, Status>>,
    req_id: Uuid,
    prefetch_size: usize,
    network_latency_millis: usize,

    transmitter_task: Option<JoinHandle<ControlFlow<(), Receiver<StreamQueryResponse>>>>,
}

impl QueryStreamTransmitter {
    fn start_new(
        response_sender: Sender<Result<Server, Status>>,
        query_response_receiver: Receiver<StreamQueryResponse>,
        req_id: Uuid,
        prefetch_size: usize,
        network_latency_millis: usize,
    ) -> Self {
        let transmitter_task = tokio::spawn(Self::respond_stream_parts(
            response_sender.clone(),
            prefetch_size,
            network_latency_millis,
            req_id,
            query_response_receiver,
        ));
        Self {
            response_sender,
            req_id,
            prefetch_size,
            network_latency_millis,
            transmitter_task: Some(transmitter_task),
        }
    }

    async fn check_finished_else_queue_continue(&mut self) -> bool {
        let task = self.transmitter_task.take().unwrap();
        if task.is_finished() {
            // Should be immediate and safe to unwrap: cancelled or panicked are the only cases
            let control = task.await.unwrap();
            match control {
                Continue(query_response_receiver) => {
                    self.transmitter_task = Some(tokio::spawn(Self::respond_stream_parts(
                        self.response_sender.clone(),
                        self.prefetch_size,
                        self.network_latency_millis,
                        self.req_id,
                        query_response_receiver,
                    )));
                    false
                }
                Break(_) => true,
            }
        } else {
            let sender = self.response_sender.clone();
            let prefetch = self.prefetch_size;
            let latency = self.network_latency_millis;
            let req_id = self.req_id;
            // append another parts responding operation to run once the existing one has finished
            self.transmitter_task = Some(tokio::spawn(async move {
                let control = task.await.unwrap();
                match control {
                    Continue(query_response_receiver) => Self::respond_stream_parts(
                        sender,
                        prefetch,
                        latency,
                        req_id,
                        query_response_receiver
                    ).await,
                    Break(()) => Break(())
                }
            }));
            false
        }
    }

    async fn finish_current(self) {
        if let Some(task) = self.transmitter_task {
            let _ = task.await;
        }
    }

    async fn respond_stream_parts(
        response_sender: Sender<Result<typedb_protocol::transaction::Server, Status>>,
        prefetch_size: usize,
        network_latency_millis: usize,
        req_id: Uuid,
        query_response_receiver: Receiver<StreamQueryResponse>,
    ) -> ControlFlow<(), Receiver<StreamQueryResponse>> {
        // stream PREFETCH answers in one big message (increases message throughput. Note: tested in Java impl)
        let query_response_receiver = Self::respond_stream_while_or_finish(
            &response_sender,
            req_id,
            query_response_receiver,
            StreamingCondition::Count(prefetch_size),
        )
        .await?;
        send_ok_message_else_return_break!(response_sender, transaction_server_res_part_stream_signal_continue(req_id));

        // stream LATENCY number of answers
        let query_response_receiver = Self::respond_stream_while_or_finish(
            &response_sender,
            req_id,
            query_response_receiver,
            StreamingCondition::Duration(Instant::now(), network_latency_millis),
        )
        .await?;
        Continue(query_response_receiver)
    }

    async fn respond_stream_while_or_finish(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        mut query_response_receiver: Receiver<StreamQueryResponse>,
        streaming_condition: StreamingCondition,
    ) -> ControlFlow<(), Receiver<StreamQueryResponse>> {
        let mut rows: Vec<typedb_protocol::ConceptRow> = Vec::new();
        let mut iteration = 0;
        while streaming_condition.continue_(iteration) {
            match query_response_receiver.recv().await {
                None => {
                    send_ok_message_else_return_break!(
                        response_sender,
                        transaction_server_res_part_stream_signal_error(
                            req_id,
                            QueryError::QueryExecutionClosedEarly {}.into_error_message()
                        )
                    );
                    return Break(());
                }
                Some(response) => match response {
                    StreamQueryResponse::InitOk(res) => {
                        // header ok
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_query_res(req_id, query_initial_res_from_query_res_ok(res))
                        );
                    }
                    StreamQueryResponse::InitErr(res_error) => {
                        // header error
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_query_res(req_id, query_initial_res_from_error(res_error))
                        );
                        return Break(());
                    }
                    StreamQueryResponse::StreamDoneOk() => {
                        if let Break(()) = Self::send_rows(response_sender, req_id, rows).await {
                            return Break(());
                        }
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_part_stream_signal_done(req_id)
                        );
                        return Break(());
                    }
                    StreamQueryResponse::StreamDoneErr(res_error) => {
                        if let Break(()) = Self::send_rows(response_sender, req_id, rows).await {
                            return Break(());
                        }
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_part_stream_signal_error(req_id, res_error)
                        );
                        return Break(());
                    }
                    StreamQueryResponse::StreamNextRow(concept_row) => rows.push(concept_row),
                },
            }
            iteration += 1;
        }
        match Self::send_rows(response_sender, req_id, rows).await {
            Continue(_) => return Continue(query_response_receiver),
            Break(_) => return Break(()),
        }
    }

    async fn send_rows(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        rows: Vec<typedb_protocol::ConceptRow>,
    ) -> ControlFlow<(), ()> {
        if !rows.is_empty() {
            send_ok_message_else_return_break!(
                response_sender,
                transaction_server_res_parts_query_part(req_id, query_res_part_from_concept_rows(rows))
            );
        }
        Continue(())
    }
}

trait ControlFlowExt<B, C> {
    fn map<T, F: FnOnce(B) -> T>(self, f: F) -> ControlFlow<T, C>;
}

impl<B, C> ControlFlowExt<B, C> for ControlFlow<B, C> {
    fn map<T, F: FnOnce(B) -> T>(self, f: F) -> ControlFlow<T, C> {
        match self {
            Continue(c) => Continue(c),
            Break(b) => Break(f(b)),
        }
    }
}

trait ControlFlowResultExt<T, E> {
    fn transpose(self) -> ControlFlow<Result<T, E>>;
}

impl<T, E> ControlFlowResultExt<T, E> for Result<ControlFlow<T>, E> {
    fn transpose(self) -> ControlFlow<Result<T, E>> {
        match self {
            Ok(Continue(())) => Continue(()),
            Ok(Break(t)) => Break(Ok(t)),
            Err(err) => Break(Err(err)),
        }
    }
}

trait ResultControlFlowExt<T, E> {
    fn transpose(self) -> Result<ControlFlow<T>, E>;
}

impl<T, E> ResultControlFlowExt<T, E> for ControlFlow<Result<T, E>> {
    fn transpose(self) -> Result<ControlFlow<T>, E> {
        match self {
            Continue(()) => Ok(Continue(())),
            Break(Ok(t)) => Ok(Break(t)),
            Break(Err(err)) => Err(err),
        }
    }
}

typedb_error!(
    pub(crate) TransactionServiceError(component = "Transaction service", prefix = "TSV") {
        DatabaseNotFound(1, "Database '{name}' not found.", name: String),
        CannotCommitReadTransaction(2, "Read transactions cannot be committed."),
        CannotRollbackReadTransaction(3, "Read transactions cannot be rolled back, since they never contain writes."),
        // TODO: these should be typedb_source
        DataCommitFailed(4, "Data transaction commit failed.", ( typedb_source: DataCommitError )),
        SchemaCommitFailed(5, "Schema transaction commit failed.", ( source : SchemaCommitError )),
        QueryParseFailed(6, "Query parsing failed.", ( typedb_source: typeql::Error )),
        SchemaQueryRequiresSchemaTransaction(7, "Schema modification queries require schema transactions."),
        WriteQueryRequiresSchemaOrWriteTransaction(8, "Data modification queries require either write or schema transactions."),
        TxnAbortSchemaQueryFailed(9, "Aborting transaction due to failed schema query.", ( typedb_source : QueryError )),
        NoOpenTransaction(10, "Operation failed - no open transaction."),
        QueryInterrupted(11, "Query was interrupted by a transaction close, rollback, or commit."),
        QueryStreamNotFound(
            12,
            r#"
            Query stream with id '{query_request_id}' was not found in the transaction.
            The stream could have already finished, or the transaction could be closed, committed, rolled back (or this is a bug).
            "#,
            query_request_id: Uuid
        ),
        ServiceClosingFailedQueueCleanup(13, "The operation failed since the service is closing."),
    }
);
