/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, VecDeque},
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
    time::Instant,
};

use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::{
    database_manager::DatabaseManager,
    transaction::{
        DataCommitError, SchemaCommitError, TransactionError, TransactionRead, TransactionSchema, TransactionWrite,
    },
};
use error::typedb_error;
use executor::{
    batch::Batch,
    pipeline::{
        pipeline::Pipeline,
        stage::{ExecutionContext, ReadPipelineStage, StageIterator},
        PipelineExecutionError,
    },
    ExecutionInterrupt, InterruptType,
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
use typedb_protocol::{
    query::Type::{Read, Write},
    transaction::{stream_signal::Req, Server},
};
use typeql::{
    parse_query,
    query::{stage::Stage, SchemaQuery},
    Query,
};
use uuid::Uuid;

use crate::service::{
    document::encode_document,
    error::{IntoGRPCStatus, IntoProtocolErrorMessage, ProtocolError},
    response_builders::transaction::{
        query_initial_res_from_error, query_initial_res_from_query_res_ok, query_initial_res_ok_from_query_res_ok_ok,
        query_res_ok_concept_document_stream, query_res_ok_concept_row_stream, query_res_ok_done,
        query_res_part_from_concept_documents, query_res_part_from_concept_rows, transaction_open_res,
        transaction_server_res_part_stream_signal_continue, transaction_server_res_part_stream_signal_done,
        transaction_server_res_part_stream_signal_error, transaction_server_res_parts_query_part,
        transaction_server_res_query_res,
    },
    row::encode_row,
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
    query_interrupt_sender: broadcast::Sender<InterruptType>,
    query_interrupt_receiver: ExecutionInterrupt,

    transaction_timeout_millis: Option<u64>,
    schema_lock_acquire_timeout_millis: Option<u64>,
    prefetch_size: Option<u64>,
    network_latency_millis: Option<u64>,

    is_open: bool,
    transaction: Option<Transaction>,
    request_queue: VecDeque<(Uuid, typeql::query::Pipeline)>,
    responders: HashMap<Uuid, (JoinHandle<()>, QueryStreamTransmitter)>,
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
    StreamNextDocument(typedb_protocol::ConceptDocument),
    StreamDoneOk(),
    StreamDoneErr(typedb_protocol::Error),
}

impl StreamQueryResponse {
    fn init_ok_rows(columns: &StreamQueryOutputDescriptor, query_type: typedb_protocol::query::Type) -> Self {
        let columns = columns.iter().map(|(name, _)| name.to_string()).collect();
        let message = query_res_ok_concept_row_stream(columns, query_type);
        Self::InitOk(query_initial_res_ok_from_query_res_ok_ok(message))
    }

    fn init_ok_documents(query_type: typedb_protocol::query::Type) -> Self {
        let message = query_res_ok_concept_document_stream(query_type);
        Self::InitOk(query_initial_res_ok_from_query_res_ok_ok(message))
    }

    fn init_err(error: impl IntoProtocolErrorMessage) -> Self {
        Self::InitErr(error.into_error_message())
    }

    fn next_row(row: typedb_protocol::ConceptRow) -> Self {
        Self::StreamNextRow(row)
    }

    fn next_document(document: typedb_protocol::ConceptDocument) -> Self {
        Self::StreamNextDocument(document)
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
            StreamingCondition::Count(count) => iteration < *count,
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
            request_queue: VecDeque::with_capacity(20),
            responders: HashMap::new(),
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
                        self.transaction = Some(transaction);
                        if let Err(status) = self.transmit_write_results(req_id, result) {
                            Err(status)
                        } else {
                            self.may_accept_from_queue().await;
                            Ok(Continue(()))
                        }
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
                println!("Received query request: {}", query_req.query);
                self.handle_query(request_id, query_req).await
            }
            (true, typedb_protocol::transaction::req::Req::StreamReq(stream_req)) => {
                match self.handle_stream_continue(request_id, stream_req).await {
                    None => Ok(Continue(())),
                    Some(query_response) => {
                        Ok(Self::respond_query_response(&self.response_sender, request_id, query_response).await)
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
                Transaction::Read(TransactionRead::open(database, transaction_options).map_err(|typedb_source| {
                    TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                })?)
            }
            typedb_protocol::transaction::Type::Write => {
                Transaction::Write(TransactionWrite::open(database, transaction_options).map_err(|typedb_source| {
                    TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                })?)
            }
            typedb_protocol::transaction::Type::Schema => Transaction::Schema(
                TransactionSchema::open(database, transaction_options).map_err(|typedb_source| {
                    TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                })?,
            ),
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
        self.finish_running_write_query_no_transmit(InterruptType::TransactionCommitted).await?;

        // interrupt active queries and close write transmitters
        self.interrupt_and_close_responders(InterruptType::TransactionCommitted).await;
        if let Break(()) = self.cancel_queued_read_queries(InterruptType::TransactionCommitted).await {
            return Err(TransactionServiceError::ServiceClosingFailedQueueCleanup {}
                .into_error_message()
                .into_status());
        }

        // finish executing any remaining writes so they make it into the commit
        self.finish_queued_write_queries(InterruptType::TransactionCommitted).await?;

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
            Transaction::Schema(transaction) => transaction.commit().map_err(|typedb_source| {
                TransactionServiceError::SchemaCommitFailed { typedb_source }.into_error_message().into_status()
            }),
        }
    }

    async fn handle_rollback(
        &mut self,
        _rollback_req: typedb_protocol::transaction::rollback::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {
        // interrupt all queries, cancel writes, then rollback
        self.interrupt_and_close_responders(InterruptType::TransactionRolledback).await;
        if let Break(_) = self.cancel_queued_read_queries(InterruptType::TransactionRolledback).await {
            return Ok(Break(()));
        }

        self.finish_running_write_query_no_transmit(InterruptType::TransactionRolledback).await?;
        if let Break(()) = self.cancel_queued_write_queries(InterruptType::TransactionRolledback).await {
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
        self.interrupt_and_close_responders(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_read_queries(InterruptType::TransactionClosed).await;
        let _ = self.finish_running_write_query_no_transmit(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_write_queries(InterruptType::TransactionClosed).await;

        match self.transaction.take() {
            None => (),
            Some(Transaction::Read(transaction)) => transaction.close(),
            Some(Transaction::Write(transaction)) => transaction.close(),
            Some(Transaction::Schema(transaction)) => transaction.close(),
        };
    }

    async fn interrupt_and_close_responders(&mut self, interrupt: InterruptType) {
        self.query_interrupt_sender.send(interrupt).unwrap();
        for (_, (worker, mut transmitter)) in self.responders.drain() {
            // WARNING: we cannot await the worker to finish first - it's a blocking task that could catch the interrupt
            // or be waiting for the queue to unblock as the transmitter task is done. So, we should first drain some answers from the transmitter
            // then wait for the worker to catch the interrupt signal
            transmitter.check_finished_else_queue_continue().await;
            if let Err(err) = worker.await {
                event!(Level::DEBUG, "Awaiting query worker returned error: {:?}", err);
            }
            transmitter.finish_current().await;
        }
    }

    async fn cancel_queued_read_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        let mut write_queries = VecDeque::with_capacity(self.request_queue.len());
        for (req_id, pipeline) in self.request_queue.drain(0..self.request_queue.len()) {
            if Self::is_write_pipeline(&pipeline) {
                write_queries.push_back((req_id, pipeline));
            }
            Self::respond_query_response(
                &self.response_sender,
                req_id,
                ImmediateQueryResponse::NonFatalErr(
                    TransactionServiceError::QueryInterrupted { interrupt }.into_error_message(),
                ),
            )
            .await?;
        }

        self.request_queue = write_queries;
        Continue(())
    }

    async fn finish_running_write_query_no_transmit(&mut self, interrupt: InterruptType) -> Result<(), Status> {
        if let Some((req_id, worker)) = self.running_write_query.take() {
            let (transaction, result) = worker.await.unwrap();
            self.transaction = Some(transaction);

            if let Err(err) = result {
                return Err(err.into_error_message().into_status());
            }

            // transmission of interrupt signal is ok if it fails
            match Self::respond_query_response(
                &self.response_sender,
                req_id,
                ImmediateQueryResponse::NonFatalErr(
                    TransactionServiceError::QueryInterrupted { interrupt }.into_error_message(),
                ),
            )
            .await
            {
                Continue(_) => Ok(()),
                Break(_) => Err(ProtocolError::FailedQueryResponse {}.into_status()),
            }
        } else {
            Ok(())
        }
    }

    fn transmit_write_results(
        &mut self,
        req_id: Uuid,
        result: Result<(StreamQueryOutputDescriptor, Batch), QueryError>,
    ) -> Result<(), Status> {
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

    async fn cancel_queued_write_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        let mut read_queries = VecDeque::with_capacity(self.request_queue.len());
        for (req_id, pipeline) in self.request_queue.drain(0..self.request_queue.len()) {
            if Self::is_write_pipeline(&pipeline) {
                Self::respond_query_response(
                    &self.response_sender,
                    req_id,
                    ImmediateQueryResponse::NonFatalErr(
                        TransactionServiceError::QueryInterrupted { interrupt }.into_error_message(),
                    ),
                )
                .await?;
            } else {
                read_queries.push_back((req_id, pipeline));
            }
        }
        self.request_queue = read_queries;
        Continue(())
    }

    async fn finish_queued_write_queries(&mut self, interrupt: InterruptType) -> Result<(), Status> {
        self.finish_running_write_query_no_transmit(interrupt).await?;
        let requests: Vec<_> = self.request_queue.drain(0..self.request_queue.len()).collect();
        for (req_id, pipeline) in requests.into_iter() {
            if Self::is_write_pipeline(&pipeline) {
                self.run_write_query(req_id, pipeline).await;
                self.finish_running_write_query_no_transmit(interrupt).await?;
            } else {
                self.request_queue.push_back((req_id, pipeline));
            }
        }
        Ok(())
    }

    async fn may_accept_from_queue(&mut self) {
        debug_assert!(self.running_write_query.is_none());

        // unblock requests until the first write request, which we begin executing if it exists
        while let Some((req_id, query_pipeline)) = self.request_queue.pop_front() {
            if Self::is_write_pipeline(&query_pipeline) {
                self.run_write_query(req_id, query_pipeline).await;
                return;
            } else {
                self.run_and_activate_read_transmitter(req_id, query_pipeline);
            }
        }
    }

    async fn handle_query(
        &mut self,
        req_id: Uuid,
        query_req: typedb_protocol::query::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {

        let _query_options = &query_req.options; // TODO: pass query options
        let parsed = match parse_query(&query_req.query) {
            Ok(parsed) => parsed,
            Err(err) => {
                let response = ImmediateQueryResponse::non_fatal_err(TransactionServiceError::QueryParseFailed {
                    typedb_source: err,
                });
                return Ok(Self::respond_query_response(&self.response_sender, req_id, response).await);
            }
        };
        match parsed {
            Query::Schema(schema_query) => {
                self.interrupt_and_close_responders(InterruptType::SchemaQueryExecution).await;
                self.cancel_queued_read_queries(InterruptType::SchemaQueryExecution).await;
                self.finish_queued_write_queries(InterruptType::SchemaQueryExecution).await?;

                // schema queries are handled immediately so there is a query response or a fatal Status
                let response = self.handle_query_schema(schema_query).await?;
                Ok(Self::respond_query_response(&self.response_sender, req_id, response).await)
            }
            Query::Pipeline(pipeline) => {
                #[allow(clippy::collapsible_else_if)]
                if Self::is_write_pipeline(&pipeline) {
                    if !self.request_queue.is_empty() || self.running_write_query.is_some() {
                        self.request_queue.push_back((req_id, pipeline));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(Continue(()))
                    } else {
                        self.run_write_query(req_id, pipeline).await;
                        Ok(Continue(()))
                    }
                } else {
                    if !self.request_queue.is_empty() || self.running_write_query.is_some() {
                        self.request_queue.push_back((req_id, pipeline));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(Continue(()))
                    } else {
                        self.run_and_activate_read_transmitter(req_id, pipeline);
                        // running read queries have no response on the main loop and will respond asynchronously
                        Ok(Continue(()))
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
                query_manager,
                database,
                transaction_options,
            } = schema_transaction;
            let mut snapshot = Arc::into_inner(snapshot).unwrap();
            let (snapshot, type_manager, thing_manager, query_manager, function_manager, result) =
                spawn_blocking(move || {
                    let result = query_manager.execute_schema(
                        &mut snapshot,
                        &type_manager,
                        &thing_manager,
                        &function_manager,
                        query,
                    );
                    (snapshot, type_manager, thing_manager, query_manager, function_manager, result)
                })
                .await
                .unwrap();

            let transaction = TransactionSchema::from(
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                query_manager,
                database,
                transaction_options,
            );
            self.transaction = Some(Transaction::Schema(transaction));

            let message_ok_done =
                result.map(|_| query_res_ok_done(typedb_protocol::query::Type::Schema)).map_err(|err| {
                    TransactionServiceError::TxnAbortSchemaQueryFailed { typedb_source: err }
                        .into_error_message()
                        .into_status()
                })?;

            Ok(ImmediateQueryResponse::ok(message_ok_done))
        } else {
            Ok(ImmediateQueryResponse::non_fatal_err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}))
        }
    }

    async fn run_write_query(&mut self, req_id: Uuid, pipeline: typeql::query::Pipeline) {
        debug_assert!(self.running_write_query.is_none());
        self.interrupt_and_close_responders(InterruptType::WriteQueryExecution).await;
        let handle = match self.spawn_blocking_execute_write_query(pipeline) {
            Ok(handle) => {
                // running write queries have no valid response yet (until they finish) and will respond asynchronously
                handle
            }
            Err(err) => {
                // non-fatal errors we will respond immediately
                Self::respond_query_response(&self.response_sender, req_id, ImmediateQueryResponse::non_fatal_err(err))
                    .await;
                return;
            }
        };
        self.running_write_query = Some((req_id, tokio::spawn(async move { handle.await.unwrap() })));
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
        self.responders.insert(req_id, (batch_reader, stream_transmitter));
    }

    fn run_and_activate_read_transmitter(&mut self, req_id: Uuid, pipeline: typeql::query::Pipeline) {
        let (sender, receiver) = channel(self.prefetch_size.unwrap() as usize);
        let worker_handle = self.blocking_read_query_worker(pipeline, sender);
        let stream_transmitter = QueryStreamTransmitter::start_new(
            self.response_sender.clone(),
            receiver,
            req_id,
            self.prefetch_size.unwrap() as usize,
            self.network_latency_millis.unwrap() as usize,
        );
        self.responders.insert(req_id, (worker_handle, stream_transmitter));
    }

    fn spawn_blocking_execute_write_query(
        &mut self,
        pipeline: typeql::query::Pipeline,
    ) -> Result<
        JoinHandle<(Transaction, Result<(StreamQueryOutputDescriptor, Batch), QueryError>)>,
        TransactionServiceError,
    > {
        debug_assert!(self.running_write_query.is_none());
        debug_assert!(self.transaction.is_some());
        let interrupt = self.query_interrupt_receiver.clone();
        match self.transaction.take() {
            Some(Transaction::Schema(schema_transaction)) => Ok(spawn_blocking(move || {
                let TransactionSchema {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    database,
                    transaction_options,
                } = schema_transaction;

                let (snapshot, result) = Self::execute_write_query_in(
                    Arc::into_inner(snapshot).unwrap(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_manager,
                    &pipeline,
                    interrupt,
                );

                let transaction = Transaction::Schema(TransactionSchema::from(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    database,
                    transaction_options,
                ));
                (transaction, result)
            })),
            Some(Transaction::Write(write_transaction)) => Ok(spawn_blocking(move || {
                let duration = Instant::now();

                let TransactionWrite {
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    database,
                    transaction_options,
                } = write_transaction;

                let (snapshot, result) = Self::execute_write_query_in(
                    Arc::into_inner(snapshot).expect("Cannot unwrap Arc<Snapshot>, still in use."),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_manager,
                    &pipeline,
                    interrupt,
                );

                let transaction = Transaction::Write(TransactionWrite::from(
                    Arc::new(snapshot),
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    database,
                    transaction_options,
                ));

                println!("Write query took: {:?}", duration.elapsed());

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
        query_manager: &QueryManager,
        pipeline: &typeql::query::Pipeline,
        interrupt: ExecutionInterrupt,
    ) -> (Snapshot, Result<(StreamQueryOutputDescriptor, Batch), QueryError>) {
        let result =
            query_manager.prepare_write_pipeline(snapshot, type_manager, thing_manager, function_manager, pipeline);
        let (query_output_descriptor, pipeline) = match result {
            Ok(pipeline) => {
                let named_outputs = pipeline.rows_positions().unwrap();
                let named_outputs: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();
                (named_outputs, pipeline)
            }
            Err((snapshot, err)) => return (snapshot, Err(err)),
        };

        let (iterator, snapshot, query_profile) = match pipeline.into_rows_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, profile, .. })) => (iterator, snapshot, profile),
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(QueryError::WritePipelineExecution { typedb_source: err }),
                );
            }
        };

        let result = match iterator.collect_owned() {
            Ok(batch) => (Arc::into_inner(snapshot).unwrap(), Ok((query_output_descriptor, batch))),
            Err(err) => {
                (Arc::into_inner(snapshot).unwrap(), Err(QueryError::WritePipelineExecution { typedb_source: err }))
            }
        };
        if query_profile.is_enabled() {
            event!(Level::INFO, "Write query completed.\n{}", query_profile);
        }
        result
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
                Self::submit_response_async(&sender, StreamQueryResponse::init_ok_rows(&output_descriptor, Write))
                    .await;

                while let Some(row) = as_lending_iter.next() {
                    if let Some(interrupt) = interrupt.check() {
                        Self::submit_response_async(
                            &sender,
                            StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
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

    fn blocking_read_query_worker(
        &self,
        pipeline: typeql::query::Pipeline,
        sender: Sender<StreamQueryResponse>,
    ) -> JoinHandle<()> {
        debug_assert!(
            self.request_queue.is_empty() && self.running_write_query.is_none() && self.transaction.is_some()
        );
        let interrupt = self.query_interrupt_receiver.clone();
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let function_manager = transaction.function_manager.clone();
            let query_manager = transaction.query_manager.clone();
            spawn_blocking(move || {
                let duration = Instant::now();

                let pipeline = Self::prepare_read_query_in(
                    snapshot.clone(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_manager,
                    &pipeline,
                );

                let pipeline = unwrap_or_execute_and_return!(pipeline, |err| {
                    Self::submit_response_sync(&sender, StreamQueryResponse::done_err(err));
                });
                Self::respond_read_query_sync(pipeline, interrupt, &sender, snapshot, &type_manager, thing_manager);

                println!("Read query took: {:?}", duration.elapsed());
            })
        })
    }

    fn respond_read_query_sync<Snapshot: ReadableSnapshot>(
        pipeline: Pipeline<Snapshot, ReadPipelineStage<Snapshot>>,
        mut interrupt: ExecutionInterrupt,
        sender: &Sender<StreamQueryResponse>,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
    ) {
        let query_profile = if pipeline.has_fetch() {
            let initial_response = StreamQueryResponse::init_ok_documents(Read);
            Self::submit_response_sync(sender, initial_response);
            let (iterator, context) =
                unwrap_or_execute_and_return!(pipeline.into_documents_iterator(interrupt.clone()), |(err, _)| {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(QueryError::ReadPipelineExecution { typedb_source: err }),
                    );
                });

            let parameters = context.parameters;
            for next in iterator {
                if let Some(interrupt) = interrupt.check() {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                    );
                    return;
                }

                let document = unwrap_or_execute_and_return!(next, |err| {
                    Self::submit_response_sync(sender, StreamQueryResponse::done_err(err));
                });

                let encoded_document =
                    encode_document(document, snapshot.as_ref(), type_manager, &thing_manager, &parameters);
                match encoded_document {
                    Ok(encoded_document) => {
                        Self::submit_response_sync(sender, StreamQueryResponse::next_document(encoded_document))
                    }
                    Err(err) => {
                        Self::submit_response_sync(
                            sender,
                            StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { source: err }),
                        );
                        return;
                    }
                }
            }
            context.profile
        } else {
            let named_outputs = pipeline.rows_positions().unwrap();
            let descriptor: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();
            let initial_response = StreamQueryResponse::init_ok_rows(&descriptor, Read);
            Self::submit_response_sync(sender, initial_response);

            let (mut iterator, context) =
                unwrap_or_execute_and_return!(pipeline.into_rows_iterator(interrupt.clone()), |(err, _)| {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(QueryError::ReadPipelineExecution { typedb_source: err }),
                    );
                });

            while let Some(next) = iterator.next() {
                if let Some(interrupt) = interrupt.check() {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                    );
                    return;
                }

                let row = unwrap_or_execute_and_return!(next, |err| {
                    Self::submit_response_sync(sender, StreamQueryResponse::done_err(err));
                });

                let encoded_row = encode_row(row, &descriptor, snapshot.as_ref(), type_manager, &thing_manager);
                match encoded_row {
                    Ok(encoded_row) => Self::submit_response_sync(sender, StreamQueryResponse::next_row(encoded_row)),
                    Err(err) => {
                        Self::submit_response_sync(
                            sender,
                            StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { source: err }),
                        );
                        return;
                    }
                }
            }
            context.profile
        };
        if query_profile.is_enabled() {
            event!(Level::INFO, "Read query done (including network request time).\n{}", query_profile);
        }
        Self::submit_response_sync(sender, StreamQueryResponse::done_ok())
    }

    fn prepare_read_query_in<Snapshot: ReadableSnapshot + 'static>(
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query_manager: &QueryManager,
        pipeline: &typeql::query::Pipeline,
    ) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, QueryError> {
        query_manager.prepare_read_pipeline(snapshot, type_manager, thing_manager, function_manager, pipeline)
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
        let responder = self.responders.get_mut(&request_id);
        if let Some((worker_handle, stream_transmitter)) = responder {
            if stream_transmitter.check_finished_else_queue_continue().await {
                debug_assert!(worker_handle.is_finished());
                self.responders.remove(&request_id);
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

    fn is_write_pipeline(pipeline: &typeql::query::Pipeline) -> bool {
        for stage in &pipeline.stages {
            match stage {
                Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => return true,
                Stage::Fetch(_) | Stage::Operator(_) | Stage::Match(_) => {}
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
                    Continue(query_response_receiver) => {
                        Self::respond_stream_parts(sender, prefetch, latency, req_id, query_response_receiver).await
                    }
                    Break(()) => Break(()),
                }
            }));
            false
        }
    }

    async fn finish_current(self) {
        if let Some(task) = self.transmitter_task {
            let result = task.await.unwrap();
            // let (control_flow, query_response_receiver) = task.await.unwrap();
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
        let mut documents: Vec<typedb_protocol::ConceptDocument> = Vec::new();
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
                        if let Break(()) = Self::send_on_stream_done(response_sender, req_id, rows, documents).await {
                            return Break(());
                        }
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_part_stream_signal_done(req_id)
                        );
                        return Break(());
                    }
                    StreamQueryResponse::StreamDoneErr(res_error) => {
                        if let Break(()) = Self::send_on_stream_done(response_sender, req_id, rows, documents).await {
                            return Break(());
                        }
                        send_ok_message_else_return_break!(
                            response_sender,
                            transaction_server_res_part_stream_signal_error(req_id, res_error)
                        );
                        return Break(());
                    }
                    StreamQueryResponse::StreamNextRow(concept_row) => rows.push(concept_row),
                    StreamQueryResponse::StreamNextDocument(concept_document) => documents.push(concept_document),
                },
            }
            iteration += 1;
        }

        debug_assert!(rows.is_empty() || documents.is_empty());
        if !rows.is_empty() {
            match Self::send_rows(response_sender, req_id, rows).await {
                Continue(_) => Continue(query_response_receiver),
                Break(_) => Break(()),
            }
        } else if !documents.is_empty() {
            match Self::send_documents(response_sender, req_id, documents).await {
                Continue(_) => Continue(query_response_receiver),
                Break(_) => Break(()),
            }
        } else {
            Continue(query_response_receiver)
        }
    }

    async fn send_on_stream_done(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        rows: Vec<typedb_protocol::ConceptRow>,
        documents: Vec<typedb_protocol::ConceptDocument>,
    ) -> ControlFlow<(), ()> {
        debug_assert!(rows.is_empty() || documents.is_empty());
        if !rows.is_empty() {
            Self::send_rows(response_sender, req_id, rows).await
        } else if !documents.is_empty() {
            Self::send_documents(response_sender, req_id, documents).await
        } else {
            Continue(())
        }
    }

    async fn send_rows(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        rows: Vec<typedb_protocol::ConceptRow>,
    ) -> ControlFlow<(), ()> {
        debug_assert!(!rows.is_empty());
        send_ok_message_else_return_break!(
            response_sender,
            transaction_server_res_parts_query_part(req_id, query_res_part_from_concept_rows(rows))
        );
        Continue(())
    }

    async fn send_documents(
        response_sender: &Sender<Result<typedb_protocol::transaction::Server, Status>>,
        req_id: Uuid,
        documents: Vec<typedb_protocol::ConceptDocument>,
    ) -> ControlFlow<(), ()> {
        debug_assert!(!documents.is_empty());
        send_ok_message_else_return_break!(
            response_sender,
            transaction_server_res_parts_query_part(req_id, query_res_part_from_concept_documents(documents))
        );
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
        TransactionFailed(4, "Transaction failed.", ( typedb_source: TransactionError )),
        DataCommitFailed(5, "Data transaction commit failed.", ( typedb_source: DataCommitError )),
        SchemaCommitFailed(6, "Schema transaction commit failed.", ( typedb_source : SchemaCommitError )),
        QueryParseFailed(7, "Query parsing failed.", ( typedb_source: typeql::Error )),
        SchemaQueryRequiresSchemaTransaction(8, "Schema modification queries require schema transactions."),
        WriteQueryRequiresSchemaOrWriteTransaction(9, "Data modification queries require either write or schema transactions."),
        TxnAbortSchemaQueryFailed(10, "Aborting transaction due to failed schema query.", ( typedb_source : QueryError )),
        NoOpenTransaction(11, "Operation failed - no open transaction."),
        QueryInterrupted(12, "Execution interrupted by to a concurrent {interrupt}.", interrupt: InterruptType),
        QueryStreamNotFound(
            13,
            r#"
            Query stream with id '{query_request_id}' was not found in the transaction.
            The stream could have already finished, or the transaction could be closed, committed, rolled back (or this is a bug).
            "#,
            query_request_id: Uuid
        ),
        ServiceClosingFailedQueueCleanup(14, "The operation failed since the service is closing."),
    }
);
