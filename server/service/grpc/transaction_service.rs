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
    time::Duration,
};

use compiler::query_structure::PipelineStructure;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::{
    database_manager::DatabaseManager,
    query::{
        execute_schema_query, execute_write_query_in_schema, execute_write_query_in_write, StreamQueryOutputDescriptor,
        WriteQueryAnswer, WriteQueryResult,
    },
    transaction::{
        DataCommitError, DataCommitError::SnapshotError, SchemaCommitError, TransactionRead, TransactionSchema,
        TransactionWrite,
    },
};
use diagnostics::{
    diagnostics_manager::DiagnosticsManager,
    metrics::{ActionKind, ClientEndpoint, LoadKind},
};
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::{pipeline::Pipeline, stage::ReadPipelineStage, PipelineExecutionError},
    ExecutionInterrupt, InterruptType,
};
use ir::pipeline::ParameterRegistry;
use itertools::{Either, Itertools};
use lending_iterator::LendingIterator;
use options::QueryOptions;
use query::error::QueryError;
use resource::profile::{EncodingProfile, QueryProfile, StorageCounters};
use storage::snapshot::{CommittableSnapshot, ReadableSnapshot};
use tokio::{
    spawn,
    sync::{
        broadcast,
        mpsc::{channel, Receiver, Sender},
        watch,
    },
    task::{spawn_blocking, JoinHandle},
    time::{timeout, Instant},
};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::{event, Level};
use typedb_protocol::{
    query::Type::{Read, Write},
    transaction::{stream_signal::Req, Server as ProtocolServer},
};
use typeql::{parse_query, query::SchemaQuery};
use uuid::Uuid;

use crate::{
    service::{
        grpc::{
            diagnostics::run_with_diagnostics_async,
            document::encode_document,
            error::{IntoGrpcStatus, IntoProtocolErrorMessage, ProtocolError},
            options::{query_options_from_proto, transaction_options_from_proto},
            response_builders::transaction::{
                query_initial_res_from_error, query_initial_res_from_query_res_ok,
                query_initial_res_ok_from_query_res_ok_ok, query_res_ok_concept_document_stream,
                query_res_ok_concept_row_stream, query_res_ok_done, query_res_part_from_concept_documents,
                query_res_part_from_concept_rows, transaction_open_res, transaction_server_res_commit_res,
                transaction_server_res_part_stream_signal_continue, transaction_server_res_part_stream_signal_done,
                transaction_server_res_part_stream_signal_error, transaction_server_res_parts_query_part,
                transaction_server_res_query_res, transaction_server_res_rollback_res,
            },
            row::encode_row,
        },
        transaction_service::{
            init_transaction_timeout, is_write_pipeline, with_readable_transaction, Transaction,
            TransactionServiceError,
        },
    },
    state::{BoxServerState, ServerStateError},
};

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

macro_rules! send_ok_message {
    ($response_sender: expr, $message: expr) => {{
        if let Err(err) = $response_sender.send(Ok($message)).await {
            event!(Level::TRACE, "Submit message failed: {:?}", err);
        }
    }};

    ($response_sender: expr, $message: expr, else $expr: expr) => {{
        if let Err(err) = $response_sender.send(Ok($message)).await {
            event!(Level::TRACE, "Submit message failed: {:?}", err);
            $expr;
        }
    }};
}

macro_rules! send_ok_message_else_return_break {
    ($response_sender: expr, $message: expr) => {{
        send_ok_message!($response_sender, $message, else return Break(()))
    }};
}

#[derive(Debug)]
pub(crate) struct TransactionService {
    server_state: Arc<BoxServerState>,

    request_stream: Streaming<typedb_protocol::transaction::Client>,
    response_sender: Sender<Result<ProtocolServer, Status>>,
    query_interrupt_sender: broadcast::Sender<InterruptType>,
    query_interrupt_receiver: ExecutionInterrupt,

    timeout_at: Instant,
    schema_lock_acquire_timeout_millis: Option<u64>,
    network_latency_millis: Option<u64>,

    is_open: bool,
    transaction: Option<Transaction>,
    query_queue: VecDeque<(Uuid, QueryOptions, typeql::query::Pipeline, String)>,
    query_responders: HashMap<Uuid, (JoinHandle<()>, QueryStreamTransmitter)>,
    running_write_query: Option<(Uuid, JoinHandle<(Transaction, WriteQueryResult)>)>,
}

impl TransactionService {
    pub(crate) fn new(
        server_state: Arc<BoxServerState>,
        request_stream: Streaming<typedb_protocol::transaction::Client>,
        response_sender: Sender<Result<ProtocolServer, Status>>,
    ) -> Self {
        let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);

        Self {
            server_state,

            request_stream,
            response_sender,
            query_interrupt_sender,
            query_interrupt_receiver: ExecutionInterrupt::new(query_interrupt_receiver),

            timeout_at: init_transaction_timeout(None),
            schema_lock_acquire_timeout_millis: None,
            network_latency_millis: None,

            is_open: false,
            transaction: None,
            query_queue: VecDeque::with_capacity(20),
            query_responders: HashMap::new(),
            running_write_query: None,
        }
    }

    pub(crate) async fn listen(&mut self) {
        loop {
            let mut shutdown_receiver = self.server_state.shutdown_receiver();
            let result = if let Some((req_id, write_query_worker)) = &mut self.running_write_query {
                tokio::select! { biased;
                    _ = shutdown_receiver.changed() => {
                        event!(Level::TRACE, "Shutdown signal received, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    _ = tokio::time::sleep_until(self.timeout_at) => {
                        event!(Level::TRACE, "Transaction timeout met, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    write_query_result = write_query_worker => {
                        let req_id = *req_id;
                        self.running_write_query = None;
                        let (transaction, result) = write_query_result.expect("Expected write query result");
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
                tokio::select! { biased;
                    _ = shutdown_receiver.changed() => {
                        event!(Level::TRACE, "Shutdown signal received, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    _ = tokio::time::sleep_until(self.timeout_at) => {
                        event!(Level::TRACE, "Transaction timeout met, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    next = self.request_stream.next() => {
                        self.handle_next(next).await
                    }
                }
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
                    let _metadata = request.metadata;
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
                run_with_diagnostics_async(
                    self.server_state.diagnostics_manager().await.clone(),
                    Some(open_req.database.clone()),
                    ActionKind::TransactionOpen,
                    || async {
                        let result = self.handle_open(request_id, open_req).await;
                        match &result {
                            Ok(Continue(_)) => event!(Level::TRACE, "Transaction opened successfully."),
                            Ok(Break(_)) => event!(Level::TRACE, "Transaction open aborted."),
                            Err(status) => event!(Level::TRACE, "Error opening transaction: {}", status),
                        }
                        result
                    },
                )
                .await
            }
            (true, typedb_protocol::transaction::req::Req::OpenReq(_)) => {
                Err(ProtocolError::TransactionAlreadyOpen {}.into_status())
            }
            (true, typedb_protocol::transaction::req::Req::QueryReq(query_req)) => {
                run_with_diagnostics_async(
                    self.server_state.diagnostics_manager().await.clone(),
                    self.get_database_name().map(|name| name.to_owned()),
                    ActionKind::TransactionQuery,
                    || async { self.handle_query(request_id, query_req).await },
                )
                .await
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
                run_with_diagnostics_async(
                    self.server_state.diagnostics_manager().await.clone(),
                    self.get_database_name().map(|name| name.to_owned()),
                    ActionKind::TransactionCommit,
                    || async {
                        // Eagerly executed in main loop
                        self.handle_commit(request_id, commit_req).await?;
                        Ok(Break(()))
                    },
                )
                .await
            }
            (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {
                run_with_diagnostics_async(
                    self.server_state.diagnostics_manager().await.clone(),
                    self.get_database_name().map(|name| name.to_owned()),
                    ActionKind::TransactionRollback,
                    || async { self.handle_rollback(request_id, rollback_req).await },
                )
                .await
            }
            (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                run_with_diagnostics_async(
                    self.server_state.diagnostics_manager().await.clone(),
                    self.get_database_name().map(|name| name.to_owned()),
                    ActionKind::TransactionClose,
                    || async {
                        self.handle_close(close_req).await;
                        Ok(Break(()))
                    },
                )
                .await
            }
            (false, _) => Err(ProtocolError::TransactionClosed {}.into_status()),
        }
    }

    async fn respond_query_response(
        response_sender: &Sender<Result<ProtocolServer, Status>>,
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
        let transaction_options = transaction_options_from_proto(open_req.options);
        let transaction_timeout_millis = transaction_options.transaction_timeout_millis;

        let transaction_type = typedb_protocol::transaction::Type::try_from(open_req.r#type)
            .map_err(|_| ProtocolError::UnrecognisedTransactionType { enum_variant: open_req.r#type }.into_status())?;

        let database_name = open_req.database;
        let database = self.server_state.databases_get(database_name.as_ref()).await.ok_or_else(|| {
            TransactionServiceError::DatabaseNotFound { name: database_name.clone() }.into_error_message().into_status()
        })?;

        let transaction = match transaction_type {
            typedb_protocol::transaction::Type::Read => {
                let transaction = spawn_blocking(move || {
                    TransactionRead::open(database, transaction_options).map_err(|typedb_source| {
                        TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                    })
                })
                .await
                .unwrap()?;
                Transaction::Read(transaction)
            }
            typedb_protocol::transaction::Type::Write => {
                let transaction = spawn_blocking(move || {
                    TransactionWrite::open(database, transaction_options).map_err(|typedb_source| {
                        TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                    })
                })
                .await
                .unwrap()?;
                Transaction::Write(transaction)
            }
            typedb_protocol::transaction::Type::Schema => {
                let transaction = spawn_blocking(move || {
                    TransactionSchema::open(database, transaction_options).map_err(|typedb_source| {
                        TransactionServiceError::TransactionFailed { typedb_source }.into_error_message().into_status()
                    })
                })
                .await
                .unwrap()?;
                Transaction::Schema(transaction)
            }
        };
        self.server_state.diagnostics_manager().await.increment_load_count(
            ClientEndpoint::Grpc,
            &database_name,
            transaction.load_kind(),
        );
        self.transaction = Some(transaction);
        self.timeout_at = init_transaction_timeout(Some(transaction_timeout_millis));
        self.is_open = true;

        let processing_time_millis = Instant::now().duration_since(receive_time).as_millis() as u64;
        send_ok_message!(
            self.response_sender,
            transaction_open_res(req_id, processing_time_millis),
            else return Ok(Break(()))
        );
        Ok(Continue(()))
    }

    async fn handle_commit(
        &mut self,
        req_id: Uuid,
        _commit_req: typedb_protocol::transaction::commit::Req,
    ) -> Result<(), Status> {
        // finish any running write query, interrupt running queries, clear all running/queued reads, finish all writes
        //   note: if any write query errors, the whole transaction errors
        // finish any active write query
        self.finish_running_write_query_no_transmit(InterruptType::TransactionCommitted).await?;

        // interrupt active queries and close write transmitters
        self.interrupt_and_close_responders(InterruptType::TransactionCommitted).await;
        if let Break(()) = self.cancel_queued_read_queries(InterruptType::TransactionCommitted).await {
            return Err(TransactionServiceError::ServiceFailedQueueCleanup {}.into_error_message().into_status());
        }

        // finish executing any remaining writes so they make it into the commit
        self.finish_queued_write_queries(InterruptType::TransactionCommitted).await?;

        let diagnostics_manager = self.server_state.diagnostics_manager().await.clone();
        let server_state = self.server_state.clone();
        match self.transaction.take().expect("Expected existing transaction") {
            Transaction::Read(transaction) => {
                self.transaction = Some(Transaction::Read(transaction));
                Err(TransactionServiceError::CannotCommitReadTransaction {}.into_error_message().into_status())
            }
            Transaction::Write(transaction) => spawn(async move {
                diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Grpc,
                    transaction.database.name(),
                    LoadKind::WriteTransactions,
                );
                let (mut profile, into_commit_record_result) = match transaction.finalise() {
                    (mut profile, Ok((database, snapshot))) => {
                        let into_commit_record_result = snapshot
                            .finalise(profile.commit_profile())
                            .map(|commit_record_opt| (database, commit_record_opt))
                            .map_err(|error| DataCommitError::SnapshotError { typedb_source: error });
                        (profile, into_commit_record_result)
                    }
                    (profile, Err(error)) => (profile, Err(error)),
                };

                let (profile, commit_result) = match into_commit_record_result {
                    Ok((database, Some(commit_record))) => {
                        let commit_result = server_state
                            .database_data_commit(database.name(), commit_record, profile.commit_profile())
                            .await;
                        (profile, commit_result)
                    }
                    Ok((_, None)) => (profile, Ok(())),
                    Err(error) => (profile, Err(ServerStateError::DatabaseDataCommitFailed { typedb_source: error })),
                };

                if profile.is_enabled() {
                    event!(Level::INFO, "commit done.\n{}", profile);
                }
                commit_result.map_err(|typedb_source| {
                    TransactionServiceError::DataCommitFailed { typedb_source }.into_error_message().into_status()
                })
            })
            .await
            .expect("Expected write transaction commit completion"),
            Transaction::Schema(transaction) => spawn(async move {
                diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Grpc,
                    transaction.database.name(),
                    LoadKind::SchemaTransactions,
                );
                let (mut profile, into_commit_record_result) = match transaction.finalise() {
                    (mut profile, Ok((database, snapshot))) => {
                        let into_commit_record_result = snapshot
                            .finalise(profile.commit_profile())
                            .map(|commit_record_opt| (database, commit_record_opt))
                            .map_err(|error| SchemaCommitError::SnapshotError { typedb_source: error });
                        (profile, into_commit_record_result)
                    }
                    (profile, Err(error)) => (profile, Err(error)),
                };

                let (profile, commit_result) = match into_commit_record_result {
                    Ok((database, Some(commit_record))) => {
                        let commit_result = server_state
                            .database_schema_commit(database.name(), commit_record, profile.commit_profile())
                            .await;
                        (profile, commit_result)
                    }
                    Ok((_, None)) => (profile, Ok(())),
                    Err(error) => (profile, Err(ServerStateError::DatabaseSchemaCommitFailed { typedb_source: error })),
                };

                if profile.is_enabled() {
                    event!(Level::INFO, "commit done.\n{}", profile);
                }
                commit_result.map_err(|typedb_source| {
                    TransactionServiceError::SchemaCommitFailed { typedb_source }.into_error_message().into_status()
                })
            })
            .await
            .expect("Expected schema transaction commit completion"),
        }?;

        send_ok_message!(
            self.response_sender,
            transaction_server_res_commit_res(req_id, typedb_protocol::transaction::commit::Res {})
        );
        Ok(())
    }

    async fn handle_rollback(
        &mut self,
        req_id: Uuid,
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

        match self.transaction.take().expect("Expected existing transaction") {
            Transaction::Read(transaction) => {
                self.transaction = Some(Transaction::Read(transaction));
                return Err(TransactionServiceError::CannotRollbackReadTransaction {}
                    .into_error_message()
                    .into_status());
            }
            Transaction::Write(mut transaction) => {
                transaction.rollback();
                self.transaction = Some(Transaction::Write(transaction));
            }
            Transaction::Schema(mut transaction) => {
                transaction.rollback();
                self.transaction = Some(Transaction::Schema(transaction));
            }
        };

        send_ok_message!(
            self.response_sender,
            transaction_server_res_rollback_res(req_id, typedb_protocol::transaction::rollback::Res {}),
            else return Ok(Break(()))
        );
        Ok(Continue(()))
    }

    async fn handle_close(&mut self, _close_req: typedb_protocol::transaction::close::Req) {
        self.do_close().await;
    }

    async fn do_close(&mut self) {
        self.interrupt_and_close_responders(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_read_queries(InterruptType::TransactionClosed).await;
        let _ = self.finish_running_write_query_no_transmit(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_write_queries(InterruptType::TransactionClosed).await;

        match self.transaction.take() {
            None => (),
            Some(Transaction::Read(transaction)) => {
                self.server_state.diagnostics_manager().await.decrement_load_count(
                    ClientEndpoint::Grpc,
                    transaction.database.name(),
                    LoadKind::ReadTransactions,
                );
                transaction.close()
            }
            Some(Transaction::Write(transaction)) => {
                self.server_state.diagnostics_manager().await.decrement_load_count(
                    ClientEndpoint::Grpc,
                    transaction.database.name(),
                    LoadKind::WriteTransactions,
                );
                transaction.close()
            }
            Some(Transaction::Schema(transaction)) => {
                self.server_state.diagnostics_manager().await.decrement_load_count(
                    ClientEndpoint::Grpc,
                    transaction.database.name(),
                    LoadKind::SchemaTransactions,
                );
                transaction.close()
            }
        };
    }

    async fn interrupt_and_close_responders(&mut self, interrupt: InterruptType) {
        self.query_interrupt_sender.send(interrupt).expect("Expected query interrupt to be sent");
        for (_, (worker, mut transmitter)) in self.query_responders.drain() {
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
        let mut write_queries = VecDeque::with_capacity(self.query_queue.len());
        for (req_id, query_options, pipeline, source_query) in self.query_queue.drain(0..self.query_queue.len()) {
            if is_write_pipeline(&pipeline) {
                write_queries.push_back((req_id, query_options, pipeline, source_query));
            } else {
                Self::respond_query_response(
                    &self.response_sender,
                    req_id,
                    ImmediateQueryResponse::NonFatalErr(
                        TransactionServiceError::QueryInterrupted { interrupt }.into_error_message(),
                    ),
                )
                .await?;
            }
        }

        self.query_queue = write_queries;
        Continue(())
    }

    async fn finish_running_write_query_no_transmit(&mut self, interrupt: InterruptType) -> Result<(), Status> {
        if let Some((req_id, worker)) = self.running_write_query.take() {
            let (transaction, result) = worker.await.expect("Expected current write query to finish");
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

    fn transmit_write_results(&mut self, req_id: Uuid, result: WriteQueryResult) -> Result<(), Status> {
        match result {
            Ok(answer) => {
                self.activate_write_transmitter(req_id, answer);
                Ok(())
            }
            Err(err) => {
                // we promote write errors to fatal status errors
                Err(err.into_error_message().into_status())
            }
        }
    }

    async fn cancel_queued_write_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        let mut read_queries = VecDeque::with_capacity(self.query_queue.len());
        for (req_id, query_options, pipeline, source_query) in self.query_queue.drain(0..self.query_queue.len()) {
            if is_write_pipeline(&pipeline) {
                Self::respond_query_response(
                    &self.response_sender,
                    req_id,
                    ImmediateQueryResponse::NonFatalErr(
                        TransactionServiceError::QueryInterrupted { interrupt }.into_error_message(),
                    ),
                )
                .await?;
            } else {
                read_queries.push_back((req_id, query_options, pipeline, source_query));
            }
        }
        self.query_queue = read_queries;
        Continue(())
    }

    async fn finish_queued_write_queries(&mut self, interrupt: InterruptType) -> Result<(), Status> {
        self.finish_running_write_query_no_transmit(interrupt).await?;
        let requests: Vec<_> = self.query_queue.drain(0..self.query_queue.len()).collect();
        for (req_id, query_options, pipeline, source_query) in requests.into_iter() {
            if is_write_pipeline(&pipeline) {
                self.run_write_query(req_id, query_options, pipeline, source_query).await;
                self.finish_running_write_query_no_transmit(interrupt).await?;
            } else {
                self.query_queue.push_back((req_id, query_options, pipeline, source_query));
            }
        }
        Ok(())
    }

    async fn may_accept_from_queue(&mut self) {
        debug_assert!(self.running_write_query.is_none());

        // unblock requests until the first write request, which we begin executing if it exists
        while let Some((req_id, query_options, query_pipeline, source_query)) = self.query_queue.pop_front() {
            if is_write_pipeline(&query_pipeline) {
                self.run_write_query(req_id, query_options, query_pipeline, source_query).await;
                return;
            } else {
                self.run_and_activate_read_transmitter(req_id, query_options, query_pipeline, source_query);
            }
        }
    }

    async fn handle_query(
        &mut self,
        req_id: Uuid,
        query_req: typedb_protocol::query::Req,
    ) -> Result<ControlFlow<(), ()>, Status> {
        let query_options = query_options_from_proto(query_req.options);
        if query_options.prefetch_size < 1 {
            let response = ImmediateQueryResponse::non_fatal_err(TransactionServiceError::InvalidPrefetchSize {
                value: query_options.prefetch_size,
            });
            return Ok(Self::respond_query_response(&self.response_sender, req_id, response).await);
        }

        let query = query_req.query;
        let parsed = match parse_query(&query) {
            Ok(parsed) => parsed,
            Err(err) => {
                let response = ImmediateQueryResponse::non_fatal_err(TransactionServiceError::QueryParseFailed {
                    typedb_source: err,
                });
                return Ok(Self::respond_query_response(&self.response_sender, req_id, response).await);
            }
        };
        match parsed.into_structure() {
            typeql::query::QueryStructure::Schema(schema_query) => {
                // schema queries are handled immediately so there is a query response or a fatal Status
                let response = self.handle_query_schema(schema_query, query).await?;
                Ok(Self::respond_query_response(&self.response_sender, req_id, response).await)
            }
            typeql::query::QueryStructure::Pipeline(pipeline) => {
                #[allow(clippy::collapsible_else_if)]
                if is_write_pipeline(&pipeline) {
                    if !self.query_queue.is_empty() || self.running_write_query.is_some() {
                        self.query_queue.push_back((req_id, query_options, pipeline, query));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(Continue(()))
                    } else {
                        self.run_write_query(req_id, query_options, pipeline, query).await;
                        Ok(Continue(()))
                    }
                } else {
                    if !self.query_queue.is_empty() || self.running_write_query.is_some() {
                        self.query_queue.push_back((req_id, query_options, pipeline, query));
                        // queued queries are not handled yet so there will be no query response yet
                        Ok(Continue(()))
                    } else {
                        self.run_and_activate_read_transmitter(req_id, query_options, pipeline, query);
                        // running read queries have no response on the main loop and will respond asynchronously
                        Ok(Continue(()))
                    }
                }
            }
        }
    }

    async fn handle_query_schema(
        &mut self,
        query: SchemaQuery,
        source_query: String,
    ) -> Result<ImmediateQueryResponse, Status> {
        self.interrupt_and_close_responders(InterruptType::SchemaQueryExecution).await;
        let _ = self.cancel_queued_read_queries(InterruptType::SchemaQueryExecution).await;
        self.finish_queued_write_queries(InterruptType::SchemaQueryExecution).await?;

        if let Some(transaction) = self.transaction.take() {
            match transaction {
                Transaction::Schema(schema_transaction) => {
                    let (transaction, result) =
                        spawn_blocking(move || execute_schema_query(schema_transaction, query, source_query))
                            .await
                            .expect("Expected schema query execution finishing");
                    self.transaction = Some(Transaction::Schema(transaction));
                    let message_ok_done =
                        result.map(|_| query_res_ok_done(typedb_protocol::query::Type::Schema)).map_err(|err| {
                            TransactionServiceError::TxnAbortSchemaQueryFailed { typedb_source: *err }
                                .into_error_message()
                                .into_status()
                        })?;
                    return Ok(ImmediateQueryResponse::ok(message_ok_done));
                }
                transaction => self.transaction = Some(transaction),
            }
        }

        Ok(ImmediateQueryResponse::non_fatal_err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}))
    }

    async fn run_write_query(
        &mut self,
        req_id: Uuid,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
    ) {
        debug_assert!(self.running_write_query.is_none());
        self.interrupt_and_close_responders(InterruptType::WriteQueryExecution).await;
        let handle = match self.spawn_blocking_execute_write_query(query_options, pipeline, source_query) {
            Ok(handle) => {
                // running write queries have no valid response yet (until they finish) and will respond asynchronously
                handle
            }
            Err(err) => {
                // non-fatal errors we will respond immediately
                let _ = Self::respond_query_response(
                    &self.response_sender,
                    req_id,
                    ImmediateQueryResponse::non_fatal_err(err),
                )
                .await;
                return;
            }
        };
        self.running_write_query = Some((req_id, tokio::spawn(async move { handle.await.unwrap() })));
    }

    fn activate_write_transmitter(&mut self, req_id: Uuid, answer: WriteQueryAnswer) {
        let prefetch_size = answer.query_options.prefetch_size;
        let (sender, receiver) = channel(prefetch_size);
        let answer_reader = self.write_query_answer_reader(answer, sender);
        let stream_transmitter = QueryStreamTransmitter::start_new(
            self.response_sender.clone(),
            receiver,
            req_id,
            prefetch_size,
            self.network_latency_millis.unwrap() as usize,
        );
        self.query_responders.insert(req_id, (answer_reader, stream_transmitter));
    }

    fn run_and_activate_read_transmitter(
        &mut self,
        req_id: Uuid,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
    ) {
        let prefetch_size = query_options.prefetch_size;
        let (sender, receiver) = channel(prefetch_size);
        let worker_handle = self.blocking_read_query_worker(sender, query_options, pipeline, source_query);
        let stream_transmitter = QueryStreamTransmitter::start_new(
            self.response_sender.clone(),
            receiver,
            req_id,
            prefetch_size,
            self.network_latency_millis.unwrap() as usize,
        );
        self.query_responders.insert(req_id, (worker_handle, stream_transmitter));
    }

    fn spawn_blocking_execute_write_query(
        &mut self,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
    ) -> Result<JoinHandle<(Transaction, WriteQueryResult)>, TransactionServiceError> {
        debug_assert!(self.running_write_query.is_none());
        debug_assert!(self.transaction.is_some());
        let interrupt = self.query_interrupt_receiver.clone();
        match self.transaction.take() {
            Some(Transaction::Schema(schema_transaction)) => Ok(spawn_blocking(move || {
                let (transaction, result) =
                    execute_write_query_in_schema(schema_transaction, query_options, pipeline, source_query, interrupt);
                (Transaction::Schema(transaction), result)
            })),
            Some(Transaction::Write(write_transaction)) => Ok(spawn_blocking(move || {
                let (transaction, result) =
                    execute_write_query_in_write(write_transaction, query_options, pipeline, source_query, interrupt);
                (Transaction::Write(transaction), result)
            })),
            Some(Transaction::Read(transaction)) => {
                self.transaction = Some(Transaction::Read(transaction));
                Err(TransactionServiceError::WriteQueryRequiresSchemaOrWriteTransaction {})
            }
            None => Err(TransactionServiceError::NoOpenTransaction {}),
        }
    }

    // Write query is already executed, but for simplicity, we convert it to something that conform to the same API as the read path
    fn write_query_answer_reader(
        &self,
        answer: WriteQueryAnswer,
        sender: Sender<StreamQueryResponse>,
    ) -> JoinHandle<()> {
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone_inner();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let timeout_at = self.timeout_at;
            let interrupt = self.query_interrupt_receiver.clone();
            tokio::spawn(async move {
                let encoding_profile = EncodingProfile::new(tracing::enabled!(Level::TRACE));
                match answer.answer {
                    Either::Left((output_descriptor, batch, pipeline_structure)) => {
                        Self::submit_write_query_batch_answer(
                            snapshot,
                            type_manager,
                            thing_manager,
                            output_descriptor,
                            answer.query_options,
                            pipeline_structure,
                            batch,
                            sender,
                            timeout_at,
                            interrupt,
                            encoding_profile.storage_counters(),
                        )
                        .await
                    }
                    Either::Right((parameters, documents)) => {
                        Self::submit_write_query_documents_answer(
                            snapshot,
                            type_manager,
                            thing_manager,
                            parameters,
                            documents,
                            sender,
                            timeout_at,
                            interrupt,
                            encoding_profile.storage_counters(),
                        )
                        .await
                    }
                }
            })
        })
    }

    async fn submit_write_query_batch_answer(
        snapshot: Arc<impl ReadableSnapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        output_descriptor: StreamQueryOutputDescriptor,
        query_options: QueryOptions,
        _pipeline_structure: Option<PipelineStructure>,
        batch: Batch,
        sender: Sender<StreamQueryResponse>,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        storage_counters: StorageCounters,
    ) {
        let mut batch_iterator = batch.into_iterator();
        Self::submit_response_async(&sender, StreamQueryResponse::init_ok_rows(&output_descriptor, Write)).await;

        while let Some(row) = batch_iterator.next() {
            if let Some(interrupt) = interrupt.check() {
                Self::submit_response_async(
                    &sender,
                    StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                )
                .await;
                return;
            }
            if Instant::now() >= timeout_at {
                Self::submit_response_async(
                    &sender,
                    StreamQueryResponse::done_err(TransactionServiceError::TransactionTimeout {}),
                )
                .await;
                return;
            }

            let encoded_row = encode_row(
                row,
                &output_descriptor,
                snapshot.as_ref(),
                &type_manager,
                &thing_manager,
                query_options.include_instance_types,
                storage_counters.clone(),
            );
            match encoded_row {
                Ok(encoded_row) => {
                    Self::submit_response_async(&sender, StreamQueryResponse::next_row(encoded_row)).await;
                }
                Err(err) => {
                    Self::submit_response_async(
                        &sender,
                        StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { typedb_source: err }),
                    )
                    .await;
                    return;
                }
            }
        }
        Self::submit_response_async(&sender, StreamQueryResponse::done_ok()).await
    }

    async fn submit_write_query_documents_answer(
        snapshot: Arc<impl ReadableSnapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        parameters: Arc<ParameterRegistry>,
        documents: Vec<ConceptDocument>,
        sender: Sender<StreamQueryResponse>,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        storage_counters: StorageCounters,
    ) {
        Self::submit_response_async(&sender, StreamQueryResponse::init_ok_documents(Write)).await;

        for document in documents {
            if let Some(interrupt) = interrupt.check() {
                Self::submit_response_async(
                    &sender,
                    StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                )
                .await;
                return;
            }
            if Instant::now() >= timeout_at {
                Self::submit_response_async(
                    &sender,
                    StreamQueryResponse::done_err(TransactionServiceError::TransactionTimeout {}),
                )
                .await;
                return;
            }

            let encoded_document = encode_document(
                document,
                snapshot.as_ref(),
                &type_manager,
                &thing_manager,
                &parameters,
                storage_counters.clone(),
            );
            match encoded_document {
                Ok(encoded_document) => {
                    Self::submit_response_async(&sender, StreamQueryResponse::next_document(encoded_document)).await;
                }
                Err(err) => {
                    Self::submit_response_async(
                        &sender,
                        StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { typedb_source: err }),
                    )
                    .await;
                    return;
                }
            }
        }
        Self::submit_response_async(&sender, StreamQueryResponse::done_ok()).await
    }

    fn blocking_read_query_worker(
        &self,
        sender: Sender<StreamQueryResponse>,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
    ) -> JoinHandle<()> {
        debug_assert!(self.query_queue.is_empty() && self.running_write_query.is_none() && self.transaction.is_some());
        let timeout_at = self.timeout_at;
        let interrupt = self.query_interrupt_receiver.clone();
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone_inner();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let function_manager = transaction.function_manager.clone();
            let query_manager = transaction.query_manager.clone();
            spawn_blocking(move || {
                let start_time = Instant::now();
                let pipeline = query_manager.prepare_read_pipeline(
                    snapshot.clone(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &pipeline,
                    &source_query,
                );
                let pipeline = unwrap_or_execute_and_return!(pipeline, |err| {
                    Self::submit_response_sync(&sender, StreamQueryResponse::done_err(err));
                });
                Self::respond_read_query_sync(
                    query_options,
                    pipeline,
                    &source_query,
                    timeout_at,
                    interrupt,
                    &sender,
                    snapshot,
                    &type_manager,
                    thing_manager,
                    start_time,
                );
            })
        })
    }

    fn respond_read_query_sync<Snapshot: ReadableSnapshot>(
        query_options: QueryOptions,
        pipeline: Pipeline<Snapshot, ReadPipelineStage<Snapshot>>,
        source_query: &str,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        sender: &Sender<StreamQueryResponse>,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        start_time: Instant,
    ) {
        let query_profile: Arc<QueryProfile>;
        let encoding_profile: EncodingProfile;

        if pipeline.has_fetch() {
            let initial_response = StreamQueryResponse::init_ok_documents(Read);
            Self::submit_response_sync(sender, initial_response);
            let (iterator, context) =
                unwrap_or_execute_and_return!(pipeline.into_documents_iterator(interrupt.clone()), |(err, _)| {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(QueryError::ReadPipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source: err,
                        }),
                    );
                });
            query_profile = context.profile;
            encoding_profile = EncodingProfile::new(query_profile.is_enabled());

            let parameters = context.parameters;
            for next in iterator {
                if let Some(interrupt) = interrupt.check() {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                    );
                    return;
                }
                if Instant::now() >= timeout_at {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::TransactionTimeout {}),
                    );
                    return;
                }

                let document = unwrap_or_execute_and_return!(next, |err| {
                    Self::submit_response_sync(sender, StreamQueryResponse::done_err(err));
                });

                let encoded_document = encode_document(
                    document,
                    snapshot.as_ref(),
                    type_manager,
                    &thing_manager,
                    &parameters,
                    encoding_profile.storage_counters(),
                );
                match encoded_document {
                    Ok(encoded_document) => {
                        Self::submit_response_sync(sender, StreamQueryResponse::next_document(encoded_document))
                    }
                    Err(err) => {
                        Self::submit_response_sync(
                            sender,
                            StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { typedb_source: err }),
                        );
                        return;
                    }
                }
            }
        } else {
            let named_outputs = pipeline.rows_positions().unwrap();
            let descriptor: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();
            let initial_response = StreamQueryResponse::init_ok_rows(&descriptor, Read);

            Self::submit_response_sync(sender, initial_response);

            let (mut iterator, context) =
                unwrap_or_execute_and_return!(pipeline.into_rows_iterator(interrupt.clone()), |(err, _)| {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(QueryError::ReadPipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source: err,
                        }),
                    );
                });
            query_profile = context.profile;
            encoding_profile = EncodingProfile::new(query_profile.is_enabled());

            while let Some(next) = iterator.next() {
                if let Some(interrupt) = interrupt.check() {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::QueryInterrupted { interrupt }),
                    );
                    return;
                }
                if Instant::now() >= timeout_at {
                    Self::submit_response_sync(
                        sender,
                        StreamQueryResponse::done_err(TransactionServiceError::TransactionTimeout {}),
                    );
                    return;
                }

                let row = unwrap_or_execute_and_return!(next, |err| {
                    Self::submit_response_sync(sender, StreamQueryResponse::done_err(err));
                });

                let encoded_row = encode_row(
                    row,
                    &descriptor,
                    snapshot.as_ref(),
                    type_manager,
                    &thing_manager,
                    query_options.include_instance_types,
                    encoding_profile.storage_counters(),
                );
                match encoded_row {
                    Ok(encoded_row) => Self::submit_response_sync(sender, StreamQueryResponse::next_row(encoded_row)),
                    Err(err) => {
                        Self::submit_response_sync(
                            sender,
                            StreamQueryResponse::done_err(PipelineExecutionError::ConceptRead { typedb_source: err }),
                        );
                        return;
                    }
                }
            }
        }

        if query_profile.is_enabled() {
            let micros = Instant::now().duration_since(start_time).as_micros();
            event!(
                Level::INFO,
                "Read query done (including network request time) in {} micros.\n{}\n{}",
                micros,
                query_profile,
                encoding_profile
            );
        }
        Self::submit_response_sync(sender, StreamQueryResponse::done_ok())
    }

    fn submit_response_sync(sender: &Sender<StreamQueryResponse>, response: StreamQueryResponse) {
        if let Err(err) = sender.blocking_send(response) {
            event!(Level::DEBUG, "Failed to send error message: {:?}", err)
        }
    }

    async fn submit_response_async(sender: &Sender<StreamQueryResponse>, response: StreamQueryResponse) {
        if let Err(err) = sender.send(response).await {
            event!(Level::DEBUG, "Failed to send error message: {:?}", err)
        }
    }

    async fn handle_stream_continue(&mut self, request_id: Uuid, _stream_req: Req) -> Option<ImmediateQueryResponse> {
        let responder = self.query_responders.get_mut(&request_id);
        if let Some((worker_handle, stream_transmitter)) = responder {
            if stream_transmitter.check_finished_else_queue_continue().await {
                const ALLOWED_CLEANUP_TIME: Duration = Duration::from_secs(60);
                if timeout(ALLOWED_CLEANUP_TIME, worker_handle).await.is_err() {
                    panic!("Query stream {request_id:?} ended but has not responded in over {ALLOWED_CLEANUP_TIME:?}, aborting. (This is a bug!)");
                }
                self.query_responders.remove(&request_id);
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

    fn get_database_name(&self) -> Option<&str> {
        self.transaction.as_ref().map(Transaction::database_name)
    }
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

#[derive(Debug)]
struct QueryStreamTransmitter {
    response_sender: Sender<Result<ProtocolServer, Status>>,
    req_id: Uuid,
    prefetch_size: usize,
    network_latency_millis: usize,

    transmitter_task: Option<JoinHandle<ControlFlow<(), Receiver<StreamQueryResponse>>>>,
}

impl QueryStreamTransmitter {
    fn start_new(
        response_sender: Sender<Result<ProtocolServer, Status>>,
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
            let _result = task.await.unwrap();
        }
    }

    async fn respond_stream_parts(
        response_sender: Sender<Result<ProtocolServer, Status>>,
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
        response_sender: &Sender<Result<ProtocolServer, Status>>,
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
        response_sender: &Sender<Result<ProtocolServer, Status>>,
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
        response_sender: &Sender<Result<ProtocolServer, Status>>,
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
        response_sender: &Sender<Result<ProtocolServer, Status>>,
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
