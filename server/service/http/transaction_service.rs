/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::VecDeque,
    fmt::{self, Debug},
    ops::{
        ControlFlow,
        ControlFlow::{Break, Continue},
    },
    sync::Arc,
};

use compiler::query_structure::QueryStructure;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
};
use diagnostics::{
    diagnostics_manager::DiagnosticsManager,
    metrics::{ClientEndpoint, LoadKind},
};
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::{pipeline::Pipeline, stage::ReadPipelineStage, PipelineExecutionError},
    ExecutionInterrupt, InterruptType,
};
use http::StatusCode;
use ir::pipeline::ParameterRegistry;
use itertools::{Either, Itertools};
use lending_iterator::LendingIterator;
use options::{QueryOptions, TransactionOptions};
use query::error::QueryError;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use tokio::{
    sync::{broadcast, mpsc::Receiver, oneshot, watch},
    task::{spawn_blocking, JoinHandle},
    time::Instant,
};
use tracing::{event, Level};
use typeql::{parse_query, query::SchemaQuery, Query};

use super::message::query::query_structure::encode_query_structure;
use crate::service::{
    http::message::query::{document::encode_document, query_structure::QueryStructureResponse, row::encode_row},
    transaction_service::{
        execute_schema_query, execute_write_query_in_schema, execute_write_query_in_write, init_transaction_timeout,
        is_write_pipeline, prepare_read_query_in, with_readable_transaction, StreamQueryOutputDescriptor, Transaction,
        TransactionServiceError, WriteQueryAnswer, WriteQueryResult,
    },
    QueryType, TransactionType,
};

macro_rules! respond_error_and_return_break {
    ($responder:ident, $error:expr) => {{
        let _ = respond_transaction_response($responder, TransactionServiceResponse::Err($error));
        return Break(());
    }};
}

macro_rules! respond_else_return_break {
    ($responder:ident, $response:expr) => {{
        match respond_transaction_response($responder, $response) {
            Ok(()) => {}
            Err(_) => return Break(()),
        }
    }};
}

macro_rules! check_interrupt_else_respond_error_and_return_break {
    ($interrupt:ident, $responder:ident) => {{
        if let Some(interrupt) = $interrupt.check() {
            respond_error_and_return_break!($responder, TransactionServiceError::QueryInterrupted { interrupt });
        }
    }};
}

macro_rules! check_timeout_else_respond_error_and_return_break {
    ($timeout_at:ident, $responder:ident) => {{
        if Instant::now() >= $timeout_at {
            respond_error_and_return_break!($responder, TransactionServiceError::TransactionTimeout {});
        }
    }};
}

macro_rules! unwrap_or_execute_else_respond_error_and_return_break {
    ($expr:expr, $responder:ident, |$err:pat_param| $err_mapper: block) => {{
        match $expr {
            Ok(result) => result,
            Err($err) => respond_error_and_return_break!($responder, $err_mapper),
        }
    }};
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum TransactionRequest {
    Query(QueryOptions, String),
    Commit,
    Rollback,
    Close,
}

pub(crate) struct TransactionResponder(pub(crate) oneshot::Sender<TransactionServiceResponse>);

impl Debug for TransactionResponder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransactionResponder(..)")
    }
}

fn respond_query_response(
    responder: TransactionResponder,
    response: QueryAnswer,
) -> Result<(), TransactionServiceResponse> {
    respond_transaction_response(responder, TransactionServiceResponse::Query(response))
}

fn respond_transaction_response(
    responder: TransactionResponder,
    response: TransactionServiceResponse,
) -> Result<(), TransactionServiceResponse> {
    let TransactionResponder(sender) = responder;
    match sender.send(response) {
        Ok(()) => Ok(()),
        Err(response) => Err(response),
    }
}

#[derive(Debug)]
pub(crate) struct TransactionService {
    database_manager: Arc<DatabaseManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,

    request_stream: Receiver<(TransactionRequest, TransactionResponder)>,
    query_interrupt_sender: broadcast::Sender<InterruptType>,
    query_interrupt_receiver: ExecutionInterrupt,
    shutdown_receiver: watch::Receiver<()>,

    timeout_at: Instant,
    schema_lock_acquire_timeout_millis: Option<u64>,

    transaction: Option<Transaction>,
    query_queue: VecDeque<(TransactionResponder, QueryOptions, typeql::query::Pipeline, String)>,
    running_write_query: Option<(TransactionResponder, JoinHandle<(Transaction, WriteQueryResult)>)>,
}

#[derive(Debug)]
pub(crate) enum TransactionServiceResponse {
    Ok,
    Query(QueryAnswer),
    Err(TransactionServiceError),
}

#[derive(Debug)]
pub(crate) enum QueryAnswer {
    ResOk(QueryType),
    ResRows((QueryType, Vec<serde_json::Value>, Option<QueryStructureResponse>, Option<QueryAnswerWarning>)),
    ResDocuments((QueryType, Vec<serde_json::Value>, Option<QueryAnswerWarning>)),
}

impl QueryAnswer {
    pub(crate) fn query_type(&self) -> QueryType {
        match self {
            QueryAnswer::ResOk(query_type) => *query_type,
            QueryAnswer::ResRows((query_type, _, _, _)) => *query_type,
            QueryAnswer::ResDocuments((query_type, _, _)) => *query_type,
        }
    }

    pub(crate) fn status_code(&self) -> StatusCode {
        match self {
            QueryAnswer::ResOk(_) => StatusCode::OK,
            QueryAnswer::ResRows((_, _, _, warning)) => match warning {
                None => StatusCode::OK,
                Some(warning) => warning.status_code(),
            },
            QueryAnswer::ResDocuments((_, _, warning)) => match warning {
                None => StatusCode::OK,
                Some(warning) => warning.status_code(),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) enum QueryAnswerWarning {
    ReadResultsLimitExceeded { limit: usize },
    WriteResultsLimitExceeded { limit: usize },
}

impl QueryAnswerWarning {
    pub(crate) fn status_code(&self) -> StatusCode {
        match self {
            QueryAnswerWarning::ReadResultsLimitExceeded { .. } => StatusCode::PARTIAL_CONTENT,
            QueryAnswerWarning::WriteResultsLimitExceeded { .. } => StatusCode::PARTIAL_CONTENT,
        }
    }
}

impl fmt::Display for QueryAnswerWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryAnswerWarning::ReadResultsLimitExceeded { limit } => write!(f, "Read query results limit ({limit}) exceeded. Not all answers are returned."),
            QueryAnswerWarning::WriteResultsLimitExceeded { limit } => write!(f, "Write query results limit ({limit}) exceeded. Not all answers are returned, but all the requested writes are completed.")
        }
    }
}

impl TransactionService {
    pub(crate) fn new(
        database_manager: Arc<DatabaseManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
        request_stream: Receiver<(TransactionRequest, TransactionResponder)>,
        shutdown_receiver: watch::Receiver<()>,
    ) -> Self {
        let (query_interrupt_sender, query_interrupt_receiver) = broadcast::channel(1);
        Self {
            database_manager,
            diagnostics_manager,

            request_stream,
            query_interrupt_sender,
            query_interrupt_receiver: ExecutionInterrupt::new(query_interrupt_receiver),
            shutdown_receiver,

            timeout_at: init_transaction_timeout(None),
            schema_lock_acquire_timeout_millis: None,

            transaction: None,
            query_queue: VecDeque::with_capacity(20),
            running_write_query: None,
        }
    }

    pub(crate) async fn open(
        &mut self,
        type_: TransactionType,
        database_name: String,
        options: TransactionOptions,
    ) -> Result<u64, TransactionServiceError> {
        let receive_time = Instant::now();
        let transaction_timeout_millis = options.transaction_timeout_millis;

        let database = self
            .database_manager
            .database(database_name.as_ref())
            .ok_or_else(|| TransactionServiceError::DatabaseNotFound { name: database_name.clone() })?;

        let transaction = match type_ {
            TransactionType::Read => {
                let transaction = spawn_blocking(move || {
                    TransactionRead::open(database, options)
                        .map_err(|typedb_source| TransactionServiceError::TransactionFailed { typedb_source })
                })
                .await
                .unwrap()?;
                Transaction::Read(transaction)
            }
            TransactionType::Write => {
                let transaction = spawn_blocking(move || {
                    TransactionWrite::open(database, options)
                        .map_err(|typedb_source| TransactionServiceError::TransactionFailed { typedb_source })
                })
                .await
                .unwrap()?;
                Transaction::Write(transaction)
            }
            TransactionType::Schema => {
                let transaction = spawn_blocking(move || {
                    TransactionSchema::open(database, options)
                        .map_err(|typedb_source| TransactionServiceError::TransactionFailed { typedb_source })
                })
                .await
                .unwrap()?;
                Transaction::Schema(transaction)
            }
        };
        self.diagnostics_manager.increment_load_count(ClientEndpoint::Http, &database_name, transaction.to_load_kind());
        self.transaction = Some(transaction);
        self.timeout_at = init_transaction_timeout(Some(transaction_timeout_millis));

        let processing_time_millis = Instant::now().duration_since(receive_time).as_millis() as u64;
        Ok(processing_time_millis)
    }

    pub(crate) async fn listen(&mut self) {
        loop {
            let control = if let Some((_, write_query_worker)) = &mut self.running_write_query {
                tokio::select! { biased;
                    _ = self.shutdown_receiver.changed() => {
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
                        let (responder, _) = self.running_write_query.take().expect("Expected running write query");
                        let (transaction, result) = write_query_result.expect("Expected write query result");
                        self.transaction = Some(transaction);
                        match self.transmit_write_results(responder, result).await {
                            Continue(()) => self.may_accept_from_queue().await,
                            Break(()) => Break(())
                        }
                    }
                    next = self.request_stream.recv() => {
                        self.handle_next(next).await
                    }
                }
            } else {
                tokio::select! { biased;
                    _ = self.shutdown_receiver.changed() => {
                        event!(Level::TRACE, "Shutdown signal received, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    _ = tokio::time::sleep_until(self.timeout_at) => {
                        event!(Level::TRACE, "Transaction timeout met, closing transaction service.");
                        self.do_close().await;
                        return;
                    }
                    next = self.request_stream.recv() => {
                        self.handle_next(next).await
                    }
                }
            };

            match control {
                Continue(()) => (),
                Break(()) => {
                    event!(Level::TRACE, "Stream ended, closing transaction service.");
                    self.do_close().await;
                    return;
                }
            }
        }
    }

    // TODO: any method using `Result<ControlFlow<(), ()>, Status>` should really be `ControlFlow<Result<(), Status>, ()>`
    async fn handle_next(&mut self, next: Option<(TransactionRequest, TransactionResponder)>) -> ControlFlow<(), ()> {
        match next {
            None => Break(()),
            Some((request, response_sender)) => {
                match request {
                    TransactionRequest::Query(query_options, query) => {
                        self.handle_query(query_options, query, response_sender).await
                        // run_with_diagnostics_async(
                        //     self.diagnostics_manager.clone(),
                        //     self.get_database_name().map(|name| name.to_owned()),
                        //     ActionKind::TransactionQuery,
                        //     || async { self.handle_query(query, response_sender).await },
                        // )
                        //     .await
                    }
                    TransactionRequest::Commit => {
                        self.handle_commit(response_sender).await
                        // run_with_diagnostics_async(
                        //     self.diagnostics_manager.clone(),
                        //     self.get_database_name().map(|name| name.to_owned()),
                        //     ActionKind::TransactionCommit,
                        //     || async {
                        //         // Eagerly executed in main loop
                        //         self.handle_commit(response_sender).await?;
                        //         Ok(Break(()))
                        //     },
                        // )
                        //     .await
                    }
                    TransactionRequest::Rollback => {
                        self.handle_rollback(response_sender).await
                        // run_with_diagnostics_async(
                        //     self.diagnostics_manager.clone(),
                        //     self.get_database_name().map(|name| name.to_owned()),
                        //     ActionKind::TransactionRollback,
                        //     || async { self.handle_rollback(response_sender).await },
                        // )
                        //     .await
                    }
                    TransactionRequest::Close => {
                        self.handle_close(response_sender).await
                        // run_with_diagnostics_async(
                        //     self.diagnostics_manager.clone(),
                        //     self.get_database_name().map(|name| name.to_owned()),
                        //     ActionKind::TransactionClose,
                        //     || async {
                        //         self.handle_close(response_sender).await;
                        //         Ok(Break(()))
                        //     },
                        // )
                        //     .await
                    }
                }
            }
        }
    }

    async fn handle_commit(&mut self, responder: TransactionResponder) -> ControlFlow<(), ()> {
        // finish any running write query, interrupt running queries, clear all running/queued reads, finish all writes
        //   note: if any write query errors, the whole transaction errors
        // finish any active write query
        if let Break(()) = self.finish_running_write_query_no_transmit(InterruptType::TransactionCommitted).await {
            return Break(());
        }

        // interrupt active queries
        self.interrupt(InterruptType::TransactionCommitted).await;
        if let Break(()) = self.cancel_queued_read_queries(InterruptType::TransactionCommitted).await {
            respond_error_and_return_break!(responder, TransactionServiceError::ServiceFailedQueueCleanup {});
        }

        // finish executing any remaining writes so they make it into the commit
        if let Break(()) = self.finish_queued_write_queries(InterruptType::TransactionCommitted).await {
            respond_error_and_return_break!(responder, TransactionServiceError::ServiceFailedQueueCleanup {});
        }

        let diagnostics_manager = self.diagnostics_manager.clone();
        match self.transaction.take().expect("Expected existing transaction") {
            Transaction::Read(transaction) => {
                self.transaction = Some(Transaction::Read(transaction));
                respond_error_and_return_break!(responder, TransactionServiceError::CannotCommitReadTransaction {});
            }
            Transaction::Write(transaction) => spawn_blocking(move || {
                diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Http,
                    transaction.database.name(),
                    LoadKind::WriteTransactions,
                );
                unwrap_or_execute_else_respond_error_and_return_break!(
                    transaction.commit().1,
                    responder,
                    |typedb_source| { TransactionServiceError::DataCommitFailed { typedb_source } }
                );
                respond_else_return_break!(responder, TransactionServiceResponse::Ok);
                Break(())
            })
            .await
            .expect("Expected write transaction execution completion"),
            Transaction::Schema(transaction) => {
                diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Http,
                    transaction.database.name(),
                    LoadKind::SchemaTransactions,
                );
                unwrap_or_execute_else_respond_error_and_return_break!(
                    transaction.commit().1,
                    responder,
                    |typedb_source| { TransactionServiceError::SchemaCommitFailed { typedb_source } }
                );
                respond_else_return_break!(responder, TransactionServiceResponse::Ok);
                Break(())
            }
        }
    }

    async fn handle_rollback(&mut self, responder: TransactionResponder) -> ControlFlow<(), ()> {
        // interrupt all queries, cancel writes, then rollback
        self.interrupt(InterruptType::TransactionRolledback).await;
        if let Break(()) = self.cancel_queued_read_queries(InterruptType::TransactionRolledback).await {
            return Break(());
        }
        if let Break(()) = self.finish_running_write_query_no_transmit(InterruptType::TransactionCommitted).await {
            return Break(());
        }
        if let Break(()) = self.cancel_queued_write_queries(InterruptType::TransactionRolledback).await {
            return Break(());
        }

        match self.transaction.take().expect("Expected existing transaction") {
            Transaction::Read(transaction) => {
                self.transaction = Some(Transaction::Read(transaction));
                respond_error_and_return_break!(responder, TransactionServiceError::CannotRollbackReadTransaction {});
            }
            Transaction::Write(mut transaction) => {
                transaction.rollback();
                self.transaction = Some(Transaction::Write(transaction));
                respond_else_return_break!(responder, TransactionServiceResponse::Ok);
                Continue(())
            }
            Transaction::Schema(mut transaction) => {
                transaction.rollback();
                self.transaction = Some(Transaction::Schema(transaction));
                respond_else_return_break!(responder, TransactionServiceResponse::Ok);
                Continue(())
            }
        }
    }

    async fn handle_close(&mut self, responder: TransactionResponder) -> ControlFlow<(), ()> {
        self.do_close().await;
        respond_else_return_break!(responder, TransactionServiceResponse::Ok);
        Break(())
    }

    async fn do_close(&mut self) {
        self.interrupt(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_read_queries(InterruptType::TransactionClosed).await;
        let _ = self.finish_running_write_query_no_transmit(InterruptType::TransactionClosed).await;
        let _ = self.cancel_queued_write_queries(InterruptType::TransactionClosed).await;

        match self.transaction.take() {
            None => (),
            Some(Transaction::Read(transaction)) => {
                self.diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Http,
                    transaction.database.name(),
                    LoadKind::ReadTransactions,
                );
                transaction.close()
            }
            Some(Transaction::Write(transaction)) => {
                self.diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Http,
                    transaction.database.name(),
                    LoadKind::WriteTransactions,
                );
                transaction.close()
            }
            Some(Transaction::Schema(transaction)) => {
                self.diagnostics_manager.decrement_load_count(
                    ClientEndpoint::Http,
                    transaction.database.name(),
                    LoadKind::SchemaTransactions,
                );
                transaction.close()
            }
        }
    }

    async fn interrupt(&mut self, interrupt: InterruptType) {
        self.query_interrupt_sender.send(interrupt).expect("Expected query interrupt to be sent");
    }

    async fn cancel_queued_read_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        let mut write_queries = VecDeque::with_capacity(self.query_queue.len());
        for (responder, query_options, pipeline, source_query) in self.query_queue.drain(0..self.query_queue.len()) {
            if is_write_pipeline(&pipeline) {
                write_queries.push_back((responder, query_options, pipeline, source_query));
            } else {
                respond_else_return_break!(
                    responder,
                    TransactionServiceResponse::Err(TransactionServiceError::QueryInterrupted { interrupt })
                );
            }
        }

        self.query_queue = write_queries;
        Continue(())
    }

    async fn finish_running_write_query_no_transmit(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        if let Some((responder, worker)) = self.running_write_query.take() {
            let (transaction, result) = worker.await.expect("Expected current write query to finish");
            self.transaction = Some(transaction);

            if let Err(typedb_source) = result {
                respond_error_and_return_break!(responder, TransactionServiceError::QueryFailed { typedb_source });
            }

            // transmission of interrupt signal is ok if it fails
            respond_error_and_return_break!(responder, TransactionServiceError::QueryInterrupted { interrupt });
        } else {
            Continue(())
        }
    }

    async fn transmit_write_results(
        &mut self,
        responder: TransactionResponder,
        result: WriteQueryResult,
    ) -> ControlFlow<(), ()> {
        match result {
            Ok(answer) => self.activate_write_transmitter(responder, answer).await,
            Err(typedb_source) => {
                respond_error_and_return_break!(responder, TransactionServiceError::QueryFailed { typedb_source });
            }
        }
    }

    async fn cancel_queued_write_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        let mut read_queries = VecDeque::with_capacity(self.query_queue.len());
        for (responder, query_options, pipeline, source_query) in self.query_queue.drain(0..self.query_queue.len()) {
            if is_write_pipeline(&pipeline) {
                respond_else_return_break!(
                    responder,
                    TransactionServiceResponse::Err(TransactionServiceError::QueryInterrupted { interrupt })
                );
            } else {
                read_queries.push_back((responder, query_options, pipeline, source_query));
            }
        }
        self.query_queue = read_queries;
        Continue(())
    }

    async fn finish_queued_write_queries(&mut self, interrupt: InterruptType) -> ControlFlow<(), ()> {
        self.finish_running_write_query_no_transmit(interrupt).await?;
        let requests: Vec<_> = self.query_queue.drain(0..self.query_queue.len()).collect();
        for (responder, query_options, pipeline, source_query) in requests.into_iter() {
            if is_write_pipeline(&pipeline) {
                if let Break(()) = self.run_write_query(responder, query_options, pipeline, source_query).await {
                    return Break(());
                }
                if let Break(()) = self.finish_running_write_query_no_transmit(interrupt).await {
                    return Break(());
                }
            } else {
                self.query_queue.push_back((responder, query_options, pipeline, source_query));
            }
        }
        Continue(())
    }

    async fn may_accept_from_queue(&mut self) -> ControlFlow<(), ()> {
        debug_assert!(self.running_write_query.is_none());

        // unblock requests until the first write request, which we begin executing if it exists
        while let Some((responder, query_options, query_pipeline, source_query)) = self.query_queue.pop_front() {
            if is_write_pipeline(&query_pipeline) {
                if let Break(()) = self.run_write_query(responder, query_options, query_pipeline, source_query).await {
                    return Break(());
                }
                return Continue(());
            } else {
                self.blocking_read_query_worker(
                    responder,
                    query_options,
                    query_pipeline,
                    source_query,
                    StorageCounters::DISABLED,
                )
                .await
                .expect("Expected read query completion");
            }
        }
        Continue(())
    }

    async fn handle_query(
        &mut self,
        query_options: QueryOptions,
        query: String,
        responder: TransactionResponder,
    ) -> ControlFlow<(), ()> {
        let parsed = match parse_query(&query) {
            Ok(parsed) => parsed,
            Err(err) => {
                let _ = respond_transaction_response(
                    responder,
                    TransactionServiceResponse::Err(TransactionServiceError::QueryParseFailed { typedb_source: err }),
                );
                return Continue(());
            }
        };
        match parsed.into_structure() {
            typeql::query::QueryStructure::Schema(schema_query) => {
                // schema queries are handled immediately so there is a query response or a fatal Status
                match self.handle_query_schema(schema_query, query).await {
                    Ok(response) => {
                        respond_else_return_break!(responder, response);
                        Continue(())
                    }
                    Err(err) => respond_error_and_return_break!(responder, err),
                }
            }
            typeql::query::QueryStructure::Pipeline(pipeline) => {
                #[allow(clippy::collapsible_else_if)]
                if is_write_pipeline(&pipeline) {
                    if !self.query_queue.is_empty() || self.running_write_query.is_some() {
                        self.query_queue.push_back((responder, query_options, pipeline, query));
                        // queued queries are not handled yet so there will be no query response yet
                        Continue(())
                    } else {
                        self.run_write_query(responder, query_options, pipeline, query).await;
                        Continue(())
                    }
                } else {
                    if !self.query_queue.is_empty() || self.running_write_query.is_some() {
                        self.query_queue.push_back((responder, query_options, pipeline, query));
                        // queued queries are not handled yet so there will be no query response yet
                        Continue(())
                    } else {
                        self.blocking_read_query_worker(
                            responder,
                            query_options,
                            pipeline,
                            query,
                            StorageCounters::DISABLED,
                        )
                        .await
                        .expect("Expected read query completion");
                        // running read queries have no response on the main loop and will respond asynchronously
                        Continue(())
                    }
                }
            }
        }
    }

    async fn handle_query_schema(
        &mut self,
        query: SchemaQuery,
        source_query: String,
    ) -> Result<TransactionServiceResponse, TransactionServiceError> {
        self.interrupt(InterruptType::SchemaQueryExecution).await;
        if let Break(()) = self.cancel_queued_read_queries(InterruptType::SchemaQueryExecution).await {
            return Err(TransactionServiceError::ServiceFailedQueueCleanup {});
        }
        if let Break(()) = self.finish_queued_write_queries(InterruptType::SchemaQueryExecution).await {
            return Err(TransactionServiceError::ServiceFailedQueueCleanup {});
        }

        if let Some(transaction) = self.transaction.take() {
            match transaction {
                Transaction::Schema(schema_transaction) => {
                    let (transaction, result) = execute_schema_query(schema_transaction, query, source_query).await;
                    self.transaction = Some(Transaction::Schema(transaction));
                    match result {
                        Ok(_) => return Ok(TransactionServiceResponse::Query(QueryAnswer::ResOk(QueryType::Schema))),
                        Err(err) => {
                            return Err(TransactionServiceError::TxnAbortSchemaQueryFailed { typedb_source: *err })
                        }
                    }
                }
                transaction => self.transaction = Some(transaction),
            }
        }

        Ok(TransactionServiceResponse::Err(TransactionServiceError::SchemaQueryRequiresSchemaTransaction {}))
    }

    async fn run_write_query(
        &mut self,
        responder: TransactionResponder,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
    ) -> ControlFlow<(), ()> {
        debug_assert!(self.running_write_query.is_none());
        self.interrupt(InterruptType::WriteQueryExecution).await;
        match self.spawn_blocking_execute_write_query(query_options, pipeline, source_query) {
            Ok(handle) => {
                // running write queries have no valid response yet (until they finish) and will respond asynchronously
                self.running_write_query = Some((responder, tokio::spawn(async move { handle.await.unwrap() })));
            }
            Err(err) => {
                // non-fatal errors we will respond immediately
                respond_else_return_break!(responder, TransactionServiceResponse::Err(err));
            }
        };
        Continue(())
    }

    async fn activate_write_transmitter(
        &mut self,
        responder: TransactionResponder,
        answer: WriteQueryAnswer,
    ) -> ControlFlow<(), ()> {
        // Write query is already executed, but for simplicity, we convert it to something that conform to the same API as the read path
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let timeout_at = self.timeout_at;
            let interrupt = self.query_interrupt_receiver.clone();
            tokio::spawn(async move {
                match answer.answer {
                    Either::Left((output_descriptor, batch, query_structure)) => {
                        Self::submit_write_query_batch_answer(
                            snapshot,
                            type_manager,
                            thing_manager,
                            answer.query_options,
                            output_descriptor,
                            query_structure,
                            batch,
                            responder,
                            timeout_at,
                            interrupt,
                            StorageCounters::DISABLED,
                        )
                        .await
                    }
                    Either::Right((parameters, documents)) => {
                        Self::submit_write_query_documents_answer(
                            snapshot,
                            type_manager,
                            thing_manager,
                            answer.query_options,
                            parameters,
                            documents,
                            responder,
                            timeout_at,
                            interrupt,
                            StorageCounters::DISABLED,
                        )
                        .await
                    }
                }
            })
            .await
            .expect("Expected write finish")
        })
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
                execute_write_query_in_schema(schema_transaction, query_options, pipeline, source_query, interrupt)
            })),
            Some(Transaction::Write(write_transaction)) => Ok(spawn_blocking(move || {
                execute_write_query_in_write(write_transaction, query_options, pipeline, source_query, interrupt)
            })),
            Some(Transaction::Read(transaction)) => {
                self.transaction = Some(Transaction::Read(transaction));
                Err(TransactionServiceError::WriteQueryRequiresSchemaOrWriteTransaction {})
            }
            None => Err(TransactionServiceError::NoOpenTransaction {}),
        }
    }

    async fn submit_write_query_batch_answer(
        snapshot: Arc<impl ReadableSnapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        query_options: QueryOptions,
        output_descriptor: StreamQueryOutputDescriptor,
        query_structure: Option<QueryStructure>,
        batch: Batch,
        responder: TransactionResponder,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        storage_counters: StorageCounters,
    ) -> ControlFlow<(), ()> {
        let mut result = vec![];
        let mut batch_iterator = batch.into_iterator();
        let mut warning = None;
        let encode_query_structure_result =
            query_structure.as_ref().map(|qs| encode_query_structure(&*snapshot, &type_manager, qs)).transpose();
        let always_taken_blocks = query_structure.map(|qs| qs.parametrised_structure.always_taken_blocks());
        let query_structure_response = match encode_query_structure_result {
            Ok(structure_opt) => structure_opt,
            Err(typedb_source) => {
                respond_error_and_return_break!(
                    responder,
                    TransactionServiceError::PipelineExecution {
                        typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                    }
                );
            }
        };
        while let Some(row) = batch_iterator.next() {
            check_timeout_else_respond_error_and_return_break!(timeout_at, responder);
            check_interrupt_else_respond_error_and_return_break!(interrupt, responder);
            // TODO: Consider multiplicity?
            if let Some(limit) = query_options.answer_count_limit {
                if result.len() >= limit {
                    warning = Some(QueryAnswerWarning::WriteResultsLimitExceeded { limit });
                    break;
                }
            }

            let encoded_row = encode_row(
                row,
                &output_descriptor,
                snapshot.as_ref(),
                &type_manager,
                &thing_manager,
                query_options.include_instance_types,
                storage_counters.clone(),
                always_taken_blocks.as_ref(),
            );
            match encoded_row {
                Ok(encoded_row) => result.push(encoded_row),
                Err(typedb_source) => {
                    respond_error_and_return_break!(
                        responder,
                        TransactionServiceError::PipelineExecution {
                            typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                        }
                    );
                }
            }
        }
        match respond_query_response(
            responder,
            QueryAnswer::ResRows((QueryType::Write, result, query_structure_response, warning)),
        ) {
            Ok(_) => Continue(()),
            Err(_) => Break(()),
        }
    }

    async fn submit_write_query_documents_answer(
        snapshot: Arc<impl ReadableSnapshot>,
        type_manager: Arc<TypeManager>,
        thing_manager: Arc<ThingManager>,
        query_options: QueryOptions,
        parameters: Arc<ParameterRegistry>,
        documents: Vec<ConceptDocument>,
        responder: TransactionResponder,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        storage_counters: StorageCounters,
    ) -> ControlFlow<(), ()> {
        let mut result = Vec::with_capacity(documents.len());
        let mut warning = None;
        for document in documents {
            check_timeout_else_respond_error_and_return_break!(timeout_at, responder);
            check_interrupt_else_respond_error_and_return_break!(interrupt, responder);
            // TODO: Consider multiplicity?
            if let Some(limit) = query_options.answer_count_limit {
                if result.len() >= limit {
                    warning = Some(QueryAnswerWarning::WriteResultsLimitExceeded { limit });
                    break;
                }
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
                Ok(encoded_document) => result.push(encoded_document),
                Err(typedb_source) => {
                    respond_error_and_return_break!(
                        responder,
                        TransactionServiceError::PipelineExecution {
                            typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                        }
                    );
                }
            }
        }
        match respond_query_response(responder, QueryAnswer::ResDocuments((QueryType::Write, result, warning))) {
            Ok(_) => Continue(()),
            Err(_) => Break(()),
        }
    }

    fn blocking_read_query_worker(
        &self,
        responder: TransactionResponder,
        query_options: QueryOptions,
        pipeline: typeql::query::Pipeline,
        source_query: String,
        storage_counters: StorageCounters,
    ) -> JoinHandle<ControlFlow<(), ()>> {
        debug_assert!(self.query_queue.is_empty() && self.running_write_query.is_none() && self.transaction.is_some());
        let timeout_at = self.timeout_at;
        let interrupt = self.query_interrupt_receiver.clone();
        with_readable_transaction!(self.transaction.as_ref().unwrap(), |transaction| {
            let snapshot = transaction.snapshot.clone();
            let type_manager = transaction.type_manager.clone();
            let thing_manager = transaction.thing_manager.clone();
            let function_manager = transaction.function_manager.clone();
            let query_manager = transaction.query_manager.clone();
            spawn_blocking(move || {
                let pipeline = prepare_read_query_in(
                    snapshot.clone(),
                    &type_manager,
                    thing_manager.clone(),
                    &function_manager,
                    &query_manager,
                    &pipeline,
                    &source_query,
                );
                let pipeline =
                    unwrap_or_execute_else_respond_error_and_return_break!(pipeline, responder, |typedb_source| {
                        TransactionServiceError::QueryFailed { typedb_source }
                    });
                Self::respond_read_query_sync(
                    query_options,
                    pipeline,
                    &source_query,
                    timeout_at,
                    interrupt,
                    responder,
                    snapshot,
                    &type_manager,
                    thing_manager,
                    storage_counters,
                )
            })
        })
    }

    fn respond_read_query_sync<Snapshot: ReadableSnapshot>(
        query_options: QueryOptions,
        pipeline: Pipeline<Snapshot, ReadPipelineStage<Snapshot>>,
        source_query: &str,
        timeout_at: Instant,
        mut interrupt: ExecutionInterrupt,
        responder: TransactionResponder,
        snapshot: Arc<Snapshot>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        storage_counters: StorageCounters,
    ) -> ControlFlow<(), ()> {
        let query_profile = if pipeline.has_fetch() {
            let (iterator, context) = unwrap_or_execute_else_respond_error_and_return_break!(
                pipeline.into_documents_iterator(interrupt.clone()),
                responder,
                |(err, _)| {
                    TransactionServiceError::QueryFailed {
                        typedb_source: Box::new(QueryError::ReadPipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source: err,
                        }),
                    }
                }
            );

            let parameters = context.parameters;
            let mut result = vec![];
            let mut warning = None;
            for next in iterator {
                if let Some(limit) = query_options.answer_count_limit {
                    if result.len() >= limit {
                        warning = Some(QueryAnswerWarning::ReadResultsLimitExceeded { limit });
                        break;
                    }
                }

                check_timeout_else_respond_error_and_return_break!(timeout_at, responder);
                check_interrupt_else_respond_error_and_return_break!(interrupt, responder);

                let document =
                    unwrap_or_execute_else_respond_error_and_return_break!(next, responder, |typedb_source| {
                        TransactionServiceError::PipelineExecution { typedb_source: *typedb_source }
                    });

                let encoded_document = encode_document(
                    document,
                    snapshot.as_ref(),
                    type_manager,
                    &thing_manager,
                    &parameters,
                    storage_counters.clone(),
                );
                match encoded_document {
                    Ok(encoded_document) => result.push(encoded_document),
                    Err(typedb_source) => {
                        respond_error_and_return_break!(
                            responder,
                            TransactionServiceError::PipelineExecution {
                                typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                            }
                        );
                    }
                }
            }
            respond_else_return_break!(
                responder,
                TransactionServiceResponse::Query(QueryAnswer::ResDocuments((QueryType::Read, result, warning)))
            );
            context.profile
        } else {
            let named_outputs = pipeline.rows_positions().unwrap();
            let descriptor: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();

            let encode_query_structure_result =
                pipeline.query_structure().map(|qs| encode_query_structure(&*snapshot, &type_manager, qs)).transpose();
            let always_taken_blocks =
                pipeline.query_structure().map(|qs| qs.parametrised_structure.always_taken_blocks());
            let query_structure_response = match encode_query_structure_result {
                Ok(structure_opt) => structure_opt,
                Err(typedb_source) => {
                    respond_error_and_return_break!(
                        responder,
                        TransactionServiceError::PipelineExecution {
                            typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                        }
                    );
                }
            };
            let (mut iterator, context) = unwrap_or_execute_else_respond_error_and_return_break!(
                pipeline.into_rows_iterator(interrupt.clone()),
                responder,
                |(err, _)| {
                    TransactionServiceError::QueryFailed {
                        typedb_source: Box::new(QueryError::ReadPipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source: err,
                        }),
                    }
                }
            );

            let mut result = vec![];
            let mut warning = None;
            while let Some(next) = iterator.next() {
                if let Some(limit) = query_options.answer_count_limit {
                    if result.len() >= limit {
                        warning = Some(QueryAnswerWarning::ReadResultsLimitExceeded { limit });
                        break;
                    }
                }

                check_timeout_else_respond_error_and_return_break!(timeout_at, responder);
                check_interrupt_else_respond_error_and_return_break!(interrupt, responder);

                let row = unwrap_or_execute_else_respond_error_and_return_break!(next, responder, |typedb_source| {
                    TransactionServiceError::PipelineExecution { typedb_source: *typedb_source }
                });

                let encoded_row = encode_row(
                    row,
                    &descriptor,
                    snapshot.as_ref(),
                    type_manager,
                    &thing_manager,
                    query_options.include_instance_types,
                    storage_counters.clone(),
                    always_taken_blocks.as_ref(),
                );
                match encoded_row {
                    Ok(encoded_row) => result.push(encoded_row),
                    Err(typedb_source) => {
                        respond_error_and_return_break!(
                            responder,
                            TransactionServiceError::PipelineExecution {
                                typedb_source: PipelineExecutionError::ConceptRead { typedb_source }
                            }
                        );
                    }
                }
            }
            respond_else_return_break!(
                responder,
                TransactionServiceResponse::Query(QueryAnswer::ResRows((
                    QueryType::Read,
                    result,
                    query_structure_response,
                    warning
                )))
            );
            context.profile
        };
        if query_profile.is_enabled() {
            event!(Level::INFO, "Read query done (including network request time).\n{}", query_profile);
        }
        Continue(())
    }

    fn get_database_name(&self) -> Option<&str> {
        self.transaction.as_ref().map(Transaction::get_database_name)
    }
}
