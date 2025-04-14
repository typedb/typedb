/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{str::FromStr, sync::Arc, time::Duration};

use axum::response::IntoResponse;
use compiler::VariablePosition;
use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use database::transaction::{
    DataCommitError, SchemaCommitError, TransactionError, TransactionRead, TransactionSchema, TransactionWrite,
};
use diagnostics::metrics::LoadKind;
use error::typedb_error;
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::{
        pipeline::Pipeline,
        stage::{ExecutionContext, ReadPipelineStage, StageIterator},
        PipelineExecutionError,
    },
    ExecutionInterrupt, InterruptType,
};
use function::function_manager::FunctionManager;
use http::StatusCode;
use ir::pipeline::ParameterRegistry;
use itertools::{Either, Itertools};
use options::QueryOptions;
use query::{error::QueryError, query_manager::QueryManager};
use resource::constants::server::DEFAULT_TRANSACTION_TIMEOUT_MILLIS;
use serde::{Deserialize, Serialize};
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use tokio::{task::spawn_blocking, time::Instant};
use tracing::{event, Level};
use typeql::{
    query::{stage::Stage, SchemaQuery},
    Query,
};
use uuid::Uuid;

pub(crate) const TRANSACTION_REQUEST_BUFFER_SIZE: usize = 10;

#[derive(Debug)]
pub enum Transaction {
    Read(TransactionRead<WALClient>),
    Write(TransactionWrite<WALClient>),
    Schema(TransactionSchema<WALClient>),
}

macro_rules! with_readable_transaction {
    ($match_:expr, |$transaction:ident| $block:block) => {{
        match $match_ {
            Transaction::Read($transaction) => $block
            Transaction::Write($transaction) => $block
            Transaction::Schema($transaction) => $block
        }
    }}
}
pub(crate) use with_readable_transaction;

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
pub(crate) use unwrap_or_execute_and_return;

use crate::service::TransactionType;

impl Transaction {
    pub fn to_load_kind(&self) -> LoadKind {
        match self {
            Transaction::Read(_) => LoadKind::ReadTransactions,
            Transaction::Write(_) => LoadKind::WriteTransactions,
            Transaction::Schema(_) => LoadKind::SchemaTransactions,
        }
    }
    pub fn to_transaction_kind(&self) -> TransactionType {
        match self {
            Transaction::Read(_) => TransactionType::Read,
            Transaction::Write(_) => TransactionType::Write,
            Transaction::Schema(_) => TransactionType::Schema,
        }
    }

    pub fn get_database_name(&self) -> &str {
        with_readable_transaction!(self, |transaction| { transaction.database.name() })
    }
}

pub(crate) type StreamQueryOutputDescriptor = Vec<(String, VariablePosition)>;
pub(crate) type WriteQueryBatchAnswer = (StreamQueryOutputDescriptor, Batch);
pub(crate) type WriteQueryDocumentsAnswer = (Arc<ParameterRegistry>, Vec<ConceptDocument>);

#[derive(Debug)]
pub(crate) struct WriteQueryAnswer {
    pub(crate) query_options: QueryOptions,
    pub(crate) answer: Either<WriteQueryBatchAnswer, WriteQueryDocumentsAnswer>,
}

impl WriteQueryAnswer {
    pub(crate) fn new_batch(query_options: QueryOptions, answer: WriteQueryBatchAnswer) -> Self {
        Self { query_options, answer: Either::Left(answer) }
    }

    pub(crate) fn new_documents(query_options: QueryOptions, answer: WriteQueryDocumentsAnswer) -> Self {
        Self { query_options, answer: Either::Right(answer) }
    }
}

pub(crate) type WriteQueryResult = Result<WriteQueryAnswer, Box<QueryError>>;

pub(crate) fn is_write_pipeline(pipeline: &typeql::query::Pipeline) -> bool {
    for stage in &pipeline.stages {
        match stage {
            Stage::Insert(_) | Stage::Put(_) | Stage::Delete(_) | Stage::Update(_) => return true,
            Stage::Fetch(_) | Stage::Operator(_) | Stage::Match(_) => {}
        }
    }
    false
}

pub(crate) async fn execute_schema_query(
    transaction: TransactionSchema<WALClient>,
    query: SchemaQuery,
    source_query: String,
) -> (TransactionSchema<WALClient>, Result<(), Box<QueryError>>) {
    let TransactionSchema {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
    } = transaction;
    let mut snapshot = Arc::into_inner(snapshot).unwrap();
    let (snapshot, type_manager, thing_manager, query_manager, function_manager, result) = spawn_blocking(move || {
        let result = query_manager.execute_schema(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            &function_manager,
            query,
            &source_query,
        );
        (snapshot, type_manager, thing_manager, query_manager, function_manager, result)
    })
    .await
    .expect("Expected schema query execution finishing");

    let transaction = TransactionSchema::from(
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
    );

    (transaction, result)
}

pub(crate) fn execute_write_query_in_schema(
    transaction: TransactionSchema<WALClient>,
    query_options: QueryOptions,
    pipeline: typeql::query::Pipeline,
    source_query: String,
    interrupt: ExecutionInterrupt,
) -> (Transaction, WriteQueryResult) {
    let TransactionSchema {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
    } = transaction;

    let (snapshot, result) = execute_write_query_in(
        Arc::into_inner(snapshot).unwrap(),
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &query_manager,
        query_options,
        &pipeline,
        &source_query,
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
}

pub(crate) fn execute_write_query_in_write(
    transaction: TransactionWrite<WALClient>,
    query_options: QueryOptions,
    pipeline: typeql::query::Pipeline,
    source_query: String,
    interrupt: ExecutionInterrupt,
) -> (Transaction, WriteQueryResult) {
    let TransactionWrite {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
    } = transaction;

    let (snapshot, result) = execute_write_query_in(
        Arc::into_inner(snapshot).expect("Cannot unwrap Arc<Snapshot>, still in use."),
        &type_manager,
        thing_manager.clone(),
        &function_manager,
        &query_manager,
        query_options,
        &pipeline,
        &source_query,
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

    (transaction, result)
}

pub(crate) fn execute_write_query_in<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_manager: &QueryManager,
    query_options: QueryOptions,
    pipeline: &typeql::query::Pipeline,
    source_query: &str,
    interrupt: ExecutionInterrupt,
) -> (Snapshot, WriteQueryResult) {
    let result = query_manager.prepare_write_pipeline(
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        pipeline,
        source_query,
    );
    let pipeline = match result {
        Ok(pipeline) => pipeline,
        Err((snapshot, err)) => return (snapshot, Err(err)),
    };

    if pipeline.has_fetch() {
        let (iterator, parameters, snapshot, query_profile) = match pipeline.into_documents_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, profile, parameters, .. })) => {
                (iterator, parameters, snapshot, profile)
            }
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(Box::new(QueryError::WritePipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    })),
                );
            }
        };

        let mut documents = Vec::new();
        for next in iterator {
            match next {
                Ok(document) => documents.push(document),
                Err(typedb_source) => {
                    return (
                        Arc::into_inner(snapshot).unwrap(),
                        Err(Box::new(QueryError::WritePipelineExecution {
                            source_query: source_query.to_string(),
                            typedb_source,
                        })),
                    )
                }
            }
        }
        if query_profile.is_enabled() {
            event!(Level::INFO, "Write query completed.\n{}", query_profile);
        }
        (
            Arc::into_inner(snapshot).unwrap(),
            Ok(WriteQueryAnswer::new_documents(query_options, (parameters, documents))),
        )
    } else {
        let named_outputs = pipeline.rows_positions().unwrap();
        let query_output_descriptor: StreamQueryOutputDescriptor = named_outputs.clone().into_iter().sorted().collect();
        let (iterator, snapshot, query_profile) = match pipeline.into_rows_iterator(interrupt) {
            Ok((iterator, ExecutionContext { snapshot, profile, .. })) => (iterator, snapshot, profile),
            Err((err, ExecutionContext { snapshot, .. })) => {
                return (
                    Arc::into_inner(snapshot).unwrap(),
                    Err(Box::new(QueryError::WritePipelineExecution {
                        source_query: source_query.to_string(),
                        typedb_source: err,
                    })),
                );
            }
        };

        let result = match iterator.collect_owned() {
            Ok(batch) => (
                Arc::into_inner(snapshot).unwrap(),
                Ok(WriteQueryAnswer::new_batch(query_options, (query_output_descriptor, batch))),
            ),
            Err(err) => (
                Arc::into_inner(snapshot).unwrap(),
                Err(Box::new(QueryError::WritePipelineExecution {
                    source_query: source_query.to_string(),
                    typedb_source: err,
                })),
            ),
        };
        if query_profile.is_enabled() {
            event!(Level::INFO, "Write query completed.\n{}", query_profile);
        }
        result
    }
}

pub(crate) fn prepare_read_query_in<Snapshot: ReadableSnapshot + 'static>(
    snapshot: Arc<Snapshot>,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    function_manager: &FunctionManager,
    query_manager: &QueryManager,
    pipeline: &typeql::query::Pipeline,
    source_query: &str,
) -> Result<Pipeline<Snapshot, ReadPipelineStage<Snapshot>>, Box<QueryError>> {
    query_manager.prepare_read_pipeline(snapshot, type_manager, thing_manager, function_manager, pipeline, source_query)
}

pub(crate) fn init_transaction_timeout(transaction_timeout_millis: Option<u64>) -> Instant {
    Instant::now() + Duration::from_millis(transaction_timeout_millis.unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_MILLIS))
}

typedb_error! {
    pub(crate) TransactionServiceError(component = "Transaction service", prefix = "TSV") {
        DatabaseNotFound(1, "Database '{name}' not found.", name: String),
        CannotCommitReadTransaction(2, "Read transactions cannot be committed."),
        CannotRollbackReadTransaction(3, "Read transactions cannot be rolled back, since they never contain writes."),
        TransactionFailed(4, "Transaction failed.", typedb_source: TransactionError),
        DataCommitFailed(5, "Data transaction commit failed.", typedb_source: DataCommitError),
        SchemaCommitFailed(6, "Schema transaction commit failed.", typedb_source: SchemaCommitError),
        QueryParseFailed(7, "Query parsing failed.", typedb_source: typeql::Error),
        SchemaQueryRequiresSchemaTransaction(8, "Schema modification queries require schema transactions."),
        WriteQueryRequiresSchemaOrWriteTransaction(9, "Data modification queries require either write or schema transactions."),
        TxnAbortSchemaQueryFailed(10, "Aborting transaction due to failed schema query.", typedb_source: QueryError),
        QueryFailed(11, "Query failed.", typedb_source: Box<QueryError>),
        NoOpenTransaction(12, "Operation failed: no open transaction."),
        QueryInterrupted(13, "Execution interrupted by to a concurrent {interrupt}.", interrupt: InterruptType),
        QueryStreamNotFound(
            14,
            r#"
            Query stream with id '{query_request_id}' was not found in the transaction.
            The stream could have already finished, or the transaction could be closed, committed, rolled back (or this is a bug).
            "#,
            query_request_id: Uuid
        ),
        ServiceFailedQueueCleanup(15, "The operation failed since the service is closing."),
        PipelineExecution(16, "Pipeline execution failed.", typedb_source: PipelineExecutionError),
        WriteResultsLimitExceeded(17, "Write query results limit ({limit}) exceeded, and the transaction is aborted. Retry with an extended limit or break the query into multiple smaller queries to achieve the same result.", limit: usize),
        TransactionTimeout(18, "Operation failed: transaction timeout."),
    }
}
