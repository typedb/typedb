/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod transaction;

use std::collections::HashMap;
use std::sync::Arc;
use compiler::VariablePosition;
use database::Database;
use database::database::DatabaseCreateError;
use database::database_manager::DatabaseManager;
use database::query::execute_schema_query;
use database::transaction::{CommitIntent, DataCommitError, DatabaseDropGuard, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use executor::batch::Batch;
use executor::document::ConceptDocument;
use executor::ExecutionInterrupt;
use executor::pipeline::pipeline::Pipeline;
use executor::pipeline::stage::StageIterator;
use executor::pipeline::PipelineExecutionError;
use executor::pipeline::stage::{ExecutionContext, ReadPipelineStage, WritePipelineStage};
use executor::row::MaybeOwnedRow;
use function::function_manager::FunctionManager;
use options::byte_size::ByteSize;
use options::{QueryOptions, TransactionOptions};
use query::error::QueryError;
use query::given_rows::GivenRowsSimple;
use query::query_manager::QueryManager;
use resource::profile::TransactionProfile;
use storage::durability_client::WALClient;
use storage::snapshot::{ReadSnapshot, ReadableSnapshot, WritableSnapshot, WriteSnapshot};
use test_utils::{TempDir, create_tmp_storage_dir};
use crate::transaction::{CommitError, PartialTx, UnifiedTransactionView};

#[derive(Debug)]
pub struct Config {
    pub rocksdb_cache_size: ByteSize,
    pub rocksdb_write_buffers_limit: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rocksdb_cache_size: ByteSize::gb(1),
            rocksdb_write_buffers_limit: ByteSize::mb(512),
        }
    }
}
pub struct Context {
    config: Config,
    tmp_dir: TempDir,
    database_manager: Arc<DatabaseManager>,
}

impl Context {
    pub fn init(config: Config) -> Self {
        let tmp_dir = create_tmp_storage_dir();
        let database_manager = DatabaseManager::new(
            &tmp_dir,
            Arc::new(DiagnosticsManager::new_disabled()),
            config.rocksdb_cache_size,
            config.rocksdb_write_buffers_limit,
        )
            .unwrap();
        Self { config, tmp_dir, database_manager }
    }

    pub fn create_database(&self, name: &str) -> Result<Arc<Database<WALClient>>, DatabaseCreateError> {
        self.database_manager.put_database(name)?;
        Ok(self.database_manager.database(name).unwrap())
    }
}

pub fn create_schema(database: Arc<Database<WALClient>>, schema: &str) {
    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}


struct ErrorWrapper<Snapshot, Err> {
    snapshot: Arc<Snapshot>,
    partial_tx: PartialTx,
    err: Err,
}

impl<Snapshot: ReadableSnapshot, Err> ErrorWrapper<Snapshot, Err> {
    fn new(snapshot: Arc<Snapshot>, partial_tx: PartialTx, err: Err) -> Self {
        Self { snapshot, partial_tx, err }
    }
}
fn to_tx_and_err<T, TX: UnifiedTransactionView, Err>(error_wrapper: ErrorWrapper<TX::Snapshot, Err>) -> (TX, Result<T, Err>) {
    (TX::reconstruct(error_wrapper.snapshot, error_wrapper.partial_tx), Err::<T, Err>(error_wrapper.err))
}

pub enum CollectedAnswer {
    Rows { descriptor: HashMap<String, VariablePosition>, rows: Batch },
    Documents { documents: Vec<ConceptDocument> }
}

/// ANSWER CONSUMER
pub trait RowConsumer {
    fn consume(row: MaybeOwnedRow<'_>);
}
pub trait DocumentConsumer {
    fn consume(row: ConceptDocument);
}

pub struct AnswerConsumer<R: RowConsumer, D: DocumentConsumer> {
    row_consumer: R,
    document_consumer: D,
}

pub fn commit(tx: impl UnifiedTransactionView) -> Result<TransactionProfile, CommitError> {
    tx.commit()
}
//
// pub fn execute_write_query_in(tx: TransactionWrite<WALClient>, query: String, given_rows: Option<GivenRowsSimple>) -> (TransactionWrite<WALClient>, Result<CollectedAnswer, Box<QueryError>>) {
//     let parsed = typeql::parse_query(query.as_str()).unwrap().into_structure().into_pipeline();
//     let interrupt = ExecutionInterrupt::new_uninterruptible();
//     let (tx, result) =
//         database::query::execute_write_query_in_write(tx, QueryOptions::default_grpc(), parsed, given_rows, query, interrupt);
//     match result  {
//         Ok(WriteQueryAnswer { answer: Either::Left((descriptor, rows, _)), .. }) => {
//             let descriptor = descriptor.into_iter().collect();
//             (tx, Ok(CollectedAnswer::Rows { descriptor, rows }))
//         }
//         Ok(WriteQueryAnswer { answer: Either::Right((_, documents)), .. }) => {
//             (tx, Ok(CollectedAnswer::Documents { documents }))
//         }
//         Err(err) => (tx, Err(err))
//     }
// }

pub fn execute_read_query_in<RC: RowConsumer, DC: DocumentConsumer>(tx: TransactionRead<WALClient>, query: &str, given_rows: Option<GivenRowsSimple>, answer_consumer: AnswerConsumer<RC, DC>) -> (TransactionRead<WALClient>, Result<CollectedAnswer, Box<QueryError>>) {
    let (snapshot, partial_tx) = tx.into_parts();
    let prepare_result = prepare_read_pipeline(snapshot, partial_tx, query, given_rows);
    let pipeline = match prepare_result {
        Ok(pipeline) => pipeline,
        Err(err) => return to_tx_and_err(err)
    };
    todo!()
}

// fn collect_results_of_pipeline<Snapshot>(
//     source_query: &str, pipeline: PipelineWrapper<Snapshot>,
// ) -> Result<(CollectedAnswer, ExecutionContext<Snapshot>), (Box<PipelineExecutionError>, ExecutionContext<Snapshot>)> {
//     let interrupt = ExecutionInterrupt::new_uninterruptible();
//     if pipeline.has_fetch() {
//         let result = pipeline.into_documents_iterator(interrupt)
//             .and_then(|(iterator, context)| squish(context, iterator.collect::<Result<Vec<_>, _>>())); //
//         match result {
//             Ok((documents, context)) => {
//                 Ok((CollectedAnswer::Documents { documents }, context))
//             },
//             Err((err, context)) => {
//                 Err((wrap_error(source_query, err), context))
//             }
//         }
//     } else {
//         let descriptor = pipeline.rows_positions().unwrap().clone();
//         let result = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible())
//             .and_then(|(iterator, context)| squish(context, iterator.collect_owned()));
//         match result {
//             Ok((rows, context)) => {
//                 Ok((CollectedAnswer::Rows { rows, descriptor }, context))
//             },
//             Err((err, context)) => {
//                 Err((wrap_error(source_query, err), context))
//             }
//         }
//     }
// }
//


struct ReadPipelineWrapper<Snapshot: ReadableSnapshot + 'static>(Arc<Snapshot>, PartialTx, Pipeline<Snapshot, ReadPipelineStage<Snapshot>>);
struct WritePipelineWrapper<Snapshot: WritableSnapshot + 'static>(PartialTx, Pipeline<Snapshot, WritePipelineStage<Snapshot>>);

fn prepare_read_pipeline<Snapshot: ReadableSnapshot>(snapshot: Arc<Snapshot>, partial_tx: PartialTx, query: &str, given_rows: Option<GivenRowsSimple>) -> Result<ReadPipelineWrapper<Snapshot>,  ErrorWrapper<Snapshot, Box<QueryError>>> {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let prepare_result = partial_tx.query_manager.prepare_read_pipeline(
        snapshot.clone(),
        &partial_tx.type_manager,
        partial_tx.thing_manager.clone(),
        partial_tx.function_manager.clone(),
        &parsed,
        given_rows,
        &query,
    );
    match prepare_result {
        Ok(pipeline) => Ok(ReadPipelineWrapper(snapshot, partial_tx, pipeline)),
        Err(err) => Err(ErrorWrapper::new(snapshot, partial_tx, err))
    }
}

// fn prepare_write_pipeline<Snapshot: WritableSnapshot>(snapshot: Snapshot, partial_tx: PartialTx, query: &str, given_rows: Option<GivenRowsSimple>) -> Result<PipelineWrapper<Snapshot>,  ErrorWrapper<Snapshot, Box<QueryError>>> {
//     let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
//     let prepare_result = partial_tx.query_manager.prepare_write_pipeline(
//         snapshot,
//         &partial_tx.type_manager,
//         partial_tx.thing_manager.clone(),
//         partial_tx.function_manager.clone(),
//         &parsed,
//         given_rows,
//         &query,
//     );
//     match prepare_result {
//         Ok(pipeline) => Ok(PipelineWrapper::Write(partial_tx, pipeline)),
//         Err((snapshot, err)) => Err(ErrorWrapper::new(snapshot, partial_tx, err))
//     }
// }
//
//
// // Utils
// fn squish<T, OK, ERR>(t: T, r: Result<OK, ERR>) -> Result<(OK, T), (ERR, T)> {
//     match r {
//         Ok(ok) => Ok((ok, t)),
//         Err(err) => Err((err, t)),
//     }
// }