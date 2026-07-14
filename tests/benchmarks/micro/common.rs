/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use itertools::{Either, Itertools};
use compiler::VariablePosition;
use database::Database;
use database::database::DatabaseCreateError;
use database::database_manager::DatabaseManager;
use database::query::{execute_schema_query, execute_write_query_in_write, WriteQueryAnswer};
use database::transaction::{CommitIntent, DataCommitError, DataCommitIntent, TransactionRead, TransactionSchema, TransactionWrite};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use executor::batch::Batch;
use executor::document::ConceptDocument;
use executor::ExecutionInterrupt;
use executor::pipeline::pipeline::Pipeline;
use executor::pipeline::stage::StageIterator;
use executor::pipeline::PipelineExecutionError;
use executor::pipeline::stage::{ExecutionContext, ReadPipelineStage, ReadStageIterator};
use options::byte_size::ByteSize;
use options::{QueryOptions, TransactionOptions};
use query::error::QueryError;
use query::given_rows::GivenRowsSimple;
use resource::profile::TransactionProfile;
use storage::durability_client::WALClient;
use storage::snapshot::ReadableSnapshot;
use test_utils::{TempDir, create_tmp_storage_dir};


pub enum CollectedAnswer {
    Rows { descriptor: HashMap<String, VariablePosition>, rows: Batch },
    Documents { documents: Vec<ConceptDocument> }
}

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

pub fn commit_write_tx(tx: TransactionWrite<WALClient>) -> Result<TransactionProfile, DataCommitError> {
    let (mut profile, finalise_result) = tx.finalise();
    finalise_result.and_then(|intent| intent.commit(profile.commit_profile()))?;
    Ok(profile)
}

pub fn create_schema(database: Arc<Database<WALClient>>, schema: &str) {
    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

pub fn execute_write_query_in(tx: TransactionWrite<WALClient>, query: String, given_rows: Option<GivenRowsSimple>) -> (TransactionWrite<WALClient>, Result<CollectedAnswer, Box<QueryError>>) {
    let parsed = typeql::parse_query(query.as_str()).unwrap().into_structure().into_pipeline();
    let interrupt = ExecutionInterrupt::new_uninterruptible();
    let (tx, result) =
        database::query::execute_write_query_in_write(tx, QueryOptions::default_grpc(), parsed, given_rows, query, interrupt);
    match result  {
        Ok(WriteQueryAnswer { answer: Either::Left((descriptor, rows, _)), .. }) => {
            let descriptor = descriptor.into_iter().collect();
            (tx, Ok(CollectedAnswer::Rows { descriptor, rows }))
        }
        Ok(WriteQueryAnswer { answer: Either::Right((_, documents)), .. }) => {
            (tx, Ok(CollectedAnswer::Documents { documents }))
        }
        Err(err) => (tx, Err(err))
    }
}

pub fn execute_read_query_in(tx: TransactionRead<WALClient>, query: String, given_rows: Option<GivenRowsSimple>) -> (TransactionRead<WALClient>, Result<CollectedAnswer, Box<QueryError>>) {
    let parsed = typeql::parse_query(query.as_str()).unwrap().into_structure().into_pipeline();
    let snapshot = tx.snapshot.clone();
    let type_manager = tx.type_manager.clone();
    let thing_manager = tx.thing_manager.clone();
    let function_manager = tx.function_manager.clone();
    let query_manager = tx.query_manager.clone();
    let prepare_result = query_manager.prepare_read_pipeline(
        snapshot.clone(),
        &type_manager,
        thing_manager.clone(),
        function_manager.clone(),
        &parsed,
        given_rows,
        &query,
    );
    match prepare_result {
        Ok(pipeline) => (tx, collect_results_of_read_pipeline(query.as_str(), pipeline)),
        Err(err) => (tx, Err(err))
    }
}

fn collect_results_of_read_pipeline<Snapshot: ReadableSnapshot>(source_query: &str, pipeline: Pipeline<Snapshot, ReadPipelineStage<Snapshot>>) -> Result<CollectedAnswer, Box<QueryError>> {
    if pipeline.has_fetch() {
        let into_result = pipeline.into_documents_iterator(ExecutionInterrupt::new_uninterruptible());
        let iterator = match into_result {
            Ok((iterator, _)) => iterator,
            Err((err, _)) => return wrap_error(source_query, Err(err)),
        };
        let documents = wrap_error(source_query, iterator.collect::<Result<Vec<_>, _>>())?;
        Ok(CollectedAnswer::Documents { documents })
    } else {
        let descriptor = pipeline.rows_positions().unwrap().clone();
        let into_result = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible());
        let iterator = match into_result {
            Ok((iterator, _)) => iterator,
            Err((err, _)) => return wrap_error(source_query, Err(err)),
        };
        let rows = wrap_error(source_query, iterator.collect_owned())?;
        Ok(CollectedAnswer::Rows { descriptor, rows })
    }
}

fn wrap_error<T>(source_query: &str, r: Result<T, Box<PipelineExecutionError>>) -> Result<T, Box<QueryError>> {
    r.map_err(|err|  Box::new(QueryError::WritePipelineExecution {
        source_query: source_query.to_owned(),
        typedb_source: err,
    }))
}
