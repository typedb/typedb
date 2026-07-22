/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod pipelines;
mod transaction;
pub mod utils;
pub mod profiler;

use std::sync::Arc;

use database::{
    Database,
    database::DatabaseCreateError,
    database_manager::DatabaseManager,
    query::execute_schema_query,
    transaction::{CommitIntent, TransactionSchema},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use executor::{document::ConceptDocument, pipeline::PipelineExecutionError, row::MaybeOwnedRow};
use lending_iterator::LendingIterator;
use options::{TransactionOptions, byte_size::ByteSize};
use query::{error::QueryError, given_rows::GivenRowsSimple};
use resource::profile::{QueryProfile, TransactionProfile};
use storage::durability_client::WALClient;
use test_utils::{TempDir, create_tmp_storage_dir};

use crate::{
    transaction::{CommitError, UnifiedTransactionView, WriteTransactionView},
    utils::PackedResult,
};

#[derive(Debug)]
pub struct Config {
    pub rocksdb_cache_size: ByteSize,
    pub rocksdb_write_buffers_limit: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self { rocksdb_cache_size: ByteSize::gb(1), rocksdb_write_buffers_limit: ByteSize::mb(512) }
    }
}
pub struct Context {
    config: Config,
    _tmp_dir: TempDir,
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
        Self { config, _tmp_dir: tmp_dir, database_manager }
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

pub fn commit(tx: impl UnifiedTransactionView) -> Result<TransactionProfile, CommitError> {
    tx.commit()
}

pub trait AnswerConsumer {
    type Output;
    fn consume_rows<Iter>(iter: &mut Iter) -> Result<Self::Output, Box<PipelineExecutionError>>
    where
        for<'a> Iter: LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>>;

    fn consume_docs(
        iter: &mut impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>,
    ) -> Result<Self::Output, Box<PipelineExecutionError>>;
}

pub struct QueryAnswer<T> {
    pub answer: T,
    pub profile: Arc<QueryProfile>,
}

pub fn execute_read_query_in<TX: UnifiedTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> PackedResult<QueryAnswer<AC::Output>, Box<QueryError>, TX> {
    pipelines::execute_read_query_in::<_, AC>(tx, query, given_rows)
}

pub fn execute_write_query_in<TX: UnifiedTransactionView + WriteTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> PackedResult<QueryAnswer<AC::Output>, Box<QueryError>, TX> {
    pipelines::execute_write_query_in::<_, AC>(tx, query, given_rows)
}
