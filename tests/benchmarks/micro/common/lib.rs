/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod pipelines;
mod transaction;
pub mod utils;

use std::{collections::HashMap, sync::Arc};

use compiler::VariablePosition;
use database::{
    Database,
    database::DatabaseCreateError,
    database_manager::DatabaseManager,
    query::execute_schema_query,
    transaction::{CommitIntent, TransactionSchema},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use executor::{
    batch::Batch,
    document::ConceptDocument,
    pipeline::{PipelineExecutionError, stage::StageIterator},
    row::MaybeOwnedRow,
};
use lending_iterator::LendingIterator;
use options::{TransactionOptions, byte_size::ByteSize};
use query::{error::QueryError, given_rows::GivenRowsSimple};
use resource::profile::TransactionProfile;
use storage::{
    durability_client::WALClient,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};
use test_utils::{TempDir, create_tmp_storage_dir};

use crate::transaction::{CommitError, UnifiedTransactionView, WriteTransactionView};

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

pub enum CollectedAnswer {
    Rows { descriptor: HashMap<String, VariablePosition>, rows: Batch },
    Documents { documents: Vec<ConceptDocument> },
}

/// ANSWER CONSUMER
pub trait AnswerConsumer {
    type Output;
    fn consume_rows<Iter>(iter: &mut Iter) -> Result<Self::Output, Box<PipelineExecutionError>>
    where
        for<'a> Iter: LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>>;

    fn consume_docs(
        iter: &mut impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>,
    ) -> Result<Self::Output, Box<PipelineExecutionError>>;
}

pub struct ResultCounter;
impl AnswerConsumer for ResultCounter {
    type Output = usize;
    fn consume_rows<Iter>(iter: &mut Iter) -> Result<usize, Box<PipelineExecutionError>>
    where
        for<'a> Iter: LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>>,
    {
        let mut count: usize = 0;
        while let Some(row) = iter.next() {
            if let Err(err) = row {
                return Err(err);
            }
            count += 1;
        }
        Ok(count)
    }

    fn consume_docs(
        iter: &mut impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>,
    ) -> Result<usize, Box<PipelineExecutionError>> {
        let mut count: usize = 0;
        while let Some(row) = iter.next() {
            if let Err(err) = row {
                return Err(err);
            }
            count += 1;
        }
        Ok(count)
    }
}

pub fn commit(tx: impl WriteTransactionView) -> Result<TransactionProfile, CommitError> {
    tx.commit()
}

pub fn execute_read_query_in<TX: UnifiedTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<(AC::Output, TX), (Box<QueryError>, TX)> {
    pipelines::execute_read_query_in::<_, AC>(tx, query, given_rows)
}

pub fn execute_write_query_in<TX: UnifiedTransactionView + WriteTransactionView, AC: AnswerConsumer>(
    tx: TX,
    query: &str,
    given_rows: Option<GivenRowsSimple>,
) -> Result<(AC::Output, TX), (Box<QueryError>, TX)> {
    pipelines::execute_write_query_in::<_, AC>(tx, query, given_rows)
}
