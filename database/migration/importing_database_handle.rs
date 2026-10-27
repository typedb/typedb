/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, time::Duration};

use concept::thing::thing_manager::ConceptPersistenceOracle;
use options::TransactionOptions;
use resource::constants::common::SECONDS_IN_DAY;
use storage::durability_client::WALClient;

use crate::{
    Database,
    migration::MigrationOperationError,
    transaction::{DataCommitIntent, SchemaCommitIntent, TransactionError, TransactionSchema, TransactionWrite},
};

pub trait ImportingDatabaseHandle: Send + Sync {
    fn database_name(&self) -> &str;

    fn open_schema(&self) -> Result<TransactionSchema<WALClient>, TransactionError>;

    fn open_write(
        &self,
        concept_persistence_oracle: Option<Box<dyn ConceptPersistenceOracle>>,
    ) -> Result<TransactionWrite<WALClient>, TransactionError>;

    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), MigrationOperationError>;

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), MigrationOperationError>;

    /// Unwraps the handle if this is its only reference, and gives it back otherwise - the Arc::try_unwrap of a trait object
    fn try_unwrap(self: Arc<Self>) -> Result<Box<dyn ImportingDatabaseHandle>, Arc<dyn ImportingDatabaseHandle>>;

    fn finalise(self: Box<Self>) -> Result<(), MigrationOperationError>;
}

const OPTIONS_PARALLEL: bool = true;
const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(1 * SECONDS_IN_DAY).as_millis() as u64;

pub fn open_import_schema_transaction(
    database: &Arc<Database<WALClient>>,
) -> Result<TransactionSchema<WALClient>, TransactionError> {
    TransactionSchema::open(database.clone(), import_transaction_options())
}

pub fn open_import_write_transaction(
    database: &Arc<Database<WALClient>>,
    concept_persistence_oracle: Option<Box<dyn ConceptPersistenceOracle>>,
) -> Result<TransactionWrite<WALClient>, TransactionError> {
    TransactionWrite::open_with_concept_persistence_oracle(
        database.clone(),
        import_transaction_options(),
        concept_persistence_oracle,
    )
}

fn import_transaction_options() -> TransactionOptions {
    TransactionOptions {
        parallel: OPTIONS_PARALLEL,
        schema_lock_acquire_timeout_millis: OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
        transaction_timeout_millis: OPTIONS_TRANSACTION_TIMEOUT_MILLIS,
    }
}
