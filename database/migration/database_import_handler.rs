/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, time::Duration};

use error::TypeDBError;
use options::TransactionOptions;
use resource::constants::common::SECONDS_IN_DAY;
use storage::durability_client::WALClient;

use crate::{
    Database,
    transaction::{DataCommitIntent, SchemaCommitIntent, TransactionError, TransactionSchema, TransactionWrite},
};

pub trait DatabaseImportHandler: Send + Sync {
    fn database_name(&self) -> &str;

    fn open_schema(&self) -> Result<TransactionSchema<WALClient>, TransactionError>;

    fn open_write(&self) -> Result<TransactionWrite<WALClient>, TransactionError>;

    fn commit_schema(&self, intent: SchemaCommitIntent<WALClient>) -> Result<(), ImportHandlerError>;

    fn commit_data(&self, intent: DataCommitIntent<WALClient>) -> Result<(), ImportHandlerError>;

    fn finalise(self: Box<Self>) -> Result<(), ImportHandlerError>;
}

pub type ImportHandlerError = Arc<dyn TypeDBError + Send + Sync>;

#[derive(Debug)]
pub struct StagedDatabase {
    database: Arc<Database<WALClient>>,
}

impl StagedDatabase {
    const OPTIONS_PARALLEL: bool = true;
    const OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS: u64 = Duration::from_secs(10).as_millis() as u64;
    const OPTIONS_TRANSACTION_TIMEOUT_MILLIS: u64 = Duration::from_secs(1 * SECONDS_IN_DAY).as_millis() as u64;

    pub fn new(database: Arc<Database<WALClient>>) -> Self {
        Self { database }
    }

    pub fn database_name(&self) -> &str {
        self.database.name()
    }

    pub fn open_schema(&self) -> Result<TransactionSchema<WALClient>, TransactionError> {
        TransactionSchema::open(self.database.clone(), Self::transaction_options())
    }

    pub fn open_write(&self) -> Result<TransactionWrite<WALClient>, TransactionError> {
        TransactionWrite::open(self.database.clone(), Self::transaction_options())
    }

    pub fn release(self) -> String {
        let name = self.database_name().to_owned();
        drop(self.database);
        name
    }

    fn transaction_options() -> TransactionOptions {
        TransactionOptions {
            parallel: Self::OPTIONS_PARALLEL,
            schema_lock_acquire_timeout_millis: Self::OPTIONS_SCHEMA_LOCK_ACQUIRE_TIMEOUT_MILLIS,
            transaction_timeout_millis: Self::OPTIONS_TRANSACTION_TIMEOUT_MILLIS,
        }
    }
}
