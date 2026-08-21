/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptReadError;
use database::{
    migration::{MigrationExportError, transaction_schema, transaction_type_schema},
    transaction::TransactionRead,
};
use error::typedb_error;
use ir::pipeline::FunctionReadError;
use storage::durability_client::DurabilityClient;

pub(crate) fn get_transaction_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, DatabaseExportError> {
    transaction_schema(transaction).map_err(DatabaseExportError::from)
}

pub(crate) fn get_transaction_type_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, DatabaseExportError> {
    transaction_type_schema(transaction).map_err(DatabaseExportError::from)
}

impl From<MigrationExportError> for DatabaseExportError {
    fn from(error: MigrationExportError) -> Self {
        match error {
            MigrationExportError::ConceptRead { typedb_source } => Self::ConceptRead { typedb_source },
            MigrationExportError::FunctionRead { typedb_source } => Self::FunctionRead { typedb_source },
        }
    }
}

typedb_error! {
    pub DatabaseExportError(component = "Database export", prefix = "DBE") {
        ConceptRead(2, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        FunctionRead(3, "Error reading functions.", typedb_source: FunctionReadError),
        ShutdownInterrupt(4, "Execution interrupted by a shutdown signal."),
        ClientChannelIsClosed(5, "Client channel is closed."),
        TransactionCloseInterrupt(6, "Execution interrupted: the export's transaction was forcefully closed."),
    }
}
