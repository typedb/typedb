/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptReadError;
use database::transaction::DatabaseReadError;
use error::typedb_error;
use ir::pipeline::FunctionReadError;

impl From<DatabaseReadError> for DatabaseExportError {
    fn from(error: DatabaseReadError) -> Self {
        match error {
            DatabaseReadError::ConceptRead { typedb_source } => Self::ConceptRead { typedb_source },
            DatabaseReadError::FunctionRead { typedb_source } => Self::FunctionRead { typedb_source },
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
