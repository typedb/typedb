/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptDecodeError;
use database::migration::database_importer::DatabaseImportError;
use error::typedb_error;

use crate::{error::ArcServerStateError, service::migration::item::ItemDecodeError};

impl From<ItemDecodeError> for DatabaseImportServiceError {
    fn from(error: ItemDecodeError) -> Self {
        match error {
            ItemDecodeError::EmptyItem => Self::ImportEmptyItem {},
            ItemDecodeError::AbsentAttributeValue => Self::AbsentAttributeValue {},
            ItemDecodeError::AttributesOwningAttributes => Self::AttributesOwningAttributes {},
            ItemDecodeError::ConceptDecode { typedb_source } => Self::ConceptDecode { typedb_source },
        }
    }
}

typedb_error! {
    pub DatabaseImportServiceError(component = "Database import service", prefix = "DIS") {
        DatabaseImport(1, "Error while importing the database's schema and data.", typedb_source: DatabaseImportError),
        ConceptDecode(2, "Cannot decode imported concept.", typedb_source: Box<ConceptDecodeError>),
        DuplicateImport(3, "Error importing '{name}': another import operation for database '{old_name}' was already initiated through this channel. It is a sign of a corrupted file or a client bug.", name: String, old_name: String),
        ImportDatabaseNotFound(4, "Imported database not found during {phase}. Make sure to use a correct client.", phase: String),
        ImportEmptyItem(5, "An empty concept item received. It is a sign of a corrupted file or a client bug."),
        AbsentAttributeValue(6, "Cannot process an attribute: value is absent."),
        AttributesOwningAttributes(7, "Invalid migration item received: attributes cannot own attributes in this version of TypeDB (this was deprecated). Please modify your data accordingly and reexport the original database before trying again."),
        ImportPrepareFailed(8, "The server could not open a database import for this request.", typedb_source: ArcServerStateError),
        ImportTaskFailed(9, "Import processing unexpectedly failed during {phase}. The import is aborted and can be retried.", phase: String),
        ImportClosed(10, "The import was closed by the server. The import is aborted and can be retried."),
        ShutdownInterrupt(11, "The import was interrupted by server shutdown. The import is aborted and can be retried."),
        ClientClosed(12, "The import stream was closed by the client before completion. The import is aborted and can be retried."),
    }
}
