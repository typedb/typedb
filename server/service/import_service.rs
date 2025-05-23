/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::{ConceptDecodeError, ConceptReadError, ConceptWriteError};
use database::{
    database::DatabaseCreateError,
    transaction::{DataCommitError, SchemaCommitError, TransactionError},
};
use encoding::value::label::Label;
use error::typedb_error;
use query::error::QueryError;

typedb_error! {
    pub(crate) DatabaseImportError(component = "Database import", prefix = "DBI") {
        TransactionFailed(1, "Import transaction failed.", typedb_source: TransactionError),
        ConceptRead(2, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        ConceptWrite(3, "Error writing concepts.", typedb_source: Box<ConceptWriteError>),
        ConceptDecode(4, "Cannot decode imported concept.", typedb_source: Box<ConceptDecodeError>),
        DatabaseCreate(5, "Error creating imported database.", typedb_source: DatabaseCreateError),
        DatabaseAlreadyExists(6, "Imported database '{name}' already exists.", name: String),
        CreatedDatabaseNotFound(7, "Interrupted: database '{name}' was not found after its creation. It might have been deleted due to a parallel operation or an internal error.", name: String),
        DataCommitFailed(8, "Import data transaction commit failed.", typedb_source: DataCommitError),
        ProvidedSchemaCommitFailed(9, "Imported schema cannot be committed due to errors.", typedb_source: SchemaCommitError),
        PreparationSchemaCommitFailed(10, "Import schema transaction commit failed on preparation. It is a sign of a bug.", typedb_source: SchemaCommitError),
        FinalizationSchemaCommitFailed(11, "Import schema transaction commit failed on finalization. It is a sign of a bug.", typedb_source: SchemaCommitError),
        SchemaQueryParseFailed(12, "Import schema query parsing failed.", typedb_source: typeql::Error),
        SchemaQueryFailed(13, "Import schema query failed.", typedb_source: Box<QueryError>),
        InvalidSchemaDefineQuery(14, "Import schema query is not a valid define query."),
        DatabaseNotFoundForItems(15, "Imported database not found while loading data. Make sure to use a correct client."),
        DatabaseNotFoundForDone(16, "Imported database not found while finalizing import. Make sure to use a correct client."),
        EmptyItem(17, "An empty concept item received. It is a sign of a corrupted file or a client bug."),
        DuplicateClientChecksums(18, "Checksums received multiple times. It is a sign of a corrupted file or a client bug."),
        AbsentAttributeValue(19, "Cannot process an attribute: value is absent."),
        UnknownAttributeType(20, "Cannot process an attribute: attribute type '{label}' does not exist in the schema.", label: Label),
        UnknownEntityType(21, "Cannot process an entity: entity type '{label}' does not exist in the schema.", label: Label),
        UnknownRelationType(22, "Cannot process a relation: relation type '{label}' does not exist in the schema.", label: Label),
        UnknownRoleType(23, "Cannot process a role player: role type '{label}' does not exist in the schema.", label: Label),
        AttributesOwningAttributes(24, "Invalid migration item received: attributes cannot own attributes in this version of TypeDB (this was deprecated). Please modify your data accordingly and reexport the original database before trying again."),
        NoClientChecksumsOnDone(25, "Cannot verify the imported database as there are no checksums received from the client. It is a sign of a corrupted file or a client bug."),
        InvalidChecksumsOnDone(26, "Invalid imported database with a checksums mismatch: {details}.", details: String),
        IncompleteOwnershipsOnDone(27, "Invalid imported database with {count} unknown owned attributes. It is a sign of a corrupted file or a client bug.", count: usize),
        IncompleteRolesOnDone(28, "Invalid imported database with {count} unknown role players. It is a sign of a corrupted file or a client bug.", count: usize),
    }
}
