/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptDecodeError;
use database::migration::database_importer::DatabaseImportError;
use error::typedb_error;

typedb_error! {
    pub(crate) DatabaseImportServiceError(component = "Database import service", prefix = "DIS") {
        DatabaseImport(1, "Error importing database.", typedb_source: DatabaseImportError),
        ConceptDecode(2, "Cannot decode imported concept.", typedb_source: Box<ConceptDecodeError>),
        DuplicateImport(3, "Error importing '{name}': another import operation for database '{old_name}'was already initiated through this channel. It is a sign of a corrupted file or a client bug.", name: String, old_name: String),
        DatabaseNotFoundForItems(4, "Imported database not found while loading data. Make sure to use a correct client."),
        DatabaseNotFoundForDone(5, "Imported database not found while finalizing import. Make sure to use a correct client."),
        EmptyItem(6, "An empty concept item received. It is a sign of a corrupted file or a client bug."),
        AbsentAttributeValue(7, "Cannot process an attribute: value is absent."),
        AttributesOwningAttributes(8, "Invalid migration item received: attributes cannot own attributes in this version of TypeDB (this was deprecated). Please modify your data accordingly and reexport the original database before trying again."),
    }
}
