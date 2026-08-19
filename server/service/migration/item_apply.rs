/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use database::migration::database_importer::DatabaseImporter;
use encoding::value::label::Label;
use itertools::Itertools;
use tracing::{Level, event};
use typedb_protocol::migration::{
    Item as MigrationItemProto,
    item::{
        Attribute as MigrationAttributeProto, Checksums as MigrationChecksumsProto, Entity as MigrationEntityProto,
        Header as MigrationHeaderProto, OwnedAttribute as MigrationOwnedAttributeProto,
        Relation as MigrationRelationProto,
        relation::{Role as MigrationRoleProto, role::Player as MigrationRolePlayerProto},
    },
};

use crate::service::{
    import_service::DatabaseImportServiceError,
    migration::item::{decode_checksums, decode_migration_value},
};

pub fn process_item(
    item_proto: MigrationItemProto,
    database_importer: &mut DatabaseImporter,
) -> Result<(), DatabaseImportServiceError> {
    use typedb_protocol::migration::item;
    let MigrationItemProto { item } = item_proto;
    let Some(item) = item else {
        return Err(DatabaseImportServiceError::ImportEmptyItem {});
    };

    match item {
        item::Item::Attribute(attribute) => process_attribute(attribute, database_importer),
        item::Item::Entity(entity) => process_entity(entity, database_importer),
        item::Item::Relation(relation) => process_relation(relation, database_importer),
        item::Item::Header(header) => process_header(database_importer, header),
        item::Item::Checksums(checksums) => process_checksums(database_importer, checksums),
    }
}

fn process_attribute(
    attribute_proto: MigrationAttributeProto,
    database_importer: &mut DatabaseImporter,
) -> Result<(), DatabaseImportServiceError> {
    let MigrationAttributeProto { id, label: label_text, attributes, value } = attribute_proto;
    if !attributes.is_empty() {
        return Err(DatabaseImportServiceError::AttributesOwningAttributes {});
    }
    let label = Label::parse_from(&label_text, None);
    let value = decode_migration_value(value.ok_or_else(|| DatabaseImportServiceError::AbsentAttributeValue {})?)
        .map_err(|typedb_source| DatabaseImportServiceError::ConceptDecode { typedb_source })?;

    database_importer
        .import_attribute(id, label, value)
        .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
}

fn process_entity(
    entity_proto: MigrationEntityProto,
    database_importer: &mut DatabaseImporter,
) -> Result<(), DatabaseImportServiceError> {
    let MigrationEntityProto { id, label: label_text, attributes } = entity_proto;
    let label = Label::parse_from(&label_text, None);

    database_importer
        .import_entity(id, label, convert_owned_attributes(attributes))
        .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
}

fn process_relation(
    relation_proto: MigrationRelationProto,
    database_importer: &mut DatabaseImporter,
) -> Result<(), DatabaseImportServiceError> {
    let MigrationRelationProto { id, label: label_text, attributes, roles } = relation_proto;
    let label = Label::parse_from(&label_text, None);

    database_importer
        .import_relation(id, label, convert_owned_attributes(attributes), convert_related_role_players(roles))
        .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
}

fn process_header(
    database_importer: &DatabaseImporter,
    header_proto: MigrationHeaderProto,
) -> Result<(), DatabaseImportServiceError> {
    let MigrationHeaderProto { typedb_version: original_version, original_database } = header_proto;
    let new_database = database_importer.database_name();
    event!(Level::DEBUG, "Importing '{original_database}' from TypeDB {original_version} to '{new_database}'.");
    Ok(())
}

fn process_checksums(
    database_importer: &mut DatabaseImporter,
    checksums_proto: MigrationChecksumsProto,
) -> Result<(), DatabaseImportServiceError> {
    database_importer
        .record_expected_checksums(decode_checksums(checksums_proto))
        .map_err(|typedb_source| DatabaseImportServiceError::DatabaseImport { typedb_source })
}

fn convert_owned_attributes(attributes: Vec<MigrationOwnedAttributeProto>) -> Vec<String> {
    attributes
        .into_iter()
        .map(|proto| {
            let MigrationOwnedAttributeProto { id } = proto;
            id
        })
        .collect_vec()
}

fn convert_related_role_players(roles: Vec<MigrationRoleProto>) -> Vec<(Label, Vec<String>)> {
    roles
        .into_iter()
        .map(|role_proto| {
            let MigrationRoleProto { label: label_text, players } = role_proto;
            let label = Label::parse_from(&label_text, None);
            (
                label,
                players
                    .into_iter()
                    .map(|proto| {
                        let MigrationRolePlayerProto { id } = proto;
                        id
                    })
                    .collect_vec(),
            )
        })
        .collect_vec()
}
