/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, relation::Relation},
};
use database::{migration::Checksums, transaction::TransactionRead};
use resource::profile::StorageCounters;
use storage::durability_client::WALClient;
use typedb_protocol::migration::Item as MigrationItemProto;

use crate::service::{
    export_service::DatabaseExportError,
    migration::item::{encode_attribute_item, encode_checksums_item, encode_entity_item, encode_relation_item},
};

pub struct ExportItems<'a> {
    transaction: &'a TransactionRead<WALClient>,
    header: Option<MigrationItemProto>,
    entities: Box<dyn Iterator<Item = Result<Entity, Box<ConceptReadError>>> + Send + 'a>,
    relations: Box<dyn Iterator<Item = Result<Relation, Box<ConceptReadError>>> + Send + 'a>,
    attributes: Box<dyn Iterator<Item = Result<Attribute, Box<ConceptReadError>>> + Send + 'a>,
    checksums_pending: bool,
}

impl<'a> ExportItems<'a> {
    pub fn new(
        transaction: &'a TransactionRead<WALClient>,
        header: MigrationItemProto,
    ) -> Result<Self, DatabaseExportError> {
        let entities = transaction.thing_manager.get_entities(transaction.snapshot(), StorageCounters::DISABLED);
        let relations = transaction.thing_manager.get_relations(transaction.snapshot(), StorageCounters::DISABLED);
        let attributes = transaction
            .thing_manager
            .get_attributes(transaction.snapshot(), StorageCounters::DISABLED)
            .map_err(|typedb_source| DatabaseExportError::ConceptRead { typedb_source })?;
        Ok(Self {
            transaction,
            header: Some(header),
            entities: Box::new(entities),
            relations: Box::new(relations),
            attributes: Box::new(attributes),
            checksums_pending: true,
        })
    }

    pub fn next_batch(
        &mut self,
        batch_size: usize,
        checksums: &mut Checksums,
    ) -> Result<Option<Vec<MigrationItemProto>>, DatabaseExportError> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            match self.next_item(checksums)? {
                Some(item) => batch.push(item),
                None => break,
            }
        }
        Ok((!batch.is_empty()).then_some(batch))
    }

    pub fn next_item(&mut self, checksums: &mut Checksums) -> Result<Option<MigrationItemProto>, DatabaseExportError> {
        let map_read_err = |typedb_source| DatabaseExportError::ConceptRead { typedb_source };
        if let Some(header) = self.header.take() {
            return Ok(Some(header));
        }
        let transaction = self.transaction;
        if let Some(entity) = self.entities.next() {
            let item = encode_entity_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                checksums,
                entity.map_err(map_read_err)?,
            )
            .map_err(map_read_err)?;
            checksums.entity_count += 1;
            return Ok(Some(item));
        }
        if let Some(relation) = self.relations.next() {
            let item = encode_relation_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                checksums,
                relation.map_err(map_read_err)?,
            )
            .map_err(map_read_err)?;
            checksums.relation_count += 1;
            return Ok(Some(item));
        }
        if let Some(attribute) = self.attributes.next() {
            let item = encode_attribute_item(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                attribute.map_err(map_read_err)?,
            )
            .map_err(map_read_err)?;
            checksums.attribute_count += 1;
            return Ok(Some(item));
        }
        if self.checksums_pending {
            self.checksums_pending = false;
            return Ok(Some(encode_checksums_item(checksums)));
        }
        Ok(None)
    }
}
