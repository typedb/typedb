/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, relation::Relation},
};
use resource::profile::StorageCounters;
use storage::durability_client::WALClient;

use crate::{
    migration::{
        Checksums,
        item::{MigrationItem, encode_attribute, encode_entity, encode_relation},
    },
    transaction::{DatabaseReadError, TransactionRead},
};

pub struct DatabaseExporter<'a> {
    transaction: &'a TransactionRead<WALClient>,
    opening: std::vec::IntoIter<MigrationItem>,
    entities: Box<dyn Iterator<Item = Result<Entity, Box<ConceptReadError>>> + Send + 'a>,
    relations: Box<dyn Iterator<Item = Result<Relation, Box<ConceptReadError>>> + Send + 'a>,
    attributes: Box<dyn Iterator<Item = Result<Attribute, Box<ConceptReadError>>> + Send + 'a>,
    checksums: Checksums,
    checksums_pending: bool,
}

impl<'a> DatabaseExporter<'a> {
    pub fn new(
        transaction: &'a TransactionRead<WALClient>,
        typedb_version: impl Into<String>,
        original_database: impl Into<String>,
    ) -> Result<Self, DatabaseReadError> {
        let entities = transaction.thing_manager.get_entities(transaction.snapshot(), StorageCounters::DISABLED);
        let relations = transaction.thing_manager.get_relations(transaction.snapshot(), StorageCounters::DISABLED);
        let attributes = transaction.thing_manager.get_attributes(transaction.snapshot(), StorageCounters::DISABLED)?;
        let header = MigrationItem::Header {
            typedb_version: typedb_version.into(),
            original_database: original_database.into(),
        };
        Ok(Self {
            transaction,
            opening: vec![MigrationItem::Schema(transaction.schema()?), header].into_iter(),
            entities: Box::new(entities),
            relations: Box::new(relations),
            attributes: Box::new(attributes),
            checksums: Checksums::new(),
            checksums_pending: true,
        })
    }

    pub fn next_item(&mut self) -> Result<Option<MigrationItem>, DatabaseReadError> {
        if let Some(item) = self.opening.next() {
            return Ok(Some(item));
        }
        let transaction = self.transaction;
        if let Some(entity) = self.entities.next() {
            let item = encode_entity(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                entity?,
            )?;
            self.checksums.entity_count += 1;
            return Ok(Some(item));
        }
        if let Some(relation) = self.relations.next() {
            let item = encode_relation(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                relation?,
            )?;
            self.checksums.relation_count += 1;
            return Ok(Some(item));
        }
        if let Some(attribute) = self.attributes.next() {
            let item = encode_attribute(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                attribute?,
            )?;
            self.checksums.attribute_count += 1;
            return Ok(Some(item));
        }
        if self.checksums_pending {
            self.checksums_pending = false;
            return Ok(Some(MigrationItem::Checksums(self.checksums.clone())));
        }
        Ok(None)
    }

    pub fn next_batch(&mut self, batch_size: usize) -> Result<Option<Vec<MigrationItem>>, DatabaseReadError> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            match self.next_item()? {
                Some(item) => batch.push(item),
                None => break,
            }
        }
        Ok((!batch.is_empty()).then_some(batch))
    }
}
