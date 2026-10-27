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
        item::{Guarantees, ItemOrder, MigrationItem, encode_attribute, encode_entity, encode_relation},
    },
    transaction::{DatabaseReadError, TransactionRead},
};

pub enum ExportMessage {
    Schema(String),
    Item(MigrationItem),
}

pub struct DatabaseExporter<'a> {
    transaction: &'a TransactionRead<WALClient>,
    opening: std::vec::IntoIter<ExportMessage>,
    attributes: Box<dyn Iterator<Item = Result<Attribute, Box<ConceptReadError>>> + Send + 'a>,
    entities: Box<dyn Iterator<Item = Result<Entity, Box<ConceptReadError>>> + Send + 'a>,
    relations: Box<dyn Iterator<Item = Result<Relation, Box<ConceptReadError>>> + Send + 'a>,
    checksums: Checksums,
    checksums_pending: bool,
}

impl<'a> DatabaseExporter<'a> {
    const GUARANTEES: Guarantees = Guarantees {
        ordering: ItemOrder::AttributeEntityRelation,
        attributes_unique: true,
        players_by_label_scoped_ids: false,
        owned_attributes_by_value: false,
    };

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
            guarantees: Self::GUARANTEES,
        };
        Ok(Self {
            transaction,
            opening: vec![ExportMessage::Schema(transaction.schema()?), ExportMessage::Item(header)].into_iter(),
            attributes: Box::new(attributes),
            entities: Box::new(entities),
            relations: Box::new(relations),
            checksums: Checksums::new(),
            checksums_pending: true,
        })
    }

    pub fn next_batch(&mut self, batch_size: usize) -> Result<Option<Vec<ExportMessage>>, DatabaseReadError> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            match self.next_item()? {
                Some(item) => batch.push(item),
                None => break,
            }
        }
        Ok((!batch.is_empty()).then_some(batch))
    }

    pub fn next_item(&mut self) -> Result<Option<ExportMessage>, DatabaseReadError> {
        if let Some(item) = self.opening.next() {
            return Ok(Some(item));
        }
        let transaction = self.transaction;
        // Stream attributes first so future exported ownerships find them directly,
        // reducing the need of import-side caches of "unknown" owned attributes
        if let Some(attribute) = self.attributes.next() {
            let item = encode_attribute(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                attribute?,
            )?;
            self.checksums.increment_attribute_count();
            return Ok(Some(ExportMessage::Item(item)));
        }
        if let Some(entity) = self.entities.next() {
            let item = encode_entity(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                entity?,
            )?;
            self.checksums.increment_entity_count();
            return Ok(Some(ExportMessage::Item(item)));
        }
        if let Some(relation) = self.relations.next() {
            let item = encode_relation(
                transaction.snapshot(),
                &transaction.type_manager,
                &transaction.thing_manager,
                &mut self.checksums,
                relation?,
            )?;
            self.checksums.increment_relation_count();
            return Ok(Some(ExportMessage::Item(item)));
        }
        if self.checksums_pending {
            self.checksums_pending = false;
            return Ok(Some(ExportMessage::Item(MigrationItem::Checksums(self.checksums.clone()))));
        }
        Ok(None)
    }
}
