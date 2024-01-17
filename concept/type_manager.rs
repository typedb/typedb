/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


use std::rc::Rc;
use encoding::Prefix;

use encoding::type_::id_generator::TypeIIDGenerator;
use encoding::type_::type_encoding::concept::TypeIID;
use encoding::type_::type_encoding::index::{TypeIIDLabelIndex, LabelTypeIIDIndex};
use storage::key::WritableKey;
use storage::snapshot::Snapshot;

pub struct TypeManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    iid_generator: &'txn TypeIIDGenerator,
    // TODO: add a shared schema cache
}

impl<'txn, 'storage: 'txn> TypeManager<'txn, 'storage> {
    pub fn new(snapshot: Rc<Snapshot<'storage>>, id_generator: &'txn TypeIIDGenerator) -> TypeManager<'txn, 'storage> {
        TypeManager {
            snapshot: snapshot,
            iid_generator: id_generator,
        }
    }

    fn create_entity_type(&self, label: &str) -> EntityType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_iid = self.iid_generator.take_entity_iid();
            let type_iid_key = type_iid.to_writable_key();
            write_snapshot.put(type_iid_key.clone());
            let (iid_label_index_key, value) = TypeIIDLabelIndex::new(type_iid, label);
            write_snapshot.put_val(iid_label_index_key.to_writable_key(), value.to_bytes());
            let label_iid_index_key = LabelTypeIIDIndex::new(label, type_iid);
            write_snapshot.put_val(label_iid_index_key.to_writable_key(), type_iid_key.bytes().into());
            return EntityType::new(type_iid);
        }
        panic!("Illegal state: create entity type requires write snapshot")
    }

    fn get_entity_type(&self, label: &str) -> EntityType {
        // let prefix = Prefix::Entity.as_bytes();
        // // self.snapshot.iterate_prefix(prefix).map(|(key, value)| Entity::new(ThingEncoder::decideThingIIDSmall))
        // empty()
        todo!()
    }

    fn create_attribute_type(&self, label: &str) -> AttributeType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_iid = self.iid_generator.take_attribute_iid();
            let type_iid_key = type_iid.to_writable_key();
            write_snapshot.put(type_iid_key.clone());
            let (iid_label_index_key, value) = TypeIIDLabelIndex::new(type_iid, label);
            write_snapshot.put_val(iid_label_index_key.to_writable_key(), value.to_bytes());
            let label_iid_index_key = LabelTypeIIDIndex::new(label, type_iid);
            write_snapshot.put_val(label_iid_index_key.to_writable_key(), type_iid_key.bytes().into());
            return AttributeType::new(type_iid);
        }
        panic!("Illegal state: create entity type requires write snapshot")
    }

    // TODO:
    //   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
    //   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious
}

pub struct EntityType {
    iid: TypeIID,
}

impl EntityType {
    pub fn new(iid: TypeIID) -> EntityType {
        if iid.prefix() != Prefix::EntityType.as_id() {
            panic!("Type IID prefix was expected to be EntityType but was {:?}", iid.prefix())
        }
        EntityType { iid: iid }
    }
}

pub struct AttributeType {
    iid: TypeIID,
}

impl AttributeType {
    pub fn new(iid: TypeIID) -> AttributeType {
        if iid.prefix() != Prefix::AttributeType.as_id() {
            panic!("Type IID prefix was expected to be AttributeType but was {:?}", iid.prefix())
        }
        AttributeType { iid: iid }
    }
}
