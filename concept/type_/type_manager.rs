/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


use std::rc::Rc;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::property::{LabelToTypeProperty, TypeToLabelProperty};
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::{new_attribute_type_vertex, new_entity_type_vertex, TypeVertex};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::primitive::label::Label;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use resource::constants::snapshot::{BUFFER_KEY_INLINE};

pub struct TypeManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    vertex_generator: &'txn TypeVertexGenerator,
    // TODO: add a shared schema cache
}

impl<'txn, 'storage: 'txn> TypeManager<'txn, 'storage> {
    pub fn new(snapshot: Rc<Snapshot<'storage>>, vertex_generator: &'txn TypeVertexGenerator) -> TypeManager<'txn, 'storage> {
        TypeManager {
            snapshot: snapshot,
            vertex_generator,
        }
    }

    pub fn initialise_types(storage: &mut MVCCStorage, vertex_generator: &TypeVertexGenerator) {
        let snapshot = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
        {
            let type_manager = TypeManager::new(snapshot.clone(), vertex_generator);
            type_manager.create_entity_type(&Root::Entity.label());
            type_manager.create_attribute_type(&Root::Attribute.label());
        }
        // TODO: handle errors
        if let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() {
            write_snapshot.commit();
        } else {
            panic!()
        }
    }

    pub fn create_entity_type(&self, label: &Label) -> EntityType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_entity_type_vertex();
            write_snapshot.put(type_vertex.as_storage_key().to_owned_array());
            self.create_type_indexes(label, &type_vertex);
            return EntityType::new(type_vertex);
        }
        panic!("Illegal state: create type requires write snapshot")
    }

    pub fn get_entity_type(&self, label: &Label) -> Option<EntityType> {
        self.get_type(label, |bytes| EntityType::new(new_entity_type_vertex(bytes)))
    }

    pub fn create_attribute_type(&self, label: &Label) -> AttributeType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_attribute_type_vertex();
            write_snapshot.put(type_vertex.as_storage_key().to_owned_array());
            self.create_type_indexes(label, &type_vertex);
            return AttributeType::new(type_vertex);
        }
        panic!("Illegal state: create type requires write snapshot")
    }

    pub fn get_attribute_type(&self, label: &Label) -> Option<AttributeType> {
        self.get_type(label, |bytes| AttributeType::new(new_attribute_type_vertex(bytes)))
    }

    fn get_type<M, U>(&self, label: &Label, mapper: M) -> Option<U> where M: FnOnce(ByteArrayOrRef<'static, BUFFER_KEY_INLINE>) -> U {
        let key = LabelToTypeProperty::build(label).into_storage_key();
        self.snapshot.get::<{ BUFFER_KEY_INLINE }>(key.as_reference()).map(|value| {
            mapper(ByteArrayOrRef::Array(value))
        })
    }

    fn create_type_indexes(&self, label: &Label, type_vertex: &TypeVertex) {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let vertex_label_index_key = TypeToLabelProperty::build(&type_vertex);
            let value = ByteArray::from(label.scoped_name.bytes());
            write_snapshot.put_val(vertex_label_index_key.into_storage_key().to_owned_array(), value);

            let label_iid_index_key = LabelToTypeProperty::build(label);
            let type_vertex_value = ByteArray::from(type_vertex.bytes());
            write_snapshot.put_val(label_iid_index_key.into_storage_key().to_owned_array(), type_vertex_value);
        } else {
            unreachable!("Must be using a write snapshot to create type indexes.")
        }
    }

    // TODO:
    //   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
    //   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious
}
