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

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{AsBytes, Keyable, Prefixed};
use encoding::graph::type_::index::{LabelToTypeIndex, TypeToLabelIndex};
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::TypeVertex;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::layout::prefix::PrefixType;
use encoding::primitive::label::Label;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;

use crate::Type;

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

    pub fn create_entity_type(&self, label_: &Label) -> EntityType {
        let label = label_.name();
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_entity_type_vertex();
            write_snapshot.put(type_vertex.as_storage_key().to_owned());
            self.create_type_indexes(label, &type_vertex);
            return EntityType::new(type_vertex);
        }
        panic!("Illegal state: create type requires write snapshot")
    }

    pub fn get_entity_type(&self, label: &Label) -> Option<EntityType> {
        self.get_type(label, |vertex| EntityType::new(vertex))
    }

    pub fn create_attribute_type(&self, label_: &Label) -> AttributeType {
        let label = &label_.name();
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_attribute_type_vertex();
            write_snapshot.put(type_vertex.as_storage_key().to_owned());
            self.create_type_indexes(label, &type_vertex);
            return AttributeType::new(type_vertex);
        }
        panic!("Illegal state: create type requires write snapshot")
    }

    pub fn get_attribute_type(&self, label: &Label) -> Option<AttributeType> {
        self.get_type(label, |vertex| AttributeType::new(vertex))
    }

    fn get_type<M, U>(&self, label: &Label, mapper: M) -> Option<U> where M: FnOnce(TypeVertex<'static>) -> U {
        let key = LabelToTypeIndex::build(label.name()).into_storage_key();
        self.snapshot.get::<48>(&key).map(|value| {
            mapper(TypeVertex::new(ByteArrayOrRef::Array(value)))
        })
    }

    fn create_type_indexes(&self, label: &str, type_vertex: &TypeVertex) {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let (vertex_label_index_key, value) = TypeToLabelIndex::build_key_value(&type_vertex, label);
            write_snapshot.put_val(vertex_label_index_key.into_storage_key().to_owned(), value.into_bytes().into_owned());

            let label_iid_index_key = LabelToTypeIndex::build(label);
            let type_vertex_value = ByteArray::from(type_vertex.bytes());
            write_snapshot.put_val(label_iid_index_key.into_storage_key().to_owned(), type_vertex_value);
        } else {
            unreachable!("Must be using a write snapshot to create type indexes.")
        }
    }

    // TODO:
    //   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
    //   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious
}

#[derive(Debug, Eq, PartialEq)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> EntityType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> EntityType {
        if vertex.prefix() != PrefixType::EntityType.prefix() {
            panic!("Type IID prefix was expected to be Prefix::EntityType ({:?}) but was {:?}",
                   PrefixType::EntityType.prefix(), vertex.prefix())
        }
        EntityType { vertex: vertex }
    }
}

impl<'a> Type<'a> for EntityType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType {
        if vertex.prefix() != PrefixType::AttributeType.prefix() {
            panic!("Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                   PrefixType::AttributeType.prefix(), vertex.prefix())
        }
        AttributeType { vertex: vertex }
    }
}

impl<'a> Type<'a> for AttributeType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }
}
