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
use std::sync::Arc;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::property::{LabelToTypeProperty, TypeToLabelProperty};
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::{new_attribute_type_vertex, new_entity_type_vertex, new_relation_type_vertex, TypeVertex};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::primitive::label::Label;
use encoding::primitive::string::StringBytes;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::encoding::LABEL_SCOPED_NAME_STRING_INLINE;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;

use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::relation_type::RelationType;
use crate::type_::type_cache::TypeCache;
use crate::type_::{EntityTypeAPI, TypeAPI};

pub struct TypeManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    vertex_generator: &'txn TypeVertexGenerator,
    schema_cache: Option<Arc<TypeCache>>,
}

impl<'txn, 'storage: 'txn> TypeManager<'txn, 'storage> {
    pub fn new(snapshot: Rc<Snapshot<'storage>>, vertex_generator: &'txn TypeVertexGenerator, schema_cache: Option<Arc<TypeCache>>) -> TypeManager<'txn, 'storage> {
        TypeManager {
            snapshot: snapshot,
            vertex_generator,
            schema_cache: schema_cache,
        }
    }

    pub fn initialise_types(storage: &mut MVCCStorage, vertex_generator: &TypeVertexGenerator) {
        let snapshot = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
        {
            let type_manager = TypeManager::new(snapshot.clone(), vertex_generator, None);
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

    pub fn get_entity_type<'this>(&'this self, label: &Label) -> MaybeOwns<'this, EntityType<'static>> {
        self.get_labelled_type(label, |bytes| {
            let vertex = new_entity_type_vertex(bytes);
            if let Some(cache) = &self.schema_cache {
                MaybeOwns::borrowed(cache.get_entity_type(vertex))
            } else {
                MaybeOwns::owned(EntityType::new(vertex))
            }
        }).unwrap()
    }

    pub fn get_relation_type<'this>(&'this self, label: &Label) -> MaybeOwns<'this, RelationType<'static>> {
        self.get_labelled_type(label, |bytes| {
            let vertex = new_relation_type_vertex(bytes);
            if let Some(cache) = &self.schema_cache {
                MaybeOwns::borrowed(cache.get_relation_type(vertex))
            } else {
                MaybeOwns::owned(RelationType::new(vertex))
            }
        }).unwrap()
    }

    pub fn get_attribute_type<'this>(&'this self, label: &Label) -> MaybeOwns<'this, AttributeType<'static>> {
        self.get_labelled_type(label, |bytes| {
            let vertex = new_attribute_type_vertex(bytes);
            if let Some(cache) = &self.schema_cache {
                MaybeOwns::borrowed(cache.get_attribute_type(vertex))
            } else {
                MaybeOwns::owned(AttributeType::new(vertex))
            }
        }).unwrap()
    }

    fn get_labelled_type<M, U>(&self, label: &Label, mapper: M) -> Option<U> where M: FnOnce(ByteArrayOrRef<'static, BUFFER_KEY_INLINE>) -> U {
        let key = LabelToTypeProperty::build(label).into_storage_key();
        self.snapshot.get::<{ BUFFER_KEY_INLINE }>(key.as_reference()).map(|value| {
            mapper(ByteArrayOrRef::Array(value))
        })
    }

    pub(crate) fn get_entity_type_supertype<'this>(&'this self, entity_type: &impl EntityTypeAPI<'this>) -> Option<MaybeOwns<'this, EntityType<'static>>> {
        // if let Some(cache) = &self.schema_cache {
        //     MaybeOwns::borrowed(cache.get_entity_type_supertype(vertex))
        // } else {
        //     MaybeOwns::owned(AttributeType::new(vertex))
        // }
        todo!()
    }

    pub fn create_entity_type(&self, label: &Label) -> EntityType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_entity_type();
            write_snapshot.put(type_vertex.as_storage_key().to_owned_array());
            self.set_storage_label(&type_vertex, label);
            EntityType::new(type_vertex)
        } else {
            // TODO: this should not crash the server, and be handled as an Error instead
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub fn create_relation_type(&self, label: &Label) -> RelationType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_relation_type();
            write_snapshot.put(type_vertex.as_storage_key().to_owned_array());
            self.set_storage_label(&type_vertex, label);
            RelationType::new(type_vertex)
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub fn create_attribute_type(&self, label: &Label) -> AttributeType {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_attribute_type();
            write_snapshot.put(type_vertex.as_storage_key().to_owned_array());
            self.set_storage_label(&type_vertex, label);
            AttributeType::new(type_vertex)
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }


    // TODO:
    //   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
    //   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious


    pub(crate) fn get_storage_label(&self, owner: &TypeVertex<'_>) -> Option<Label<'static>> {
        let key = TypeToLabelProperty::build(owner);
        self.snapshot.get_mapped(key.into_storage_key().as_reference(), |reference| {
            let value = StringBytes::new(ByteArrayOrRef::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
            let as_str = value.decode();
            let mut splits = as_str.split(":");
            let first = splits.next().unwrap();
            if let Some(second) = splits.next() {
                Label::build_scoped(first, second)
            } else {
                Label::build(first)
            }
        })
    }

    pub(crate) fn set_storage_label(&self, owner: &TypeVertex<'_>, label: &Label) {
        self.may_delete_storage_label(owner);
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let vertex_to_label_key = TypeToLabelProperty::build(owner);
            let label_value = ByteArray::from(label.scoped_name.bytes());
            write_snapshot.put_val(vertex_to_label_key.into_storage_key().to_owned_array(), label_value);

            let label_to_vertex_key = LabelToTypeProperty::build(label);
            let vertex_value = ByteArray::from(owner.bytes());
            write_snapshot.put_val(label_to_vertex_key.into_storage_key().to_owned_array(), vertex_value);
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub(crate) fn may_delete_storage_label(&self, owner: &TypeVertex<'_>) {
        let existing_label = self.get_storage_label(owner);
        if let Some(label) = existing_label {
            if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
                let vertex_to_label_key = TypeToLabelProperty::build(owner);
                write_snapshot.delete(vertex_to_label_key.into_storage_key().to_owned_array());
                let label_to_vertex_key = LabelToTypeProperty::build(&label);
                write_snapshot.delete(label_to_vertex_key.into_storage_key().to_owned_array());
            } else {
                panic!("Illegal state: creating types requires write snapshot")
            }
        }
    }
}
