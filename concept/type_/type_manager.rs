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


use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::edge::{build_edge_sub, build_edge_sub_prefix, build_edge_sub_reverse, new_edge_sub};
use encoding::graph::type_::index::LabelToTypeVertexIndex;
use encoding::graph::type_::property::{build_property_type_annotation_abstract, build_property_type_label};
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::{new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, TypeVertex};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::primitive::label::Label;
use encoding::primitive::string::StringBytes;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::encoding::LABEL_SCOPED_NAME_STRING_INLINE;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;

use crate::type_::{AttributeTypeAPI, EntityTypeAPI, RelationTypeAPI, TypeAPI};
use crate::type_::annotation::AnnotationAbstract;
use crate::type_::attribute_type::{AttributeType, AttributeTypeAnnotation};
use crate::type_::entity_type::{EntityType, EntityTypeAnnotation};
use crate::type_::relation_type::{RelationType, RelationTypeAnnotation};
use crate::type_::type_cache::TypeCache;

pub struct TypeManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    vertex_generator: &'txn TypeVertexGenerator,
    type_cache: Option<Arc<TypeCache>>,
}


// TODO:
//   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
//   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious


impl<'txn, 'storage: 'txn> TypeManager<'txn, 'storage> {
    pub fn new(snapshot: Rc<Snapshot<'storage>>, vertex_generator: &'txn TypeVertexGenerator, schema_cache: Option<Arc<TypeCache>>) -> TypeManager<'txn, 'storage> {
        TypeManager {
            snapshot: snapshot,
            vertex_generator,
            type_cache: schema_cache,
        }
    }

    pub fn initialise_types(storage: &mut MVCCStorage, vertex_generator: &TypeVertexGenerator) {
        let snapshot = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
        {
            let type_manager = TypeManager::new(snapshot.clone(), vertex_generator, None);
            type_manager.create_entity_type(&Root::Entity.label(), true);
            type_manager.create_relation_type(&Root::Relation.label(), true);
            type_manager.create_attribute_type(&Root::Attribute.label(), true);
        }
        // TODO: handle error properly
        if let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() {
            write_snapshot.commit().unwrap();
        } else {
            panic!()
        }
    }

    pub fn get_entity_type<'this>(&'this self, label: &Label) -> Option<EntityType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_entity_type(label)
        } else {
            self.get_labelled_type(label, |bytes| EntityType::new(new_vertex_entity_type(bytes)))
        }
    }

    pub fn get_relation_type<'this>(&'this self, label: &Label) -> Option<RelationType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_relation_type(label)
        } else {
            self.get_labelled_type(label, |bytes| RelationType::new(new_vertex_relation_type(bytes)))
        }
    }

    pub fn get_attribute_type(&self, label: &Label) -> Option<AttributeType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_attribute_type(label)
        } else {
            self.get_labelled_type(label, |bytes| AttributeType::new(new_vertex_attribute_type(bytes)))
        }
    }

    fn get_labelled_type<M, U>(&self, label: &Label, mapper: M) -> Option<U> where M: FnOnce(ByteArrayOrRef<'static, BUFFER_KEY_INLINE>) -> U {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        self.snapshot.get::<{ BUFFER_KEY_INLINE }>(key.as_reference()).map(|value| {
            mapper(ByteArrayOrRef::Array(value))
        })
    }

    pub(crate) fn get_entity_type_supertype<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> Option<EntityType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_entity_type_supertype(entity_type)
        } else {
            // TODO: handle possible errors
            self.snapshot.iterate_prefix(build_edge_sub_prefix(entity_type.vertex().clone().into_owned())).first_cloned().unwrap()
                .map(|(key, _)| EntityType::new(new_edge_sub(key.into_byte_array_or_ref()).to().into_owned()))
        }
    }

    pub(crate) fn get_relation_type_supertype<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> Option<RelationType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_relation_type_supertype(relation_type)
        } else {
            // TODO: handle possible errors
            self.snapshot.iterate_prefix(build_edge_sub_prefix(relation_type.vertex().clone())).first_cloned().unwrap()
                .map(|(key, _)| RelationType::new(new_edge_sub(key.into_byte_array_or_ref()).to().into_owned()))
        }
    }

    pub(crate) fn get_attribute_type_supertype<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> Option<AttributeType<'static>> {
        if let Some(cache) = &self.type_cache {
            cache.get_attribute_type_supertype(attribute_type)
        } else {
            // TODO: handle possible errors
            self.snapshot.iterate_prefix(build_edge_sub_prefix(attribute_type.vertex().clone())).first_cloned().unwrap()
                .map(|(key, _)| AttributeType::new(new_edge_sub(key.into_byte_array_or_ref()).to().into_owned()))
        }
    }

    // WARN: supertypes currently do NOT include themselves
    pub(crate) fn get_entity_type_supertypes<'this>(&'this self, entity_type: EntityType<'static>) -> MaybeOwns<'this, Vec<EntityType<'static>>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_entity_type_supertypes(entity_type))
        } else {
            let mut supertypes = Vec::new();
            let mut supertype = self.get_storage_supertype(entity_type.vertex().clone().into_owned());
            while supertype.is_some() {
                let super_entity = EntityType::new(supertype.as_ref().unwrap().clone());
                supertype = self.get_storage_supertype(super_entity.vertex().clone());
                supertypes.push(super_entity);
            }
            MaybeOwns::owned(supertypes)
        }
    }

    pub(crate) fn get_relation_type_supertypes<'this>(&'this self, relation_type: RelationType<'static>) -> MaybeOwns<'this, Vec<RelationType<'static>>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_relation_type_supertypes(relation_type))
        } else {
            let mut supertypes = Vec::new();
            let mut supertype = self.get_storage_supertype(relation_type.vertex().clone().into_owned());
            while supertype.is_some() {
                let super_relation = RelationType::new(supertype.as_ref().unwrap().clone());
                supertype = self.get_storage_supertype(super_relation.vertex().clone());
                supertypes.push(super_relation);
            }
            MaybeOwns::owned(supertypes)
        }
    }

    pub(crate) fn get_attribute_type_supertypes<'this>(&'this self, attribute_type: AttributeType<'static>) -> MaybeOwns<'this, Vec<AttributeType<'static>>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_attribute_type_supertypes(attribute_type))
        } else {
            let mut supertypes = Vec::new();
            let mut supertype = self.get_storage_supertype(attribute_type.vertex().clone().into_owned());
            while supertype.is_some() {
                let super_attribute = AttributeType::new(supertype.as_ref().unwrap().clone());
                supertype = self.get_storage_supertype(super_attribute.vertex().clone());
                supertypes.push(super_attribute);
            }
            MaybeOwns::owned(supertypes)
        }
    }

    pub fn create_entity_type(&self, label: &Label, is_root: bool) -> EntityType<'static> {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_entity_type();
            write_snapshot.put(type_vertex.as_storage_key().into_owned_array());
            self.set_storage_label(type_vertex.clone().into_owned(), label);
            if !is_root {
                self.set_storage_supertype(type_vertex.clone().into_owned(), self.get_entity_type(&Root::Entity.label()).unwrap().vertex().clone());
            }
            EntityType::new(type_vertex)
        } else {
            // TODO: this should not crash the server, and be handled as an Error instead
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub fn create_relation_type(&self, label: &Label, is_root: bool) -> RelationType<'static> {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_relation_type();
            write_snapshot.put(type_vertex.as_storage_key().into_owned_array());
            self.set_storage_label(type_vertex.clone().into_owned(), label);
            if !is_root {
                self.set_storage_supertype(type_vertex.clone().into_owned(), self.get_relation_type(&Root::Relation.label()).unwrap().vertex().clone());
            }
            RelationType::new(type_vertex)
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub fn create_attribute_type(&self, label: &Label, is_root: bool) -> AttributeType<'static> {
        // TODO: validate type doesn't exist already
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let type_vertex = self.vertex_generator.take_attribute_type();
            write_snapshot.put(type_vertex.as_storage_key().into_owned_array());
            self.set_storage_label(type_vertex.clone(), label);
            if !is_root {
                self.set_storage_supertype(type_vertex.clone(), self.get_attribute_type(&Root::Attribute.label()).unwrap().vertex().clone());
            }
            AttributeType::new(type_vertex)
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    pub(crate) fn get_entity_type_is_root(&self, entity_type: impl EntityTypeAPI<'static>) -> bool {
        if let Some(cache) = &self.type_cache {
            cache.get_entity_type_is_root(entity_type)
        } else {
            entity_type.get_label(self).deref() == &Root::Entity.label()
        }
    }

    pub(crate) fn get_relation_type_is_root(&self, relation_type: impl RelationTypeAPI<'static>) -> bool {
        if let Some(cache) = &self.type_cache {
            cache.get_relation_type_is_root(relation_type)
        } else {
            relation_type.get_label(self).deref() == &Root::Relation.label()
        }
    }

    pub(crate) fn get_attribute_type_is_root(&self, attribute_type: impl AttributeTypeAPI<'static>) -> bool {
        if let Some(cache) = &self.type_cache {
            cache.get_attribute_type_is_root(attribute_type)
        } else {
            attribute_type.get_label(self).deref() == &Root::Attribute.label()
        }
    }

    pub(crate) fn get_entity_type_label<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> MaybeOwns<'this, Label<'static>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_entity_type_label(entity_type))
        } else {
            MaybeOwns::owned(self.get_storage_label(entity_type.vertex().clone()).unwrap())
        }
    }

    pub(crate) fn get_relation_type_label<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> MaybeOwns<'this, Label<'static>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_relation_type_label(relation_type))
        } else {
            MaybeOwns::owned(self.get_storage_label(relation_type.vertex().clone()).unwrap())
        }
    }

    pub(crate) fn get_attribute_type_label<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> MaybeOwns<'this, Label<'static>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_attribute_type_label(attribute_type))
        } else {
            MaybeOwns::owned(self.get_storage_label(attribute_type.vertex().clone()).unwrap())
        }
    }

    pub(crate) fn get_entity_type_annotations<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> MaybeOwns<'this, Vec<EntityTypeAnnotation>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_entity_type_annotations(entity_type))
        } else {
            let mut annotations = Vec::new();
            self.get_storage_vertex_annotation_abstract(entity_type.vertex().clone())
                .map(|ann| annotations.push(EntityTypeAnnotation::from(ann)));
            MaybeOwns::owned(annotations)
        }
    }

    pub(crate) fn get_relation_type_annotations<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> MaybeOwns<'this, Vec<RelationTypeAnnotation>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_relation_type_annotations(relation_type))
        } else {
            let mut annotations = Vec::new();
            self.get_storage_vertex_annotation_abstract(relation_type.vertex().clone())
                .map(|ann| annotations.push(RelationTypeAnnotation::Abstract(ann)));
            MaybeOwns::owned(annotations)
        }
    }

    pub(crate) fn get_attribute_type_annotations<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> MaybeOwns<'this, Vec<AttributeTypeAnnotation>> {
        if let Some(cache) = &self.type_cache {
            MaybeOwns::borrowed(cache.get_attribute_type_annotations(attribute_type))
        } else {
            let mut annotations = Vec::new();
            self.get_storage_vertex_annotation_abstract(attribute_type.vertex().clone())
                .map(|ann| annotations.push(AttributeTypeAnnotation::Abstract(ann)));
            MaybeOwns::owned(annotations)
        }
    }

    // --- storage operations ---

    fn get_storage_label(&self, owner: TypeVertex<'_>) -> Option<Label<'static>> {
        let key = build_property_type_label(owner.clone().into_owned());
        self.snapshot.get_mapped(key.into_storage_key().as_reference(), |reference| {
            let value = StringBytes::new(ByteArrayOrRef::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
            Label::parse_from(value)
        })
    }

    pub(crate) fn set_storage_label(&self, owner: TypeVertex<'static>, label: &Label) {
        self.may_delete_storage_label(owner.clone());
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let vertex_to_label_key = build_property_type_label(owner.clone().into_owned());
            let label_value = ByteArray::from(label.scoped_name.bytes());
            write_snapshot.put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

            let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
            let vertex_value = ByteArray::from(owner.bytes());
            write_snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
        } else {
            panic!("Illegal state: creating types requires write snapshot")
        }
    }

    fn may_delete_storage_label(&self, owner: TypeVertex<'_>) {
        let existing_label = self.get_storage_label(owner.clone());
        if let Some(label) = existing_label {
            if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
                let vertex_to_label_key = build_property_type_label(owner.clone().into_owned());
                write_snapshot.delete(vertex_to_label_key.into_storage_key().into_owned_array());
                let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
                write_snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
            } else {
                panic!("Illegal state: creating types requires write snapshot")
            }
        }
    }

    fn get_storage_supertype(&self, subtype: TypeVertex<'static>) -> Option<TypeVertex<'static>> {
        // TODO: handle possible errors
        self.snapshot.iterate_prefix(build_edge_sub_prefix(subtype.clone()))
            .first_cloned().unwrap().map(|(key, _)| new_edge_sub(key.into_byte_array_or_ref()).to().into_owned())
    }

    pub(crate) fn set_storage_supertype(&self, subtype: TypeVertex<'static>, supertype: TypeVertex<'static>) {
        self.may_delete_storage_supertype(subtype.clone());
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let sub = build_edge_sub(subtype.clone(), supertype.clone());
            write_snapshot.put(sub.into_storage_key().into_owned_array());
            let sub_reverse = build_edge_sub_reverse(supertype, subtype);
            write_snapshot.put(sub_reverse.into_storage_key().into_owned_array());
        } else {
            panic!("Illegal state: creating supertype edge requires write snapshot")
        }
    }

    fn may_delete_storage_supertype(&self, subtype: TypeVertex<'static>) {
        let supertype = self.get_storage_supertype(subtype.clone());
        if let Some(supertype) = supertype {
            if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
                let sub = build_edge_sub(subtype.clone(), supertype.clone());
                write_snapshot.delete(sub.into_storage_key().into_owned_array());
                let sub_reverse = build_edge_sub_reverse(supertype, subtype);
                write_snapshot.delete(sub_reverse.into_storage_key().into_owned_array());
            } else {
                panic!("Illegal state: deleting supertype edge requires write snapshot")
            }
        }
    }

    fn get_storage_vertex_annotation_abstract(&self, vertex: TypeVertex<'static>) -> Option<AnnotationAbstract> {
        self.snapshot.get_mapped(build_property_type_annotation_abstract(vertex).into_storage_key().as_reference(), |bytes| {
            AnnotationAbstract::new()
        })
    }

    pub(crate) fn set_storage_annotation_abstract(&self, vertex: TypeVertex<'static>) {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let annotation_property = build_property_type_annotation_abstract(vertex);
            write_snapshot.put(annotation_property.into_storage_key().into_owned_array());
        } else {
            panic!("Illegal state: setting annotation requires write snapshot.")
        }
    }

    pub(crate) fn delete_storage_annotation_abstract(&self, vertex: TypeVertex<'static>) {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let annotation_property = build_property_type_annotation_abstract(vertex);
            write_snapshot.delete(annotation_property.into_storage_key().into_owned_array());
        } else {
            panic!("Illegal state: deleting annotation requires write snapshot.")
        }
    }
}