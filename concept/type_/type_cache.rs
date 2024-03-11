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

use std::collections::{Bound, BTreeMap, HashMap};
use std::hash::Hash;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use durability::SequenceNumber;
use encoding::{Keyable, Prefixed};
use encoding::graph::type_::edge::{build_edge_sub_forward_prefix, new_edge_sub_forward};
use encoding::graph::type_::property::TypeToLabelProperty;
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::{build_vertex_attribute_type_prefix, build_vertex_entity_type_prefix, build_vertex_relation_type_prefix, is_vertex_attribute_type, is_vertex_entity_type, is_vertex_relation_type, new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, TypeVertex};
use encoding::graph::Typed;
use encoding::layout::prefix::PrefixType;
use encoding::primitive::label::Label;
use encoding::primitive::string::StringBytes;
use resource::constants::encoding::LABEL_SCOPED_NAME_STRING_INLINE;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::key_value::StorageKeyArray;
use storage::MVCCStorage;

use crate::type_::{AttributeTypeAPI, EntityTypeAPI, RelationTypeAPI, TypeAPI};
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::relation_type::RelationType;

// TODO: could/should we slab allocate the schema cache?
pub struct TypeCache {
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityTypeCache>]>,
    relation_types: Box<[Option<RelationTypeCache>]>,
    attribute_types: Box<[Option<AttributeTypeCache>]>,

    entity_types_index_label: HashMap<Label<'static>, EntityType<'static>>,
    relation_types_index_label: HashMap<Label<'static>, RelationType<'static>>,
    attribute_types_index_label: HashMap<Label<'static>, AttributeType<'static>>,
}

#[derive(Debug)]
struct EntityTypeCache {
    type_: EntityType<'static>,
    label: Label<'static>,
    is_root: bool,

    supertype: Option<EntityType<'static>>,
    supertypes: Vec<EntityType<'static>>, // TODO: use smallvec if we want to have some inline
    // subtypes: Vec<EntityType<'static>>, // TODO: use smallvec
    // subtypes_direct: Vec<EntityType<'static>>, // TODO: use smallvec

    // owns
    // owns_direct

    // ...
}

struct RelationTypeCache {
    type_: RelationType<'static>,
    label: Label<'static>,
    is_root: bool,

    supertype: Option<RelationType<'static>>,
    supertypes: Vec<RelationType<'static>>, // TODO: use smallvec
    // subtypes: Vec<RelationType<'static>>, // TODO: use smallvec
    // subtypes_direct: Vec<RelationType<'static>>, // TODO: use smallvec
}

struct AttributeTypeCache {
    type_: AttributeType<'static>,
    label: Label<'static>,
    is_root: bool,

    supertype: Option<AttributeType<'static>>,
    supertypes: Vec<AttributeType<'static>>, // TODO: use smallvec
    // subtypes: Vec<AttributeType<'static>>, // TODO: use smallvec
    // subtypes_direct: Vec<AttributeType<'static>>, // TODO: use smallvec
}

impl TypeCache {
    pub fn new(storage: &MVCCStorage, open_sequence_number: SequenceNumber) -> Self {
        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let snapshot = storage.open_snapshot_read_at(open_sequence_number);

        let type_label_properties = snapshot.iterate_prefix(TypeToLabelProperty::build_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>().unwrap();

        let mut entity_types_data = snapshot.iterate_prefix(build_vertex_entity_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>().unwrap();
        let max_entity_id = entity_types_data.iter().filter_map(|(key, _)| {
            if is_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                Some(new_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))).type_id().as_u16())
            } else { None }
        }).max().unwrap_or(0);
        let entity_type_caches = Self::create_entity_caches(&entity_types_data, &type_label_properties, max_entity_id);
        let entity_type_index_labels = entity_type_caches.iter().filter_map(|entry|
            entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone()))
        ).collect();
        dbg!(&entity_type_index_labels);

        let mut relation_types_data = snapshot.iterate_prefix(build_vertex_relation_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>().unwrap();
        let max_relation_id = relation_types_data.iter().filter_map(|(key, _)| {
            if is_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                Some(new_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))).type_id().as_u16())
            } else { None }
        }).max().unwrap_or(0);
        let relation_type_caches = Self::create_relation_caches(&relation_types_data, &type_label_properties, max_relation_id);
        let relation_type_index_labels = relation_type_caches.iter().filter_map(|entry|
            entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone()))
        ).collect();

        let mut attribute_types_data = snapshot.iterate_prefix(build_vertex_attribute_type_prefix())
            .collect_cloned_bmap::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>().unwrap();
        let max_attribute_id = attribute_types_data.iter().filter_map(|(key, _)| {
            if is_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                Some(new_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))).type_id().as_u16())
            } else { None }
        }).max().unwrap_or(0);
        let attribute_type_caches = Self::create_attribute_caches(&attribute_types_data, &type_label_properties, max_attribute_id);
        let attribute_type_index_labels = attribute_type_caches.iter().filter_map(|entry|
            entry.as_ref().map(|cache| (cache.label.clone(), cache.type_.clone()))
        ).collect();

        TypeCache {
            open_sequence_number: open_sequence_number,
            entity_types: entity_type_caches,
            relation_types: relation_type_caches,
            attribute_types: attribute_type_caches,

            entity_types_index_label: entity_type_index_labels,
            relation_types_index_label: relation_type_index_labels,
            attribute_types_index_label: attribute_type_index_labels,
        }
    }

    fn create_entity_caches(entity_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                            type_label_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                            max_entity: u16) -> Box<[Option<EntityTypeCache>]> {
        let mut caches: Box<[Option<EntityTypeCache>]> = (0..=max_entity).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in entity_types_data.iter() {
            if is_vertex_entity_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let entity_type = EntityType::new(new_vertex_entity_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(entity_type.vertex()).as_u16();

                let label = Self::read_type_label(type_label_properties, entity_type.vertex().clone());
                let is_root = label == Root::Entity.label();
                let supertype = Self::read_supertype_vertex(entity_types_data, entity_type.vertex().clone().into_owned()).map(|v| EntityType::new(v));
                let cache = EntityTypeCache {
                    type_: entity_type,
                    label: label,
                    is_root: is_root,
                    supertype: supertype,
                    supertypes: Vec::new(),
                };
                let i = type_index as usize;
                caches[i] = Some(cache);
                dbg!(&caches);
            }
        }
        Self::set_entity_supertypes_transitive(&mut caches);
        caches
    }

    fn set_entity_supertypes_transitive(entity_type_caches: &mut Box<[Option<EntityTypeCache>]>) {
        for index in (0..entity_type_caches.len()) {
            if entity_type_caches[index].is_none() { continue; }
            let mut supertype = entity_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache = Self::get_entity_type_cache(entity_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().map(|t| t.clone());
                entity_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_relation_caches(relation_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                              type_label_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                              max_relation: u16) -> Box<[Option<RelationTypeCache>]> {
        let mut caches: Box<[Option<RelationTypeCache>]> = (0..=max_relation).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in relation_types_data.iter() {
            if is_vertex_relation_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let relation_type = RelationType::new(new_vertex_relation_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(relation_type.vertex()).as_u16();

                let label = Self::read_type_label(type_label_properties, relation_type.vertex().clone());
                let is_root = label == Root::Relation.label();
                let supertype = Self::read_supertype_vertex(relation_types_data, relation_type.vertex().clone().into_owned()).map(|v| RelationType::new(v));
                let cache = RelationTypeCache {
                    type_: relation_type,
                    label: label,
                    is_root: is_root,
                    supertype: supertype,
                    supertypes: Vec::new(),
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_relation_supertypes_transitive(&mut caches);
        caches
    }

    fn set_relation_supertypes_transitive(relation_type_caches: &mut Box<[Option<RelationTypeCache>]>) {
        for index in (0..relation_type_caches.len()) {
            if relation_type_caches[index].is_none() { continue; }
            let mut supertype = relation_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache = Self::get_relation_type_cache(relation_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().map(|t| t.clone());
                relation_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn create_attribute_caches(attribute_types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                               type_label_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                               max_attribute: u16) -> Box<[Option<AttributeTypeCache>]> {
        let mut caches: Box<[Option<AttributeTypeCache>]> = (0..=max_attribute).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        for (key, _) in attribute_types_data.iter() {
            if is_vertex_attribute_type(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let attribute_type = AttributeType::new(new_vertex_attribute_type(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                let type_index = Typed::type_id(attribute_type.vertex()).as_u16();

                let label = Self::read_type_label(type_label_properties, attribute_type.vertex().clone());
                let is_root = label == Root::Attribute.label();
                let supertype = Self::read_supertype_vertex(attribute_types_data, attribute_type.vertex().clone().into_owned()).map(|v| AttributeType::new(v));
                let cache = AttributeTypeCache {
                    type_: attribute_type,
                    label: label,
                    is_root: is_root,
                    supertype: supertype,
                    supertypes: Vec::new(),
                };
                caches[type_index as usize] = Some(cache);
            }
        }
        Self::set_attribute_supertypes_transitive(&mut caches);
        caches
    }

    fn set_attribute_supertypes_transitive(attribute_type_caches: &mut Box<[Option<AttributeTypeCache>]>) {
        for index in (0..attribute_type_caches.len()) {
            if attribute_type_caches[index].is_none() { continue; }
            let mut supertype = attribute_type_caches[index].as_ref().unwrap().supertype.clone();
            while let Some(current_supertype) = supertype {
                let next_super_cache = Self::get_attribute_type_cache(attribute_type_caches, current_supertype.vertex().clone()).unwrap();
                supertype = next_super_cache.supertype.as_ref().map(|t| t.clone());
                attribute_type_caches[index].as_mut().unwrap().supertypes.push(current_supertype);
            }
        }
    }

    fn read_type_label(type_label_properties: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                       type_vertex: TypeVertex<'_>) -> Label<'static> {
        let property = TypeToLabelProperty::build(type_vertex);
        type_label_properties.get(&property.into_storage_key().to_owned_array())
            .map(|bytes| Label::parse_from(StringBytes::new(ByteArrayOrRef::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(ByteReference::from(bytes)))))
            .unwrap()
    }

    fn read_supertype_vertex(types_data: &BTreeMap<StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>>,
                             type_vertex: TypeVertex<'static>) -> Option<TypeVertex<'static>> {
        let edge_prefix = build_edge_sub_forward_prefix(type_vertex).to_owned_array();
        let mut edges = types_data.range::<[u8], _>((Bound::Included(edge_prefix.bytes()), Bound::Unbounded))
            .take_while(|(key, _)| key.bytes().starts_with(edge_prefix.bytes()));
        let supertype = edges.next().map(|(key, _)| new_edge_sub_forward(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))).to().into_owned());
        debug_assert!(edges.next().is_none());
        supertype
    }

    pub(crate) fn get_entity_type(&self, label: &Label<'_>) -> Option<EntityType<'static>> {
        self.entity_types_index_label.get(label).map(|t| t.clone())
    }

    pub(crate) fn get_relation_type(&self, label: &Label<'_>) -> Option<RelationType<'static>> {
        self.relation_types_index_label.get(label).map(|t| t.clone())
    }

    pub(crate) fn get_attribute_type(&self, label: &Label<'_>) -> Option<AttributeType<'static>> {
        self.attribute_types_index_label.get(label).map(|t| t.clone())
    }

    pub(crate) fn get_entity_type_supertype<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> Option<EntityType<'static>> {
        Self::get_entity_type_cache(&self.entity_types, entity_type.vertex().clone()).unwrap().supertype.as_ref().map(|t| t.clone())
    }

    pub(crate) fn get_relation_type_supertype<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> Option<RelationType<'static>> {
        Self::get_relation_type_cache(&self.relation_types, relation_type.vertex().clone()).unwrap().supertype.as_ref().map(|t| t.clone())
    }

    pub(crate) fn get_attribute_type_supertype<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> Option<AttributeType<'static>> {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.vertex().clone()).unwrap().supertype.as_ref().map(|t| t.clone())
    }

    pub(crate) fn get_entity_type_supertypes<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> &Vec<EntityType<'static>> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.vertex().clone()).unwrap().supertypes
    }

    pub(crate) fn get_relation_type_supertypes<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> &Vec<RelationType<'static>> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.vertex().clone()).unwrap().supertypes
    }

    pub(crate) fn get_attribute_type_supertypes<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> &Vec<AttributeType<'static>> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.vertex().clone()).unwrap().supertypes
    }

    pub(crate) fn get_entity_type_label<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> &Label<'static> {
        &Self::get_entity_type_cache(&self.entity_types, entity_type.vertex().clone()).unwrap().label
    }

    pub(crate) fn get_relation_type_label<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> &Label<'static> {
        &Self::get_relation_type_cache(&self.relation_types, relation_type.vertex().clone()).unwrap().label
    }

    pub(crate) fn get_attribute_type_label<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> &Label<'static> {
        &Self::get_attribute_type_cache(&self.attribute_types, attribute_type.vertex().clone()).unwrap().label
    }

    pub(crate) fn get_entity_type_is_root<'this>(&'this self, entity_type: impl EntityTypeAPI<'static>) -> bool {
        Self::get_entity_type_cache(&self.entity_types, entity_type.vertex().clone()).unwrap().is_root
    }

    pub(crate) fn get_relation_type_is_root<'this>(&'this self, relation_type: impl RelationTypeAPI<'static>) -> bool {
        Self::get_relation_type_cache(&self.relation_types, relation_type.vertex().clone()).unwrap().is_root
    }

    pub(crate) fn get_attribute_type_is_root<'this>(&'this self, attribute_type: impl AttributeTypeAPI<'static>) -> bool {
        Self::get_attribute_type_cache(&self.attribute_types, attribute_type.vertex().clone()).unwrap().is_root
    }

    fn get_entity_type_cache<'c>(entity_type_caches: &'c Box<[Option<EntityTypeCache>]>, type_vertex: TypeVertex<'_>) -> Option<&'c EntityTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexEntityType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        entity_type_caches[as_u16 as usize].as_ref()
    }

    fn get_relation_type_cache<'c>(relation_type_caches: &'c Box<[Option<RelationTypeCache>]>, type_vertex: TypeVertex<'_>) -> Option<&'c RelationTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRelationType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        relation_type_caches[as_u16 as usize].as_ref()
    }

    fn get_attribute_type_cache<'c>(attribute_type_caches: &'c Box<[Option<AttributeTypeCache>]>, type_vertex: TypeVertex<'_>) -> Option<&'c AttributeTypeCache> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexAttributeType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        attribute_type_caches[as_u16 as usize].as_ref()
    }
}


