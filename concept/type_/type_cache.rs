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

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use durability::SequenceNumber;
use encoding::graph::type_::edge::{is_sub_edge_forward, new_sub_edge_forward};
use encoding::graph::type_::vertex::{build_attribute_type_vertex_prefix, build_entity_type_vertex_prefix, build_relation_type_vertex_prefix, is_attribute_type_vertex, is_entity_type_vertex, is_relation_type_vertex, new_attribute_type_vertex, new_entity_type_vertex, new_relation_type_vertex, TypeVertex};
use encoding::graph::Typed;
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::key_value::StorageKeyArray;
use storage::MVCCStorage;

use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::relation_type::RelationType;
use crate::type_::TypeAPI;

// TODO: could we slab allocate the schema cache? It is going to be a very hot region of memory
pub struct TypeCache {
    storage: Arc<MVCCStorage>,
    open_sequence_number: SequenceNumber,

    // Types that are borrowable and returned from the cache
    entity_types: Box<[Option<EntityType<'static>>]>,
    relation_types: Box<[Option<RelationType<'static>>]>,
    attribute_types: Box<[Option<AttributeType<'static>>]>,

    // // Supertypes indexed into the set of Types
    // entity_types_supertypes: Box<[Option<TypeVertex<'static>>]>,
    // relation_type_supertypes: Box<[Option<TypeVertex<'static>>]>,
    // attribute_types_supertypes: Box<[Option<TypeVertex<'static>>]>,
}

impl TypeCache {
    fn new(storage: Arc<MVCCStorage>, open_sequence_number: SequenceNumber) -> TypeCache {

        // TODO: we could either lazily or eagerly preload the schema?
        //       will pre-load the schema for now

        // note: since we will parse out many heterogenous properties/edges from the schema, we will scan once into a vector,
        //       then go through it again to pull out the type information.

        let mut entity_types_data = BTreeSet::new();
        storage.iterate_prefix(build_entity_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>(&mut entity_types_data);
        let entities_count = entity_types_data.iter().filter(|(key, _)| {
            is_entity_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
        }).count();
        let entity_types = Self::read_entity_types(&entity_types_data, entities_count);
        let entity_types_supertypes = Self::read_entity_type_supertypes(&entity_types_data, &entity_types);

        let mut relation_types_data = BTreeSet::new();
        storage.iterate_prefix(build_relation_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>(&mut relation_types_data);
        let relations_count = relation_types_data.iter().filter(|(key, _)| {
            is_relation_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
        }).count();
        let relation_types = Self::read_relation_types(&relation_types_data, relations_count);

        let mut attribute_types_data = BTreeSet::new();
        storage.iterate_prefix(build_attribute_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE>(&mut attribute_types_data);
        let attributes_count = attribute_types_data.iter().filter(|(key, _)| {
            is_attribute_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))
        }).count();
        let attribute_types = Self::read_attribute_types(&attribute_types_data, attributes_count);

        TypeCache {
            storage: storage,
            open_sequence_number: open_sequence_number,

            entity_types: entity_types,
            relation_types: relation_types,
            attribute_types: attribute_types,
        }
    }

    fn read_entity_types(entity_types_data: &BTreeSet<(StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>)>,
                         entities_count: usize) -> Box<[Option<EntityType<'static>>]> {
        let mut entity_types: Box<[Option<EntityType<'static>>]> = (0..entities_count).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        entity_types_data.iter().for_each(|(key, _)|
            {
                if (is_entity_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))) {
                    let entity_type = EntityType::new(new_entity_type_vertex(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                    let type_index = Typed::type_id(entity_type.vertex()).as_u16();
                    entity_types[type_index as usize] = Some(entity_type);
                }
            }
        );
        entity_types
    }

    fn read_entity_type_supertypes(entity_types_data: &BTreeSet<(StorageKeyArray<{ BUFFER_KEY_INLINE }>, ByteArray<{ BUFFER_VALUE_INLINE }>)>,
                                   entity_types: &Box<[Option<EntityType<'static>>]>) -> Box<[Option<TypeVertex<'static>>]> {
        let mut super_types: Box<[Option<TypeVertex<'static>>]> = (0..entity_types.len()).map(|_| None).collect::<Vec<_>>().into_boxed_slice();

         entity_types_data.iter().for_each(|(key, _)|
            if is_sub_edge_forward(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array()))) {
                let edge = new_sub_edge_forward(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())));
                let subtype = edge.from();
                let supertype = edge.to();

                // let entity_type = EntityType::new(new_entity_type_vertex(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                // entity_types[Typed::type_id(entity_type.vertex()).as_u16() as usize] = Some(entity_type);
            }
        );
        // entity_types
        todo!()
    }


    fn read_relation_types(relation_types_data: &BTreeSet<(StorageKeyArray<64>, ByteArray<64>)>,
                           relations_count: usize) -> Box<[Option<RelationType<'static>>]> {
        let mut relation_types: Box<[Option<RelationType<'static>>]> = (0..relations_count).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        relation_types_data.iter().for_each(|(key, _)|
            {
                if (is_relation_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))) {
                    let relation_type = RelationType::new(new_relation_type_vertex(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                    let type_index = Typed::type_id(relation_type.vertex()).as_u16();
                    relation_types[type_index as usize] = Some(relation_type);
                }
            }
        );
        relation_types
    }

    fn read_attribute_types(attribute_types_data: &BTreeSet<(StorageKeyArray<64>, ByteArray<64>)>,
                            attributes_count: usize) -> Box<[Option<AttributeType<'static>>]> {
        let mut attribute_types: Box<[Option<AttributeType<'static>>]> = (0..attributes_count).map(|_| None).collect::<Vec<_>>().into_boxed_slice();
        attribute_types_data.iter().for_each(|(key, _)|
            {
                if (is_attribute_type_vertex(ByteArrayOrRef::Reference(ByteReference::from(key.byte_array())))) {
                    let attribute_type = AttributeType::new(new_attribute_type_vertex(ByteArrayOrRef::Array(ByteArray::copy(key.bytes()))));
                    let type_index = Typed::type_id(attribute_type.vertex()).as_u16();
                    attribute_types[type_index as usize] = Some(attribute_type);
                }
            }
        );
        attribute_types
    }

    pub(crate) fn get_entity_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &EntityType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexEntityType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        self.entity_types.get(as_u16 as usize).unwrap().as_ref().unwrap()
    }

    pub(crate) fn get_relation_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &RelationType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRelationType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        self.relation_types.get(as_u16 as usize).unwrap().as_ref().unwrap()
    }

    pub(crate) fn get_attribute_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &AttributeType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexAttributeType);
        let as_u16 = Typed::type_id(&type_vertex).as_u16();
        self.attribute_types.get(as_u16 as usize).unwrap().as_ref().unwrap()
    }
}
