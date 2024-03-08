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

use std::sync::Arc;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use durability::SequenceNumber;
use encoding::graph::type_::vertex::{build_attribute_type_vertex_prefix, build_entity_type_vertex_prefix, build_relation_type_vertex_prefix, new_attribute_type_vertex, new_entity_type_vertex, new_relation_type_vertex, TypeVertex};
use encoding::graph::Typed;
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use storage::MVCCStorage;
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::relation_type::RelationType;

// TODO: could we slab allocate the schema cache? It is going to be a very hot region of memory
pub struct TypeCache {
    storage: Arc<MVCCStorage>,
    open_sequence_number: SequenceNumber,

    entity_types: Box<[EntityType<'static>]>,
    relation_types: Box<[RelationType<'static>]>,
    attribute_types: Box<[AttributeType<'static>]>,
}

impl TypeCache {
    fn new(storage: Arc<MVCCStorage>, open_sequence_number: SequenceNumber) -> TypeCache {

        // TODO: we could either lazily or eagerly preload the schema?
        //       will pre-load the schema for now

        // note: we will not create specialised iterator types here, since this is the only place (so far) we will create types directly from MVCCIterator
        //       instead of from the SnapshotIterator. Instead, we'll pay the cost of two memcopies (collect into vec of (key, value), then into vec of Types)
        let entity_types = storage.iterate_prefix(build_entity_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<64, 64>().into_iter()
            .map(|(key, _)| {
                EntityType::new(new_entity_type_vertex(ByteArrayOrRef::Array(key.into_byte_array())))
            }).collect::<Vec<_>>().into_boxed_slice();

        let relation_types = storage.iterate_prefix(build_relation_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<64, 64>().into_iter()
            .map(|(key, _)| {
                RelationType::new(new_relation_type_vertex(ByteArrayOrRef::Array(key.into_byte_array())))
            }).collect::<Vec<_>>().into_boxed_slice();

        let attribute_types = storage.iterate_prefix(build_attribute_type_vertex_prefix(), &open_sequence_number)
            .collect_cloned::<64, 64>().into_iter()
            .map(|(key, _)| {
                AttributeType::new(new_attribute_type_vertex(ByteArrayOrRef::Array(key.into_byte_array())))
            }).collect::<Vec<_>>().into_boxed_slice();


        TypeCache {
            storage: storage,
            open_sequence_number: open_sequence_number,

            entity_types: entity_types,
            relation_types: relation_types,
            attribute_types: attribute_types,
        }
    }

    pub(crate) fn get_entity_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &EntityType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexEntityType);
        let as_u16 = type_vertex.type_id().as_u16();
        self.entity_types.get(as_u16 as usize).unwrap()
    }

    pub(crate) fn get_relation_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &RelationType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexRelationType);
        let as_u16 = type_vertex.type_id().as_u16();
        self.relation_types.get(as_u16 as usize).unwrap()
    }

    pub(crate) fn get_attribute_type<'this>(&'this self, type_vertex: TypeVertex<'_>) -> &AttributeType<'static> {
        debug_assert_eq!(type_vertex.prefix(), PrefixType::VertexAttributeType);
        let as_u16 = type_vertex.type_id().as_u16();
        self.attribute_types.get(as_u16 as usize).unwrap()
    }
}
