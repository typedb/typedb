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

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{edge::ThingEdgeHas, vertex_object::ObjectVertex},
        type_::vertex::build_vertex_entity_type,
        Typed,
    },
    layout::prefix::Prefix,
    AsBytes, Prefixed,
};
use encoding::graph::thing::edge::{ThingEdgeRelationIndex, ThingEdgeRolePlayer};
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    concept_iterator,
    error::ConceptWriteError,
    thing::{
        object::Object,
        attribute::{Attribute, AttributeIterator},
        thing_manager::ThingManager,
        relation::{IndexedPlayersIterator, RelationRoleIterator},
    },
    type_::entity_type::EntityType,
    ByteReference, ConceptAPI,
};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexEntity);
        Entity { vertex }
    }

    pub fn type_(&self) -> EntityType<'static> {
        EntityType::new(build_vertex_entity_type(self.vertex.type_id()))
    }

    pub fn as_reference<'this>(&'this self) -> Entity<'this> {
        Entity { vertex: self.vertex.as_reference() }
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_has<'m>(
        &self,
        thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> AttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.storage_get_has(self.vertex())
    }

    pub fn set_has(&self, thing_manager: &ThingManager<'_, impl WritableSnapshot>, attribute: &Attribute<'_>) {
        // TODO: validate schema
        thing_manager.storage_set_has(self.vertex(), attribute.vertex())
    }

    pub fn delete_has(&self, thing_manager: &ThingManager<'_, impl WritableSnapshot>, attribute: &Attribute<'_>) {
        // TODO: validate schema

        todo!()
    }

    pub fn get_relations<'m>(
        &self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> RelationRoleIterator<'m, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        thing_manager.storage_get_relations(self.vertex())
    }

    pub fn get_indexed_players<'m>(
        &self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> IndexedPlayersIterator<'m, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        thing_manager.get_indexed_players(Object::Entity(self.as_reference()))
    }

    pub(crate) fn vertex<'this: 'a>(&'this self) -> ObjectVertex<'this> {
        self.vertex.as_reference()
    }

    pub(crate) fn into_owned(self) -> Entity<'static> {
        Entity { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Entity<'a> {}

fn storage_key_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Entity<'_> {
    Entity::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(EntityIterator, Entity, storage_key_to_entity);
