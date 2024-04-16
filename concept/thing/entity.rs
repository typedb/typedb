/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
            vertex_object::ObjectVertex,
        },
        type_::vertex::build_vertex_entity_type,
        Typed,
    },
    layout::prefix::Prefix,
    AsBytes, Keyable, Prefixed,
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    concept_iterator,
    error::ConceptWriteError,
    thing::{
        attribute::{Attribute, AttributeIterator},
        object::{HasAttributeIterator, Object},
        relation::{IndexedPlayersIterator, RelationRoleIterator},
        thing_manager::ThingManager,
        ObjectAPI, ThingAPI,
    },
    type_::entity_type::EntityType,
    ByteReference, ConceptAPI, ConceptStatus, GetStatus,
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
        EntityType::new(build_vertex_entity_type(self.vertex.type_id_()))
    }

    pub fn as_reference<'this>(&'this self) -> Entity<'this> {
        Entity { vertex: self.vertex.as_reference() }
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_has<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> HasAttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.get_has_of(self.as_reference())
    }

    pub fn set_has(&self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>) {
        // TODO: validate schema
        // TODO: handle duplicates/counts
        thing_manager.set_has(self.as_reference(), attribute.as_reference())
    }

    pub fn delete_has(&self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: &Attribute<'_>) {
        // TODO: validate schema
        // TODO: handle duplicates/counts
        thing_manager.delete_has(self.as_reference(), attribute.as_reference())
    }

    pub fn get_relations<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> RelationRoleIterator<'m, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        thing_manager.get_relations_of(self.as_reference())
    }

    pub fn get_indexed_players<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> IndexedPlayersIterator<'m, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        thing_manager.get_indexed_players_of(Object::Entity(self.as_reference()))
    }

    pub(crate) fn into_owned(self) -> Entity<'static> {
        Entity { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Entity<'a> {}

impl<'a> ThingAPI<'a> for Entity<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<impl WritableSnapshot>) {
        if matches!(self.get_status(thing_manager), ConceptStatus::Persisted) {
            thing_manager.lock_existing(self.as_reference());
        }
    }

    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus {
        thing_manager.get_status(self.vertex().as_storage_key())
    }

    fn delete<'m>(self, thing_manager: &'m ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError> {
        let mut has_iter = self.get_has(thing_manager);
        let mut attr = has_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        while attr.is_some() {
            self.delete_has(thing_manager, &attr.unwrap());
            attr = has_iter.next().transpose()
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        }

        let mut relation_iter = self.get_relations(thing_manager);
        let mut playing = relation_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        while let Some((relation, role, count)) = playing {
            relation.delete_player_many(thing_manager, role, Object::Entity(self.as_reference()), count);
            playing = relation_iter.next().transpose()
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        }

        thing_manager.delete_entity(self);
        Ok(())
    }
}

impl<'a> ObjectAPI<'a> for Entity<'a> {
    fn vertex<'this>(&'this self) -> ObjectVertex<'this> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> ObjectVertex<'a> {
        self.vertex
    }
}

fn storage_key_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Entity<'_> {
    Entity::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(EntityIterator, Entity, storage_key_to_entity);
