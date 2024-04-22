/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use encoding::{
    AsBytes,
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
            vertex_object::ObjectVertex,
        },
        type_::vertex::build_vertex_entity_type,
        Typed,
    },
    Keyable, layout::prefix::Prefix, Prefixed,
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    ByteReference,
    concept_iterator,
    ConceptAPI,
    ConceptStatus,
    error::ConceptWriteError, GetStatus, thing::{
        attribute::Attribute,
        object::{HasAttributeIterator, Object},
        ObjectAPI,
        relation::{IndexedPlayersIterator, RelationRoleIterator},
        thing_manager::ThingManager, ThingAPI,
    }, type_::entity_type::EntityType,
};
use crate::error::ConceptReadError;
use crate::thing::value::Value;
use crate::type_::attribute_type::AttributeType;
use crate::type_::OwnerAPI;

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

    pub fn has_attribute(
        &self,
        thing_manager: &ThingManager<impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<bool, ConceptReadError> {
        thing_manager.has_attribute(self.as_reference(), attribute_type, value)
    }

    pub fn get_has<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> HasAttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.get_has(self.as_reference())
    }

    pub fn get_has_type<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> HasAttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }> {
        thing_manager.get_has_type(self.as_reference(), attribute_type)
    }

    pub fn set_has(&self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>) {
        // TODO: validate schema
        // TODO: handle duplicates/counts
        thing_manager.set_has(self.as_reference(), attribute.as_reference())
    }

    pub fn delete_has_single(
        &self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>,
    ) -> Result<(), ConceptWriteError> {
        self.delete_has_many(thing_manager, attribute, 1)
    }

    pub fn delete_has_many(
        &self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>, count: u64,
    ) -> Result<(), ConceptWriteError> {
        let owns = self.type_().get_owns_attribute(
            thing_manager.type_manager(),
            attribute.type_(),
        ).map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        match owns {
            None => {
                todo!("throw useful schema violation error")
            }
            Some(owns) => {
                if owns.is_distinct(thing_manager.type_manager())
                    .map_err(|err| ConceptWriteError::ConceptRead { source: err })? {
                    debug_assert_eq!(count, 1);
                    thing_manager.delete_has(self.as_reference(), attribute);
                } else {
                    thing_manager.decrement_has(self.as_reference(), attribute, count);
                }
            }
        }
        Ok(())
    }

    pub fn get_relations<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> RelationRoleIterator<'m, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        thing_manager.get_relations_roles(self.as_reference())
    }

    pub fn get_indexed_players<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> IndexedPlayersIterator<'m, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        thing_manager.get_indexed_players(Object::Entity(self.as_reference()))
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

    fn errors(&self, thing_manager: &ThingManager<impl WritableSnapshot>) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        todo!()
    }

    fn delete<'m>(self, thing_manager: &'m ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError> {
        let mut has_iter = self.get_has(thing_manager);
        let mut has = has_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        while let Some((attr, count)) = has {
            self.delete_has_many(thing_manager, attr, count)?;
            has = has_iter.next().transpose()
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        }

        let mut relation_iter = self.get_relations(thing_manager);
        let mut playing = relation_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        while let Some((relation, role, count)) = playing {
            relation.delete_player_many(thing_manager, role, Object::Entity(self.as_reference()), count)?;
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
