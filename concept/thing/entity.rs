/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{vertex_object::ObjectVertex, ThingVertex},
        type_::vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    AsBytes, Keyable, Prefixed,
};
use itertools::Itertools;
use lending_iterator::higher_order::Hkt;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        object::{Object, ObjectAPI},
        relation::IndexedPlayersIterator,
        thing_manager::ThingManager,
        HKInstance, ThingAPI,
    },
    type_::{entity_type::EntityType, ObjectTypeAPI, Ordering, OwnerAPI},
    ConceptAPI, ConceptStatus,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Entity {
    vertex: ObjectVertex,
}

impl Entity {
    pub fn type_(&self) -> EntityType {
        EntityType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn get_indexed_players<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> IndexedPlayersIterator {
        thing_manager.get_indexed_players(snapshot, Object::Entity(self))
    }

    pub fn next_possible(&self) -> Entity {
        let mut bytes = self.vertex.to_bytes().into_array();
        bytes.increment().unwrap();
        Entity::new(ObjectVertex::decode(&bytes))
    }
}

impl ConceptAPI for Entity {}

impl ThingAPI for Entity {
    type Vertex = ObjectVertex;
    type TypeAPI = EntityType;
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexEntity, Prefix::VertexEntity);

    fn new(vertex: ObjectVertex) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexEntity);
        Entity { vertex }
    }

    fn vertex(&self) -> Self::Vertex {
        self.vertex
    }

    fn iid(&self) -> Bytes<'_, BUFFER_KEY_INLINE> {
        self.vertex.to_bytes()
    }

    fn set_required(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptReadError>> {
        if matches!(self.get_status(snapshot, thing_manager), ConceptStatus::Persisted) {
            thing_manager.lock_existing_object(snapshot, *self);
        }
        Ok(())
    }

    fn get_status(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().into_storage_key())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        for attr in self.get_has_unordered(snapshot, thing_manager).map_ok(|(has, _count)| has.attribute()) {
            thing_manager.unset_has(snapshot, self, &attr?);
        }

        for owns in self.type_().get_owns(snapshot, thing_manager.type_manager())?.iter() {
            let ordering = owns.get_ordering(snapshot, thing_manager.type_manager())?;
            if matches!(ordering, Ordering::Ordered) {
                thing_manager.unset_has_ordered(snapshot, self, owns.attribute());
            }
        }

        for relates in self.get_relations_roles(snapshot, thing_manager) {
            let (relation, role, _count) = relates?;
            thing_manager.unset_links(snapshot, relation, self, role)?;
        }

        thing_manager.delete_entity(snapshot, self);
        Ok(())
    }

    fn prefix_for_type(_type: Self::TypeAPI) -> Prefix {
        Prefix::VertexEntity
    }
}

impl ObjectAPI for Entity {
    fn type_(&self) -> impl ObjectTypeAPI {
        self.type_()
    }

    fn into_object(self) -> Object {
        Object::Entity(self)
    }
}

impl HKInstance for Entity {}

impl Hkt for Entity {
    type HktSelf<'a> = Entity;
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Entity:{}:{}]", self.type_().vertex().type_id_(), self.vertex.object_id())
    }
}
