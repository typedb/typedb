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
use lending_iterator::{higher_order::Hkt, LendingIterator};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    pub fn type_(&self) -> EntityType {
        EntityType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn get_indexed_players<'m>(
        &'m self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> IndexedPlayersIterator {
        thing_manager.get_indexed_players(snapshot, Object::Entity(self.as_reference()))
    }

    pub fn next_possible(&self) -> Entity<'static> {
        let mut bytes = self.vertex.clone().into_bytes().into_array();
        bytes.increment().unwrap();
        Entity::new(ObjectVertex::new(Bytes::Array(bytes)))
    }

    pub fn as_reference(&self) -> Entity<'_> {
        Entity { vertex: self.vertex.as_reference() }
    }

    pub fn into_owned(self) -> Entity<'static> {
        Entity { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Entity<'a> {}

impl<'a> ThingAPI<'a> for Entity<'a> {
    type Vertex<'b> = ObjectVertex<'b>;
    type TypeAPI<'b> = EntityType;
    type Owned = Entity<'static>;
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexEntity, Prefix::VertexEntity);

    fn new(vertex: ObjectVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexEntity);
        Entity { vertex }
    }

    fn vertex(&self) -> Self::Vertex<'_> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> Self::Vertex<'a> {
        self.vertex
    }

    fn into_owned(self) -> Self::Owned {
        Entity::new(self.vertex.into_owned())
    }

    fn iid(&self) -> Bytes<'_, BUFFER_KEY_INLINE> {
        self.vertex.clone().into_bytes()
    }

    fn set_required(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptReadError>> {
        if matches!(self.get_status(snapshot, thing_manager), ConceptStatus::Persisted) {
            thing_manager.lock_existing_object(snapshot, self.as_reference());
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
        for attr in self
            .get_has_unordered(snapshot, thing_manager)
            .map_static(|res| res.map(|(k, _)| k.into_owned()))
            .try_collect::<Vec<_>, _>()?
        {
            thing_manager.unset_has(snapshot, &self, attr);
        }

        for owns in self.type_().get_owns(snapshot, thing_manager.type_manager())?.iter() {
            let ordering = owns.get_ordering(snapshot, thing_manager.type_manager())?;
            if matches!(ordering, Ordering::Ordered) {
                thing_manager.unset_has_ordered(snapshot, &self, owns.attribute());
            }
        }

        for (relation, role) in self
            .get_relations_roles(snapshot, thing_manager)
            .map_static(|res| res.map(|(relation, role, _count)| (relation.into_owned(), role.into_owned())))
            .try_collect::<Vec<_>, _>()?
        {
            thing_manager.unset_links(snapshot, relation, self.as_reference(), role)?;
        }

        thing_manager.delete_entity(snapshot, self);
        Ok(())
    }

    fn prefix_for_type(_type: Self::TypeAPI<'_>) -> Prefix {
        Prefix::VertexEntity
    }
}

impl<'a> ObjectAPI<'a> for Entity<'a> {
    fn type_(&self) -> impl ObjectTypeAPI<'static> {
        self.type_()
    }

    fn into_owned_object(self) -> Object<'static> {
        Object::Entity(self.into_owned())
    }
}

impl HKInstance for Entity<'static> {}

impl Hkt for Entity<'static> {
    type HktSelf<'a> = Entity<'a>;
}

impl<'a> fmt::Display for Entity<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Entity:{}:{}]", self.type_().vertex().type_id_(), self.vertex.object_id())
    }
}
