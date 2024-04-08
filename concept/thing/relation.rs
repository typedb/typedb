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
use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
            vertex_object::ObjectVertex},
        type_::vertex::{build_vertex_relation_type, build_vertex_role_type},
        Typed,
    },
    layout::prefix::Prefix,
    AsBytes, Prefixed,
    value::{decode_value_u64},
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::{
        iterator::SnapshotRangeIterator,
    },
};
use storage::snapshot::iterator::SnapshotIteratorError;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    ByteReference,
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::{Attribute, AttributeIterator},
        object::Object,
        thing_manager::ThingManager,
    },
    type_::{
        annotation::AnnotationDuplicate,
        relation_type::RelationType,
        role_type::{RoleType, RoleTypeAnnotation},
        TypeAPI,
    },
    ConceptAPI,
};
use crate::thing::entity::Entity;
use crate::thing::ObjectAPI;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Relation<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Relation<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexRelation);
        Relation { vertex }
    }

    pub fn type_(&self) -> RelationType<'static> {
        RelationType::new(build_vertex_relation_type(self.vertex.type_id_()))
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn get_has<'m>(
        &self,
        thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> AttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.storage_get_has(self.as_reference())
    }

    pub fn set_has(&self, thing_manager: &ThingManager<'_, impl WritableSnapshot>, attribute: Attribute<'_>) {
        // TODO: validate schema
        thing_manager.storage_set_has(self.as_reference(), attribute.as_reference())
    }

    pub fn delete_has(&self, thing_manager: &ThingManager<'_, impl ReadableSnapshot>, attribute: Attribute<'_>) {
        // TODO: validate schema
        todo!()
    }

    pub fn get_relations<'m>(
        &self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> RelationRoleIterator<'m, { ThingEdgeRolePlayer::LENGTH_PREFIX_FROM }> {
        thing_manager.storage_get_relations(self.as_reference())
    }

    pub fn get_indexed_players<'m>(
        &self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>,
    ) -> IndexedPlayersIterator<'m, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        thing_manager.get_indexed_players(Object::Relation(self.as_reference()))
    }

    pub fn get_players<'m>(&self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>)
                           -> RolePlayerIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.storage_get_role_players(self.as_reference())
    }

    ///
    /// Semantics:
    ///   When duplicates are not allowed, we use set semantics and put the edge idempotently, which cannot fail other txn's
    ///   When duplicates are allowed, we increment the count of the role player edge and fail other txn's doing the same
    ///
    /// TODO: to optimise the common case of creating a full relation, we could introduce a RelationBuilder, which can accumulate role players,
    ///   Then write all players + indexes in one go
    ///
    pub fn add_player(
        &self,
        thing_manager: &ThingManager<'_, impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        // TODO: validate schema

        let role_annotations = role_type.get_annotations(thing_manager.type_manager()).unwrap();
        let duplicates_allowed = role_annotations.contains(&RoleTypeAnnotation::Duplicate(AnnotationDuplicate::new()));
        let mut player_count = 0;
        if duplicates_allowed {
            player_count = thing_manager.storage_increment_role_player(
                self.as_reference(),
                player.as_reference(),
                role_type.clone()
            );
        } else {
            thing_manager.storage_set_role_player(self.as_reference(), player.as_reference(), role_type.clone());
            player_count = 1;
        }

        // TODO handle error
        if thing_manager.type_manager().relation_index_available(self.type_()) {
            // TODO: what about schema transactions?
            thing_manager.relation_index_new_player(
                self.as_reference(), player.as_reference(), role_type, duplicates_allowed, player_count,
            );
        }
    }

    pub(crate) fn into_owned(self) -> Relation<'static> {
        Relation { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Relation<'a> {}

impl<'a> ObjectAPI<'a> for Relation<'a> {

    fn as_reference(&self) -> Relation<'_> {
        Relation { vertex: self.vertex.as_reference() }
    }

    fn vertex<'this>(&'this self) -> ObjectVertex<'this> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> ObjectVertex<'a> {
        self.vertex
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Relation<'_> {
    Relation::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(RelationIterator, Relation, storage_key_ref_to_entity);

pub(crate) struct RolePlayer<'a, const S: usize> {
    player: Object<'a>,
    role_type: RoleType<'static>,
}

impl<'a, const S: usize> RolePlayer<'a, S> {
    pub(crate) fn player<'this>(&'this self) -> Object<'this> {
        self.player.as_reference()
    }

    pub(crate) fn role_type(&self) -> RoleType<'static> {
        self.role_type.clone()
    }
}

pub struct RolePlayerIterator<'a, const S: usize> {
    snapshot_iterator: Option<SnapshotRangeIterator<'a, S>>,
}

impl<'a, const S: usize> RolePlayerIterator<'a, S> {
    pub(crate) fn new(snapshot_iterator: SnapshotRangeIterator<'a, S>) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator) }
    }

    pub(crate) fn new_empty() -> Self {
        Self { snapshot_iterator: None }
    }

    pub fn peek(&mut self) -> Option<Result<(RolePlayer<'_, S>, u64), ConceptReadError>> {
        self.iter_peek().map(|result|
            result.map(|(storage_key, value)| {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key.byte_ref()));
                let role_type = build_vertex_role_type(edge.role_id());
                (
                    RolePlayer {
                        player: Object::new(edge.into_to()),
                        role_type: RoleType::new(role_type),
                    },
                    decode_value_u64(value)
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error.clone() }
            )
        )
    }

    // a lending iterator trait is infeasible with the current borrow checker
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(RolePlayer<'_, S>, u64), ConceptReadError>> {
        self.iter_next().map(|result| {
            result.map(|(storage_key, value)| {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key.byte_ref()));
                let role_type = build_vertex_role_type(edge.role_id());
                (
                    RolePlayer {
                        player: Object::new(edge.into_to()),
                        role_type: RoleType::new(role_type),
                    },
                    decode_value_u64(value)
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error.clone() }
            )
        })
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.peek()
        } else {
            None
        }
    }

    fn iter_next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.next()
        } else {
            None
        }
    }

    pub fn count(mut self) -> usize {
        let mut count = 0;
        let mut next = self.next();
        while next.is_some() {
            next = self.next();
            count += 1;
        }
        count
    }
}


pub struct RelationRoleIterator<'a, const S: usize> {
    snapshot_iterator: Option<SnapshotRangeIterator<'a, S>>,
}

impl<'a, const S: usize> RelationRoleIterator<'a, S> {
    pub(crate) fn new(snapshot_iterator: SnapshotRangeIterator<'a, S>) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator) }
    }

    pub(crate) fn new_empty() -> Self {
        Self { snapshot_iterator: None }
    }

    pub fn peek(&mut self) -> Option<Result<(Relation<'_>, RoleType<'static>, u64), ConceptReadError>> {
        self.iter_peek().map(|result|
            result.map(|(storage_key, value)| {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key.byte_ref()));
                let role_type = build_vertex_role_type(edge.role_id());
                (
                    Relation::new(edge.into_to()),
                    RoleType::new(role_type),
                    decode_value_u64(value)
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error }
            )
        )
    }

    // a lending iterator trait is infeasible with the current borrow checker
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(Relation<'_>, RoleType<'static>, u64), ConceptReadError>> {
        self.iter_next().map(|result| {
            result.map(|(storage_key, value)| {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key.byte_ref()));
                let role_type = build_vertex_role_type(edge.role_id());
                (
                    Relation::new(edge.into_to()),
                    RoleType::new(role_type),
                    decode_value_u64(value)
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error }
            )
        })
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.peek()
        } else {
            None
        }
    }

    fn iter_next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.next()
        } else {
            None
        }
    }

    pub fn count(mut self) -> usize {
        let mut count = 0;
        let mut next = self.next();
        while next.is_some() {
            next = self.next();
            count += 1;
        }
        count
    }
}


pub struct IndexedPlayersIterator<'a, const S: usize> {
    snapshot_iterator: Option<SnapshotRangeIterator<'a, S>>,
}

impl<'a, const S: usize> IndexedPlayersIterator<'a, S> {
    pub(crate) fn new(snapshot_iterator: SnapshotRangeIterator<'a, S>) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator) }
    }

    pub(crate) fn new_empty() -> Self {
        Self { snapshot_iterator: None }
    }

    pub fn peek(&mut self) -> Option<Result<(RolePlayer<'_, S>, RolePlayer<'_, S>, Relation<'_>), ConceptReadError>> {
        self.iter_peek().map(|result|
            result.map(|(storage_key, value)| {
                let from_role_player = RolePlayer {
                    player: Object::new(ThingEdgeRelationIndex::read_from(storage_key.byte_ref())),
                    role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_from_role_id(storage_key.byte_ref()))),
                };
                let to_role_player = RolePlayer {
                    player: Object::new(ThingEdgeRelationIndex::read_to(storage_key.byte_ref())),
                    role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_to_role_id(storage_key.byte_ref()))),
                };
                (
                    from_role_player,
                    to_role_player,
                    Relation::new(ThingEdgeRelationIndex::read_relation(storage_key.byte_ref())),
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error }
            )
        )
    }

    // a lending iterator trait is infeasible with the current borrow checker
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<(RolePlayer<'_, S>, RolePlayer<'_, S>, Relation<'_>), ConceptReadError>> {
        self.iter_next().map(|result| {
            result.map(|(storage_key, value)| {
                let from_role_player = RolePlayer {
                    player: Object::new(ThingEdgeRelationIndex::read_from(storage_key.byte_ref())),
                    role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_from_role_id(storage_key.byte_ref()))),
                };
                let to_role_player = RolePlayer {
                    player: Object::new(ThingEdgeRelationIndex::read_to(storage_key.byte_ref())),
                    role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_to_role_id(storage_key.byte_ref()))),
                };
                (
                    from_role_player,
                    to_role_player,
                    Relation::new(ThingEdgeRelationIndex::read_relation(storage_key.byte_ref())),
                )
            }).map_err(|snapshot_error|
                ConceptReadError::SnapshotIterate { source: snapshot_error }
            )
        })
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_peek(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.peek()
        } else {
            None
        }
    }

    fn iter_next(&mut self) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.next()
        } else {
            None
        }
    }

    pub fn count(mut self) -> usize {
        let mut count = 0;
        let mut next = self.next();
        while next.is_some() {
            next = self.next();
            count += 1;
        }
        count
    }
}
