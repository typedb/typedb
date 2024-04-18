/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use bytes::Bytes;
use encoding::{AsBytes, graph::{
    thing::{
        edge::{ThingEdgeHas, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
        vertex_object::ObjectVertex},
    type_::vertex::{build_vertex_relation_type, build_vertex_role_type},
    Typed,
}, Keyable, layout::prefix::Prefix, Prefixed, value::decode_value_u64};
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{ByteReference, concept_iterator, ConceptAPI, ConceptStatus, edge_iterator, error::ConceptReadError, GetStatus, thing::{
    attribute::Attribute,
    object::Object,
    thing_manager::ThingManager,
}, type_::{
    annotation::AnnotationDistinct,
    relation_type::RelationType,
    role_type::{RoleType, RoleTypeAnnotation},
    TypeAPI,
}};
use crate::error::ConceptWriteError;
use crate::thing::{ObjectAPI, ThingAPI};
use crate::thing::object::HasAttributeIterator;
use crate::type_::OwnerAPI;
use crate::type_::owns::OwnsAnnotation;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Relation<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Relation<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexRelation);
        Relation { vertex }
    }

    pub(crate) fn as_reference(&self) -> Relation<'_> {
        Relation { vertex: self.vertex.as_reference() }
    }

    pub fn type_(&self) -> RelationType<'static> {
        RelationType::new(build_vertex_relation_type(self.vertex.type_id_()))
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
                let annotations = owns
                    .get_annotations(thing_manager.type_manager())
                    .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
                if annotations.contains(&OwnsAnnotation::Distinct(AnnotationDistinct::new())) {
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
        thing_manager.get_relations_of(self.as_reference())
    }

    pub fn get_indexed_players<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> IndexedPlayersIterator<'m, { ThingEdgeRelationIndex::LENGTH_PREFIX_FROM }> {
        thing_manager.get_indexed_players_of(Object::Relation(self.as_reference()))
    }

    pub fn has_players<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> bool {
        match self.get_status(thing_manager) {
            ConceptStatus::Inserted => thing_manager.has_role_players(self.as_reference(), true),
            ConceptStatus::Put | ConceptStatus::Persisted => thing_manager.has_role_players(self.as_reference(), false),
            ConceptStatus::Deleted => unreachable!("Cannot operate on a deleted concept."),
        }
    }

    pub fn get_players<'m>(
        &self,
        thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> RolePlayerIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.get_role_players_of(self.as_reference())
    }

    fn get_player_counts(
        &self,
        thing_manager: &ThingManager<impl ReadableSnapshot>,
    ) -> Result<HashMap<RoleType<'static>, u64>, ConceptReadError> {
        let mut map = HashMap::new();
        let mut rp_iter = self.get_players(thing_manager);
        let mut rp = rp_iter.next().transpose()?;
        while let Some((role_player, count)) = rp {
            let mut value = map.entry(role_player.role_type.clone()).or_insert(0);
            *value += count;
            rp = rp_iter.next().transpose()?;
        }
        Ok(map)
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
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        // TODO: validate schema
        let role_annotations = role_type.get_annotations(thing_manager.type_manager()).unwrap();
        let distinct = role_annotations.contains(&RoleTypeAnnotation::Distinct(AnnotationDistinct::new()));
        if distinct {
            self.add_player_distinct(thing_manager, role_type, player);
        } else {
            self.add_player_increment(thing_manager, role_type, player);
        }
    }

    fn add_player_distinct(
        &self,
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        if thing_manager.type_manager().relation_index_available(self.type_()) {
            let compound_update_guard = thing_manager.relation_compound_update_mutex().lock().unwrap();
            thing_manager.set_role_player(self.as_reference(), player.as_reference(), role_type.clone());
            thing_manager.relation_index_player_regenerate(
                self.as_reference(), player.as_reference(), role_type, 1,
                &compound_update_guard,
            );
        } else {
            thing_manager.set_role_player(self.as_reference(), player.as_reference(), role_type.clone());
        }
    }

    fn add_player_increment(
        &self,
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        let compound_update_guard = thing_manager.relation_compound_update_mutex().lock().unwrap();
        let player_count = thing_manager.increment_role_player(
            self.as_reference(),
            player.as_reference(),
            role_type.clone(),
            &compound_update_guard,
        );
        if thing_manager.type_manager().relation_index_available(self.type_()) {
            thing_manager.relation_index_player_regenerate(
                self.as_reference(), player.as_reference(), role_type, player_count,
                &compound_update_guard,
            );
        }
    }

    pub fn delete_player_single(
        &self,
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        self.delete_player_many(thing_manager, role_type, player, 1);
    }

    pub fn delete_player_many(
        &self,
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
        delete_count: u64,
    ) {
        let role_annotations = role_type.get_annotations(thing_manager.type_manager()).unwrap();
        let distinct = role_annotations.contains(&RoleTypeAnnotation::Distinct(AnnotationDistinct::new()));
        if distinct {
            debug_assert_eq!(delete_count, 1);
            self.delete_player_distinct(thing_manager, role_type, player);
        } else {
            self.delete_player_decrement(thing_manager, role_type, player, delete_count);
        }
    }

    fn delete_player_distinct(
        &self,
        thing_manager: &ThingManager<impl WritableSnapshot>,
        role_type: RoleType<'static>,
        player: Object<'_>,
    ) {
        if thing_manager.type_manager().relation_index_available(self.type_()) {
            let compound_update_guard = thing_manager.relation_compound_update_mutex().lock().unwrap();
            thing_manager.delete_role_player(self.as_reference(), player.as_reference(), role_type.clone());
            thing_manager.relation_index_player_deleted(
                self.as_reference(), player.as_reference(), role_type,
                &compound_update_guard,
            );
        } else {
            thing_manager.delete_role_player(self.as_reference(), player.as_reference(), role_type.clone());
        }
    }

    fn delete_player_decrement(&self,
                               thing_manager: &ThingManager<impl WritableSnapshot>,
                               role_type: RoleType<'static>,
                               player: Object<'_>,
                               decrement_count: u64,
    ) {
        let compound_update_guard = thing_manager.relation_compound_update_mutex().lock().unwrap();
        let remaining_player_count = thing_manager.decrement_role_player(
            self.as_reference(),
            player.as_reference(),
            role_type.clone(),
            decrement_count,
            &compound_update_guard,
        );
        if thing_manager.type_manager().relation_index_available(self.type_()) {
            debug_assert_eq!(remaining_player_count, 0);
            thing_manager.relation_index_player_regenerate(
                self.as_reference(), player.as_reference(), role_type, remaining_player_count,
                &compound_update_guard,
            );
        }
    }

    pub(crate) fn into_owned(self) -> Relation<'static> {
        Relation { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Relation<'a> {}

impl<'a> ThingAPI<'a> for Relation<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<impl WritableSnapshot>) {
        if matches!(self.get_status(thing_manager), ConceptStatus::Persisted) {
            thing_manager.lock_existing(self.as_reference());
        }
    }

    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus {
        thing_manager.get_status(self.vertex().as_storage_key())
    }

    fn errors(&self, thing_manager: &ThingManager<impl WritableSnapshot>) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        let mut errors = Vec::new();

        // validate cardinality
        let type_ = self.type_();
        let relation_relates = type_.get_relates(thing_manager.type_manager())?;
        let role_player_count = self.get_player_counts(thing_manager)?;
        for relates in relation_relates.iter() {
            let role_type = relates.role();
            let cardinality = role_type.get_cardinality(thing_manager.type_manager())?;
            let player_count = role_player_count.get(&role_type).map_or(0, |c| *c);
            if !cardinality.is_valid(player_count) {
                errors.push(ConceptWriteError::RelationRoleCardinality {
                    relation: self.clone().into_owned(),
                    role_type: role_type.clone(),
                    cardinality: cardinality.clone(),
                    actual_cardinality: player_count,
                });
            }
        }
        Ok(errors)
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
            relation.delete_player_many(thing_manager, role, Object::Relation(self.as_reference()), count);
            playing = relation_iter.next().transpose()
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        }

        let mut player_iter = self.get_players(thing_manager);
        let mut role_player = player_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source: ConceptReadError::from(err) })?;
        while let Some((rp, count)) = role_player {
            self.delete_player_many(thing_manager, rp.role_type(), rp.player(), count);
            role_player = player_iter.next().transpose()
                .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        }

        thing_manager.delete_relation(self);
        Ok(())
    }
}

impl<'a> ObjectAPI<'a> for Relation<'a> {
    fn vertex(&self) -> ObjectVertex<'_> {
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

#[derive(Debug, Eq, PartialEq)]
pub struct RolePlayer<'a> {
    player: Object<'a>,
    role_type: RoleType<'static>,
}

impl<'a> RolePlayer<'a> {
    pub(crate) fn player(&self) -> Object<'_> {
        self.player.as_reference()
    }

    pub(crate) fn role_type(&self) -> RoleType<'static> {
        self.role_type.clone()
    }
}

fn storage_key_to_role_player<'a>(
    storage_key_ref: StorageKeyReference<'a>,
    value: ByteReference<'a>,
) -> (RolePlayer<'a>, u64) {
    let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key_ref.byte_ref()));
    let role_type = build_vertex_role_type(edge.role_id());
    (
        RolePlayer { player: Object::new(edge.into_to()), role_type: RoleType::new(role_type) },
        decode_value_u64(value),
    )
}

edge_iterator!(
    RolePlayerIterator;
    (RolePlayer<'_>, u64);
    storage_key_to_role_player
);

fn storage_key_to_relation_role<'a>(
    storage_key_ref: StorageKeyReference<'a>,
    value: ByteReference<'a>,
) -> (Relation<'a>, RoleType<'static>, u64) {
    let edge = ThingEdgeRolePlayer::new(Bytes::Reference(storage_key_ref.byte_ref()));
    let role_type = build_vertex_role_type(edge.role_id());
    (Relation::new(edge.into_to()), RoleType::new(role_type), decode_value_u64(value))
}

edge_iterator!(
    RelationRoleIterator;
    (Relation<'_>, RoleType<'static>, u64);
    storage_key_to_relation_role
);

fn storage_key_to_indexed_players<'a>(
    storage_key_ref: StorageKeyReference<'a>,
    value: ByteReference<'a>,
) -> (RolePlayer<'a>, RolePlayer<'a>, Relation<'a>, u64) {
    let from_role_player = RolePlayer {
        player: Object::new(ThingEdgeRelationIndex::read_from(storage_key_ref.byte_ref())),
        role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_from_role_id(storage_key_ref.byte_ref()))),
    };
    let to_role_player = RolePlayer {
        player: Object::new(ThingEdgeRelationIndex::read_to(storage_key_ref.byte_ref())),
        role_type: RoleType::new(build_vertex_role_type(ThingEdgeRelationIndex::read_to_role_id(storage_key_ref.byte_ref()))),
    };
    (
        from_role_player,
        to_role_player,
        Relation::new(ThingEdgeRelationIndex::read_relation(storage_key_ref.byte_ref())),
        decode_value_u64(value)
    )
}

edge_iterator!(
    IndexedPlayersIterator;
    (RolePlayer<'_>, RolePlayer<'_>, Relation<'_>, u64);
    storage_key_to_indexed_players
);
