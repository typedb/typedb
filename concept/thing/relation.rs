/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use bytes::Bytes;
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeIndexedRelation, ThingEdgeLinks},
            vertex_object::{ObjectID, ObjectVertex},
            ThingVertex,
        },
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    value::decode_value_u64,
    AsBytes, Keyable, Prefixed,
};
use itertools::Itertools;
use lending_iterator::higher_order::Hkt;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::StorageCounters,
};
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        object::{Object, ObjectAPI},
        thing_manager::{validation::operation_time_validation::OperationTimeValidation, ThingManager},
        HKInstance, ThingAPI,
    },
    type_::{relation_type::RelationType, role_type::RoleType, ObjectTypeAPI, Ordering, OwnerAPI},
    ConceptAPI, ConceptStatus,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Relation {
    vertex: ObjectVertex,
}

impl Relation {
    const fn new_const(vertex: ObjectVertex) -> Self {
        Relation { vertex }
    }

    pub fn type_(&self) -> RelationType {
        RelationType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn has_players(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> bool {
        match self.get_status(snapshot, thing_manager, storage_counters) {
            ConceptStatus::Inserted => thing_manager.has_links(snapshot, self, true),
            ConceptStatus::Persisted => thing_manager.has_links(snapshot, self, false),
            ConceptStatus::Put => unreachable!("Encountered a `put` relation"),
            ConceptStatus::Deleted => unreachable!("Cannot operate on a deleted concept."),
        }
    }

    pub fn has_role_player(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: impl ObjectAPI,
        role: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_role_player(snapshot, self, player, role, storage_counters)
    }

    pub fn get_players(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        thing_manager.get_role_players(snapshot, self, storage_counters)
    }

    // TODO: It is basically the same as `get_players_role_type`, but with counts. Do we need to return counts?
    // `has`-related Object's methods return counts. Please refactor when working on lists.
    pub fn get_players_by_role(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        thing_manager.get_role_players_role(snapshot, self, role_type, storage_counters)
    }

    pub fn get_players_ordered(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> Result<Vec<Object>, Box<ConceptReadError>> {
        thing_manager.get_role_players_ordered(snapshot, self, role_type, storage_counters)
    }

    pub fn get_players_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        storage_counters: StorageCounters,
    ) -> impl Iterator<Item = Result<Object, Box<ConceptReadError>>> {
        self.get_players(snapshot, thing_manager, storage_counters).filter_map::<Result<Object, _>, _>(move |res| {
            match res {
                Ok((roleplayer, _count)) => (roleplayer.role_type() == role_type).then_some(Ok(roleplayer.player)),
                Err(error) => Some(Err(error)),
            }
        })
    }

    pub fn get_player_counts(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<HashMap<RoleType, u64>, Box<ConceptReadError>> {
        let mut counts = HashMap::new();
        let mut rp_iter = self.get_players(snapshot, thing_manager, storage_counters);
        while let Some((role_player, count)) = rp_iter.next().transpose()? {
            let value = counts.entry(role_player.role_type()).or_insert(0);
            *value += count;
        }
        Ok(counts)
    }

    /// Semantics:
    ///   When duplicates are not allowed, we use set semantics and put the edge idempotently, which cannot fail other txn's
    ///   When duplicates are allowed, we increment the count of the role player edge and fail other txn's doing the same
    ///
    /// TODO: to optimise the common case of creating a full relation, we could introduce a RelationBuilder, which can accumulate role players,
    ///   Then write all players + indexes in one go
    pub fn add_player(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        player: Object,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_exists_to_add_player(
            snapshot,
            thing_manager,
            self,
            storage_counters.clone(),
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_role_player_exists_to_add_player(
            snapshot,
            thing_manager,
            self,
            player,
            storage_counters.clone(),
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relation_type_relates_role_type(
            snapshot,
            thing_manager,
            self.type_(),
            role_type,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_object_type_plays_role_type(
            snapshot,
            thing_manager,
            player.type_(),
            role_type,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relates_is_not_abstract(snapshot, thing_manager, self, role_type)
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_plays_is_not_abstract(snapshot, thing_manager, player, role_type)
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        let distinct = self.type_().is_related_role_type_distinct(snapshot, thing_manager.type_manager(), role_type)?;
        if distinct {
            thing_manager.put_links_unordered(snapshot, self, player, role_type, storage_counters)
        } else {
            thing_manager.increment_links_count(snapshot, self, player, role_type, storage_counters)
        }
    }

    pub fn set_players_ordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        new_players: Vec<Object>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        match role_type.get_ordering(snapshot, thing_manager.type_manager())? {
            Ordering::Unordered => return Err(Box::new(ConceptWriteError::SetPlayersOrderedRoleUnordered {})),
            Ordering::Ordered => (),
        }

        OperationTimeValidation::validate_relation_exists_to_add_player(
            snapshot,
            thing_manager,
            self,
            storage_counters.clone(),
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relation_type_relates_role_type(
            snapshot,
            thing_manager,
            self.type_(),
            role_type,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relates_is_not_abstract(snapshot, thing_manager, self, role_type)
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        let mut new_counts = HashMap::<_, u64>::new();
        for &player in &new_players {
            OperationTimeValidation::validate_object_type_plays_role_type(
                snapshot,
                thing_manager,
                player.type_(),
                role_type,
            )
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

            OperationTimeValidation::validate_plays_is_not_abstract(snapshot, thing_manager, player, role_type)
                .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

            OperationTimeValidation::validate_role_player_exists_to_add_player(
                snapshot,
                thing_manager,
                self,
                player,
                storage_counters.clone(),
            )
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

            *new_counts.entry(player).or_default() += 1;
        }

        OperationTimeValidation::validate_relates_distinct_constraint(
            snapshot,
            thing_manager,
            self,
            role_type,
            &new_counts,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        // 1. get owned list
        let old_players =
            thing_manager.get_role_players_ordered(snapshot, self, role_type, storage_counters.clone())?;

        let mut old_counts = HashMap::<_, u64>::new();
        for &player in &old_players {
            *old_counts.entry(player).or_default() += 1;
        }

        // 2. Delete existing but no-longer necessary has, and add new ones, with the correct counts (!)
        for &player in old_counts.keys() {
            if !new_counts.contains_key(&player) {
                thing_manager.unset_links(snapshot, self, player, role_type, storage_counters.clone())?;
            }
        }

        for (player, count) in new_counts {
            // Don't skip unchanged count to ensure that locks are placed correctly
            thing_manager.set_links_count(snapshot, self, player, role_type, count, storage_counters.clone())?;
        }

        // 3. Overwrite owned list
        thing_manager.set_links_ordered(snapshot, self, role_type, new_players)?;
        Ok(())
    }

    pub fn remove_player_single(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        player: Object,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        self.remove_player_many(snapshot, thing_manager, role_type, player, 1, storage_counters)
    }

    pub fn remove_player_many(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        player: Object,
        delete_count: u64,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_exists_to_remove_player(
            snapshot,
            thing_manager,
            self,
            storage_counters.clone(),
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relation_type_relates_role_type(
            snapshot,
            thing_manager,
            self.type_(),
            role_type,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_object_type_plays_role_type(
            snapshot,
            thing_manager,
            player.type_(),
            role_type,
        )
        .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        OperationTimeValidation::validate_relates_is_not_abstract(snapshot, thing_manager, self, role_type)
            .map_err(|error| Box::new(ConceptWriteError::DataValidation { typedb_source: error }))?;

        let distinct = self.type_().is_related_role_type_distinct(snapshot, thing_manager.type_manager(), role_type)?;
        if distinct {
            debug_assert_eq!(delete_count, 1);
            thing_manager.unset_links(snapshot, self, player, role_type, storage_counters)
        } else {
            thing_manager.decrement_links_count(snapshot, self, player, role_type, delete_count, storage_counters)
        }
    }

    pub fn next_possible(&self) -> Relation {
        let mut bytes = self.vertex.to_bytes().into_array();
        bytes.increment().unwrap();
        Relation::new(ObjectVertex::decode(&bytes))
    }
}

impl ConceptAPI for Relation {}

impl ThingAPI for Relation {
    type TypeAPI = RelationType;
    type Vertex = ObjectVertex;
    const MIN: Self = Self::new_const(Self::Vertex::MIN_RELATION);
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexRelation, Prefix::VertexRelation);

    fn new(vertex: Self::Vertex) -> Self {
        debug_assert_eq!(
            vertex.prefix(),
            Prefix::VertexRelation,
            "non-relation prefix when constructing from a vertex"
        );
        Self::new_const(vertex)
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        if matches!(self.get_status(snapshot, thing_manager, storage_counters), ConceptStatus::Persisted) {
            thing_manager.lock_existing_object(snapshot, *self);
        }
        Ok(())
    }

    fn get_status(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> ConceptStatus {
        thing_manager.get_status(snapshot, self.vertex().into_storage_key(), storage_counters)
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        for attr in self
            .get_has_unordered(snapshot, thing_manager, storage_counters.clone())?
            .map_ok(|(has, _value)| has.attribute())
        {
            thing_manager.unset_has(snapshot, self, &attr?, storage_counters.clone())?;
        }

        for owns in self.type_().get_owns(snapshot, thing_manager.type_manager())?.iter() {
            let ordering = owns.get_ordering(snapshot, thing_manager.type_manager())?;
            if matches!(ordering, Ordering::Ordered) {
                thing_manager.unset_has_ordered(snapshot, self, owns.attribute(), storage_counters.clone());
            }
        }

        for relation_role in self.get_relations_roles(snapshot, thing_manager, storage_counters.clone()) {
            let (relation, role, _count) =
                relation_role.map_err(|error| Box::new(ConceptWriteError::ConceptRead { typedb_source: error }))?;
            thing_manager.unset_links(snapshot, relation, self, role, storage_counters.clone())?;
        }

        let players = self
            .get_players(snapshot, thing_manager, storage_counters.clone())
            .map_ok(|(roleplayer, _count)| (roleplayer.role_type, roleplayer.player));
        for role_player in players {
            let (role, player) =
                role_player.map_err(|error| Box::new(ConceptWriteError::ConceptRead { typedb_source: error }))?;
            // TODO: Deleting one player at a time, each of which will delete parts of the relation index, isn't optimal
            //       Instead, we could delete the players, then delete the entire index at once, if there is one
            thing_manager.unset_links(snapshot, self, player, role, storage_counters.clone())?;

            debug_assert!(!player
                .get_indexed_relations(snapshot, thing_manager, self.type_(), storage_counters.clone())
                .is_ok_and(|mut iterator| iterator.any(|result| {
                    match result {
                        Ok(((start, _, _, _, start_role, _), _)) => start == player && start_role == role,
                        Err(_) => false,
                    }
                })));
        }

        thing_manager.delete_relation(snapshot, self, storage_counters);
        Ok(())
    }

    fn prefix_for_type(_type: Self::TypeAPI) -> Prefix {
        Prefix::VertexRelation
    }
}

impl ObjectAPI for Relation {
    fn type_(&self) -> impl ObjectTypeAPI {
        self.type_()
    }

    fn into_object(self) -> Object {
        Object::Relation(self)
    }
}

impl HKInstance for Relation {}

impl Hkt for Relation {
    type HktSelf<'a> = Relation;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RolePlayer {
    player: Object,
    role_type: RoleType,
}

impl RolePlayer {
    pub fn player(self) -> Object {
        self.player
    }

    pub fn role_type(self) -> RoleType {
        self.role_type
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub enum Links {
    Edge(ThingEdgeLinks),
    EdgeReverse(ThingEdgeLinks),
}

impl Links {
    pub fn role_type(&self) -> RoleType {
        match self {
            Links::Edge(edge) | Links::EdgeReverse(edge) => RoleType::build_from_type_id(edge.role_id()),
        }
    }

    pub fn relation(&self) -> Relation {
        match self {
            Links::Edge(edge) | Links::EdgeReverse(edge) => Relation::new(edge.relation()),
        }
    }

    pub fn player(&self) -> Object {
        match self {
            Links::Edge(edge) | Links::EdgeReverse(edge) => Object::new(edge.player()),
        }
    }

    // TODO: legacy - ideally we'd delete these
    pub(crate) fn into_role_player(self) -> RolePlayer {
        RolePlayer { player: self.player(), role_type: self.role_type() }
    }

    pub(crate) fn into_relation_role(self) -> (Relation, RoleType) {
        (self.relation(), self.role_type())
    }
}

fn storage_key_edge_to_links<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Links, u64) {
    let edge = ThingEdgeLinks::decode(storage_key.into_bytes());
    debug_assert!(!edge.is_reverse());
    let links = Links::Edge(edge);
    (links, decode_value_u64(&value))
}

fn links_to_edge_storage_key(links_count: &(Links, u64)) -> StorageKey<'static, BUFFER_KEY_INLINE> {
    let (links, _count) = links_count;
    let edge = ThingEdgeLinks::new(links.relation().vertex(), links.player().vertex(), links.role_type().vertex());
    edge.into_storage_key()
}

edge_iterator!(
    LinksIterator;
    (Links, u64);
    storage_key_edge_to_links,
    links_to_edge_storage_key
);

fn storage_key_reverse_edge_to_links<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Links, u64) {
    let edge = ThingEdgeLinks::decode(storage_key.into_bytes());
    debug_assert!(edge.is_reverse());
    let links = Links::EdgeReverse(edge);
    (links, decode_value_u64(&value))
}

fn links_to_reverse_edge_storage_key(links_count: &(Links, u64)) -> StorageKey<'static, BUFFER_KEY_INLINE> {
    let (links, _count) = links_count;
    let edge =
        ThingEdgeLinks::new_reverse(links.player().vertex(), links.relation().vertex(), links.role_type().vertex());
    edge.into_storage_key()
}

edge_iterator!(
    LinksReverseIterator;
    (Links, u64);
    storage_key_reverse_edge_to_links,
    links_to_reverse_edge_storage_key
);

pub type IndexedRelationPlayers = (Object, Object, TypeID, ObjectID, RoleType, RoleType);

fn storage_key_to_indexed_players<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (IndexedRelationPlayers, u64) {
    let edge = ThingEdgeIndexedRelation::decode(Bytes::reference(storage_key.bytes()));
    let start_player = Object::new(edge.from());
    let end_player = Object::new(edge.to());
    let start_role_type = RoleType::build_from_type_id(edge.from_role_id());
    let end_role_type = RoleType::build_from_type_id(edge.to_role_id());
    let decoded =
        (start_player, end_player, edge.relation_type_id(), edge.relation_id(), start_role_type, end_role_type);
    (decoded, decode_value_u64(&value))
}

fn indexed_players_to_edge_storage_key(
    indexed_relation_players_count: &(IndexedRelationPlayers, u64),
) -> StorageKey<'static, BUFFER_KEY_INLINE> {
    let ((from, to, relation_type_id, relation_id, from_role, to_role), _count) = indexed_relation_players_count;
    let edge = ThingEdgeIndexedRelation::new_from_relation_parts(
        from.vertex(),
        to.vertex(),
        *relation_type_id,
        *relation_id,
        from_role.vertex().type_id_(),
        to_role.vertex().type_id_(),
    );
    edge.into_storage_key()
}

edge_iterator!(
    IndexedRelationsIterator;
    (IndexedRelationPlayers, u64);
    storage_key_to_indexed_players,
    indexed_players_to_edge_storage_key
);

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Relation:{}:{}]", self.type_().vertex().type_id_(), self.vertex.object_id())
    }
}
