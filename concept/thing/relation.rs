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
            edge::{ThingEdgeLinks, ThingEdgeRolePlayerIndex},
            vertex_object::ObjectVertex,
            ThingVertex,
        },
        type_::vertex::{PrefixedTypeVertexEncoding, TypeVertexEncoding},
        Typed,
    },
    layout::prefix::Prefix,
    value::decode_value_u64,
    AsBytes, Keyable, Prefixed,
};
use itertools::Itertools;
use lending_iterator::{higher_order::Hkt, LendingIterator};
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
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
    pub fn type_(&self) -> RelationType {
        RelationType::build_from_type_id(self.vertex.type_id_())
    }

    pub fn get_indexed_players<'m>(
        self,
        snapshot: &'m impl ReadableSnapshot,
        thing_manager: &'m ThingManager,
    ) -> IndexedPlayersIterator {
        thing_manager.get_indexed_players(snapshot, Object::Relation(self))
    }

    pub fn has_players(self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager) -> bool {
        match self.get_status(snapshot, thing_manager) {
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
    ) -> Result<bool, Box<ConceptReadError>> {
        thing_manager.has_role_player(snapshot, self, player, role)
    }

    pub fn get_players(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        thing_manager.get_role_players(snapshot, self)
    }

    pub fn get_players_by_role(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> impl Iterator<Item = Result<(RolePlayer, u64), Box<ConceptReadError>>> {
        thing_manager.get_role_players_role(snapshot, self, role_type)
    }

    pub fn get_players_ordered(
        self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<Vec<Object>, Box<ConceptReadError>> {
        thing_manager.get_role_players_ordered(snapshot, self, role_type)
    }

    pub fn get_players_role_type(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> impl Iterator<Item = Result<Object, Box<ConceptReadError>>> {
        self.get_players(snapshot, thing_manager).filter_map::<Result<Object, _>, _>(move |res| match res {
            Ok((roleplayer, _count)) => (roleplayer.role_type() == role_type).then_some(Ok(roleplayer.player)),
            Err(error) => Some(Err(error)),
        })
    }

    pub fn get_player_counts(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
    ) -> Result<HashMap<RoleType, u64>, Box<ConceptReadError>> {
        let mut counts = HashMap::new();
        let mut rp_iter = self.get_players(snapshot, thing_manager);
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
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_exists_to_add_player(snapshot, thing_manager, self)
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
            thing_manager.put_links_unordered(snapshot, self, player, role_type)
        } else {
            thing_manager.increment_links_count(snapshot, self, player, role_type)
        }
    }

    pub fn set_players_ordered(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        new_players: Vec<Object>,
    ) -> Result<(), Box<ConceptWriteError>> {
        match role_type.get_ordering(snapshot, thing_manager.type_manager())? {
            Ordering::Unordered => return Err(Box::new(ConceptWriteError::SetPlayersOrderedRoleUnordered {})),
            Ordering::Ordered => (),
        }

        OperationTimeValidation::validate_relation_exists_to_add_player(snapshot, thing_manager, self)
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
        let old_players = thing_manager.get_role_players_ordered(snapshot, self, role_type)?;

        let mut old_counts = HashMap::<_, u64>::new();
        for &player in &old_players {
            *old_counts.entry(player).or_default() += 1;
        }

        // 2. Delete existing but no-longer necessary has, and add new ones, with the correct counts (!)
        for &player in old_counts.keys() {
            if !new_counts.contains_key(&player) {
                thing_manager.unset_links(snapshot, self, player, role_type)?;
            }
        }

        for (player, count) in new_counts {
            // Don't skip unchanged count to ensure that locks are placed correctly
            thing_manager.set_links_count(snapshot, self, player, role_type, count)?;
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
    ) -> Result<(), Box<ConceptWriteError>> {
        self.remove_player_many(snapshot, thing_manager, role_type, player, 1)
    }

    pub fn remove_player_many(
        self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        role_type: RoleType,
        player: Object,
        delete_count: u64,
    ) -> Result<(), Box<ConceptWriteError>> {
        OperationTimeValidation::validate_relation_exists_to_remove_player(snapshot, thing_manager, self)
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
            thing_manager.unset_links(snapshot, self, player, role_type)
        } else {
            thing_manager.decrement_links_count(snapshot, self, player, role_type, delete_count)
        }
    }

    pub fn next_possible(&self) -> Relation {
        let mut bytes = self.vertex.to_bytes().into_array();
        bytes.increment().unwrap();
        Relation::new(ObjectVertex::new(&bytes))
    }
}

impl ConceptAPI for Relation {}

impl ThingAPI for Relation {
    type Vertex = ObjectVertex;
    type TypeAPI = RelationType;
    const PREFIX_RANGE_INCLUSIVE: (Prefix, Prefix) = (Prefix::VertexRelation, Prefix::VertexRelation);

    fn new(vertex: Self::Vertex) -> Self {
        debug_assert_eq!(
            vertex.prefix(),
            Prefix::VertexRelation,
            "non-relation prefix when constructing from a vertex"
        );
        Relation { vertex }
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
        for attr in self.get_has_unordered(snapshot, thing_manager).map_ok(|(key, _value)| key) {
            thing_manager.unset_has(snapshot, self, &attr?);
        }

        for owns in self.type_().get_owns(snapshot, thing_manager.type_manager())?.iter() {
            let ordering = owns.get_ordering(snapshot, thing_manager.type_manager())?;
            if matches!(ordering, Ordering::Ordered) {
                thing_manager.unset_has_ordered(snapshot, self, owns.attribute());
            }
        }

        for relation_role in self.get_relations_roles(snapshot, thing_manager) {
            let (relation, role, _count) =
                relation_role.map_err(|error| Box::new(ConceptWriteError::ConceptRead { source: error }))?;
            thing_manager.unset_links(snapshot, relation, self, role)?;
        }

        let players = self
            .get_players(snapshot, thing_manager)
            .map_ok(|(roleplayer, _count)| (roleplayer.role_type, roleplayer.player));
        for role_player in players {
            let (role, player) =
                role_player.map_err(|error| Box::new(ConceptWriteError::ConceptRead { source: error }))?;
            // TODO: Deleting one player at a time, each of which will delete parts of the relation index, isn't optimal
            //       Instead, we could delete the players, then delete the entire index at once, if there is one
            thing_manager.unset_links(snapshot, self, player, role)?;
        }

        debug_assert_eq!(self.get_indexed_players(snapshot, thing_manager).count(), 0);

        if self.get_status(snapshot, thing_manager) == ConceptStatus::Inserted {
            thing_manager.uninsert_relation(snapshot, self);
        } else {
            thing_manager.delete_relation(snapshot, self);
        }
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

fn storage_key_links_edge_to_role_player<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (RolePlayer, u64) {
    let edge = ThingEdgeLinks::new(storage_key.into_bytes());
    let role_type = RoleType::build_from_type_id(edge.role_id());
    let player = Object::new(edge.player());
    (RolePlayer { player, role_type }, decode_value_u64(&value))
}

impl Hkt for RolePlayer {
    type HktSelf<'a> = RolePlayer;
}

edge_iterator!(
    RolePlayerIterator;
    (RolePlayer, u64);
    storage_key_links_edge_to_role_player
);

fn storage_key_links_edge_to_relation_role<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Relation, RoleType, u64) {
    let edge = ThingEdgeLinks::new(storage_key.into_bytes());
    let role_type = RoleType::build_from_type_id(edge.role_id());
    (Relation::new(edge.relation()), role_type, decode_value_u64(&value))
}

edge_iterator!(
    RelationRoleIterator;
    (Relation, RoleType, u64);
    storage_key_links_edge_to_relation_role
);

fn storage_key_links_edge_to_relation_role_player<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (Relation, RolePlayer, u64) {
    let edge = ThingEdgeLinks::new(storage_key.into_bytes());
    let relation = Relation::new(edge.relation());
    let role_type = RoleType::build_from_type_id(edge.role_id());
    let player = Object::new(edge.player());
    let role_player = RolePlayer { player, role_type };
    (relation, role_player, decode_value_u64(&value))
}

edge_iterator!(
    LinksIterator;
    (Relation, RolePlayer, u64);
    storage_key_links_edge_to_relation_role_player
);

fn storage_key_to_indexed_players<'a>(
    storage_key: StorageKey<'a, BUFFER_KEY_INLINE>,
    value: Bytes<'a, BUFFER_VALUE_INLINE>,
) -> (RolePlayer, RolePlayer, Relation, u64) {
    let edge = ThingEdgeRolePlayerIndex::decode(Bytes::reference(storage_key.bytes()));
    let from_role_player =
        RolePlayer { player: Object::new(edge.from()), role_type: RoleType::build_from_type_id(edge.from_role_id()) };
    let to_role_player =
        RolePlayer { player: Object::new(edge.to()), role_type: RoleType::build_from_type_id(edge.to_role_id()) };
    (from_role_player, to_role_player, Relation::new(edge.relation()), decode_value_u64(&value))
}

edge_iterator!(
    IndexedPlayersIterator;
    (RolePlayer, RolePlayer, Relation, u64);
    storage_key_to_indexed_players
);

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Relation:{}:{}]", self.type_().vertex().type_id_(), self.vertex.object_id())
    }
}
