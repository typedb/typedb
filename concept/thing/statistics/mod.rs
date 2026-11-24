/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::min,
    collections::{HashMap, hash_map},
    fmt,
    hash::Hash,
    iter::Sum,
    ops::Add,
    sync::Arc,
    time::Instant,
};

use durability::DurabilityRecordType;
use encoding::{DecodableKey, graph::type_::vertex::PrefixedTypeVertexEncoding};
use error::typedb_error;
use lending_iterator::LendingIterator;
use resource::{
    constants::database::{STATISTICS_DURABLE_WRITE_CHANGE_COUNT, STATISTICS_DURABLE_WRITE_SEQ_NUMBERS},
    profile::StorageCounters,
};
use storage::{
    MVCCStorage, MVCCVisibility,
    durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord, UnsequencedDurabilityRecord},
    iterator::MVCCReadError,
    key_value::StorageKeyReference,
    keyspace::IteratorPool,
    record::CommitType,
    recovery::commit_recovery::StorageRecoveryError,
    sequence_number::SequenceNumber,
    snapshot::{
        ReadableSnapshot,
        buffer::OperationsBuffer,
        write::{PutAction, Write},
    },
};
use tracing::{Level, event};

use crate::{
    thing::{
        ThingAPI,
        attribute::Attribute,
        entity::Entity,
        object::Object,
        relation::Relation,
        statistics::{
            deltas::{CommitDeltas, Delta},
            sync::{SyncRecord, load_sync_records},
        },
    },
    type_::{
        TypeAPI, attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
        relation_type::RelationType, role_type::RoleType,
    },
};

pub mod deltas;
mod serialise;
mod sync;

#[derive(Debug, Clone, Copy)]
#[repr(u64)]
pub enum StatisticsEncodingVersion {
    V0 = 0,
    V1 = 1,
}

impl From<StatisticsEncodingVersion> for u64 {
    fn from(value: StatisticsEncodingVersion) -> u64 {
        value as u64
    }
}

pub struct UnknownStatisticsEncodingVersion(pub u64);

impl fmt::Display for UnknownStatisticsEncodingVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown statistics encoding version: {}", self.0)
    }
}

impl fmt::Debug for UnknownStatisticsEncodingVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl TryFrom<u64> for StatisticsEncodingVersion {
    type Error = UnknownStatisticsEncodingVersion;

    fn try_from(u64: u64) -> Result<Self, Self::Error> {
        match u64 {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            other => Err(UnknownStatisticsEncodingVersion(other)),
        }
    }
}

type DoubleHashMap<K1, K2, V> = HashMap<K1, HashMap<K2, V>>;
type TripleHashMap<K1, K2, K3, V> = DoubleHashMap<K1, K2, HashMap<K3, V>>;

trait DoubleHashMapExt<K1, K2, V> {
    fn double_entry(&mut self, k1: K1, k2: K2) -> hash_map::Entry<'_, K2, V>;
}

impl<K1: Eq + Hash, K2: Eq + Hash, V> DoubleHashMapExt<K1, K2, V> for DoubleHashMap<K1, K2, V> {
    fn double_entry(&mut self, k1: K1, k2: K2) -> hash_map::Entry<'_, K2, V> {
        self.entry(k1).or_default().entry(k2)
    }
}

trait TripleHashMapExt<K1, K2, K3, V> {
    fn triple_entry(&mut self, k1: K1, k2: K2, k3: K3) -> hash_map::Entry<'_, K3, V>;
}

impl<K1: Eq + Hash, K2: Eq + Hash, K3: Eq + Hash, V> TripleHashMapExt<K1, K2, K3, V> for TripleHashMap<K1, K2, K3, V> {
    fn triple_entry(&mut self, k1: K1, k2: K2, k3: K3) -> hash_map::Entry<'_, K3, V> {
        self.entry(k1).or_default().entry(k2).or_default().entry(k3)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub visible: u64,
    pub invisible: u64,
}

impl Counts {
    pub const fn new() -> Self {
        Self { visible: 0, invisible: 0 }
    }

    pub const fn from_visible(visible: u64) -> Self {
        Self { visible, invisible: 0 }
    }

    pub const fn is_zero(self) -> bool {
        self.visible == 0 && self.invisible == 0
    }

    pub const fn total(self) -> u64 {
        self.visible + self.invisible
    }

    pub fn saturating_add_assign(&mut self, delta: Delta, label: &str) {
        self.saturating_add_assign_visible(delta.net_change(), label);
        self.saturating_add_assign_invisible(delta.invisible_keys_created(), label);
    }

    fn saturating_add_assign_visible(&mut self, rhs: i64, label: &str) {
        self.visible = self.visible.checked_add_signed(rhs).unwrap_or_else(|| {
            diagnostics::error_with_report!(
                "Unexpected underflow in statistics {} count: {} + {}",
                label,
                self.visible,
                rhs
            );
            0
        });
    }

    fn saturating_add_assign_invisible(&mut self, rhs: u64, label: &str) {
        self.invisible = self.invisible.checked_add(rhs).unwrap_or_else(|| {
            diagnostics::error_with_report!(
                "Unexpected overflow in statistics invisible {} count: {} + {}",
                label,
                self.invisible,
                rhs
            );
            u64::MAX
        });
    }
}

impl Add for Counts {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self { visible: self.visible + rhs.visible, invisible: self.invisible + rhs.invisible }
    }
}

impl<'a> Sum<&'a Counts> for Counts {
    fn sum<I: Iterator<Item = &'a Counts>>(iter: I) -> Self {
        iter.copied().reduce(Counts::add).unwrap_or_default()
    }
}

/// Thing statistics, reflecting a snapshot of statistics accurate as of a particular sequence number
/// When types are undefined, we retain the last count of the instances of the type
/// Invariant: all undefined types are
#[derive(Clone)]
pub struct Statistics {
    encoding_version: StatisticsEncodingVersion,
    pub sequence_number: SequenceNumber,

    pub last_durable_write_sequence_number: SequenceNumber,
    pub last_durable_write_total_count: Counts,

    pub total_count: Counts,

    pub total_thing_count: Counts,
    pub total_entity_count: Counts,
    pub total_relation_count: Counts,
    pub total_attribute_count: Counts,
    pub total_role_count: Counts,
    pub total_has_count: Counts,

    pub entity_counts: HashMap<EntityType, Counts>,
    pub relation_counts: HashMap<RelationType, Counts>,
    pub attribute_counts: HashMap<AttributeType, Counts>,
    pub role_counts: HashMap<RoleType, Counts>,

    pub has_attribute_counts: DoubleHashMap<ObjectType, AttributeType, Counts>,
    pub attribute_owner_counts: DoubleHashMap<AttributeType, ObjectType, Counts>,
    pub role_player_counts: DoubleHashMap<ObjectType, RoleType, Counts>,
    pub relation_role_counts: DoubleHashMap<RelationType, RoleType, Counts>,
    pub relation_role_player_counts: TripleHashMap<RelationType, RoleType, ObjectType, Counts>,
    pub player_role_relation_counts: TripleHashMap<ObjectType, RoleType, RelationType, Counts>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub links_index_counts: DoubleHashMap<ObjectType, ObjectType, Counts>,
    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl Statistics {
    const ENCODING_VERSION: StatisticsEncodingVersion = StatisticsEncodingVersion::V1;

    pub fn new(sequence_number: SequenceNumber) -> Self {
        Statistics {
            encoding_version: Self::ENCODING_VERSION,
            sequence_number,
            last_durable_write_total_count: Counts::new(),
            last_durable_write_sequence_number: sequence_number,
            total_count: Counts::new(),
            total_thing_count: Counts::new(),
            total_entity_count: Counts::new(),
            total_relation_count: Counts::new(),
            total_attribute_count: Counts::new(),
            total_role_count: Counts::new(),
            total_has_count: Counts::new(),
            entity_counts: HashMap::new(),
            relation_counts: HashMap::new(),
            attribute_counts: HashMap::new(),
            role_counts: HashMap::new(),
            has_attribute_counts: HashMap::new(),
            attribute_owner_counts: HashMap::new(),
            role_player_counts: HashMap::new(),
            relation_role_counts: HashMap::new(),
            relation_role_player_counts: HashMap::new(),
            player_role_relation_counts: HashMap::new(),
            links_index_counts: HashMap::new(),
        }
    }

    pub fn read(
        snapshot: &impl ReadableSnapshot,
        storage_counters: StorageCounters,
    ) -> Result<Self, Box<MVCCReadError>> {
        let mut entity_counts = HashMap::<_, Counts>::new();
        let mut relation_counts = HashMap::<_, Counts>::new();
        let mut attribute_counts = HashMap::<_, Counts>::new();
        let mut has_attribute_counts = DoubleHashMap::<_, _, Counts>::new();
        let mut attribute_owner_counts = DoubleHashMap::<_, _, Counts>::new();
        let mut role_counts = HashMap::<_, Counts>::new();
        let mut role_player_counts = DoubleHashMap::<_, _, Counts>::new();
        let mut relation_role_counts = DoubleHashMap::<_, _, Counts>::new();
        let mut relation_role_player_counts = TripleHashMap::<_, _, _, Counts>::new();
        let mut player_role_relation_counts = TripleHashMap::<_, _, _, Counts>::new();
        let mut links_index_counts = DoubleHashMap::<_, _, Counts>::new();

        let mut storage_iterator = snapshot.iterate_storage(storage_counters);
        while let Some(item) = storage_iterator.next() {
            let mvcc_key_value = item?;
            let update: fn(&mut Counts, &str) = match mvcc_key_value.visibility() {
                MVCCVisibility::Future => continue, // ignore keys written by concurrent transactions while this update was running
                MVCCVisibility::Shadowed | MVCCVisibility::Deleted => {
                    |counts, label| counts.saturating_add_assign_invisible(1, label)
                }
                MVCCVisibility::Visible => |counts, label| counts.saturating_add_assign_visible(1, label),
            };
            match DecodableKey::try_decode(mvcc_key_value.key().bytes()) {
                Some(DecodableKey::EntityVertex(entity_vertex)) => {
                    let type_ = Entity::new(entity_vertex).type_();
                    update(entity_counts.entry(type_).or_default(), "entity");
                }
                Some(DecodableKey::RelationVertex(relation_vertex)) => {
                    let type_ = Relation::new(relation_vertex).type_();
                    update(relation_counts.entry(type_).or_default(), "relation");
                }
                Some(DecodableKey::AttributeVertex(attribute_vertex)) => {
                    let type_ = Attribute::new(attribute_vertex).type_();
                    update(attribute_counts.entry(type_).or_default(), "attribute");
                }
                Some(DecodableKey::ThingEdgeHas(has_edge)) => {
                    let from = Object::new(has_edge.from()).type_();
                    let to = Attribute::new(has_edge.to()).type_();
                    update(has_attribute_counts.double_entry(from, to).or_default(), "has_attribute");
                    update(attribute_owner_counts.double_entry(to, from).or_default(), "attribute_owner");
                }
                Some(DecodableKey::ThingEdgeHasReverse(_)) => (), // already covered by the forward edge
                Some(DecodableKey::ThingEdgeLinks(links_edge)) => {
                    if !links_edge.is_reverse() {
                        let relation = Relation::new(links_edge.from()).type_();
                        let role = RoleType::build_from_type_id(links_edge.role_id());
                        let player = Object::new(links_edge.to()).type_();
                        update(role_counts.entry(role).or_default(), "role");
                        update(relation_role_counts.double_entry(relation, role).or_default(), "role_player");
                        update(role_player_counts.double_entry(player, role).or_default(), "role_player");
                        update(
                            relation_role_player_counts.triple_entry(relation, role, player).or_default(),
                            "relation_role_player",
                        );
                        update(
                            player_role_relation_counts.triple_entry(player, role, relation).or_default(),
                            "player_role_relation",
                        );
                    }
                }
                Some(DecodableKey::ThingEdgeIndexedRelation(index_edge)) => {
                    let from = Object::new(index_edge.from()).type_();
                    let to = Object::new(index_edge.to()).type_();
                    update(links_index_counts.double_entry(from, to).or_default(), "links_index");
                }

                // TODO: remove entries for deleted types
                None
                | Some(DecodableKey::VertexEntityType(_))
                | Some(DecodableKey::VertexRelationType(_))
                | Some(DecodableKey::VertexAttributeType(_))
                | Some(DecodableKey::VertexRoleType(_))
                | Some(DecodableKey::DefinitionStruct(_))
                | Some(DecodableKey::DefinitionFunction(_))
                | Some(DecodableKey::TypeEdgeSub(_))
                | Some(DecodableKey::TypeEdgeSubReverse(_))
                | Some(DecodableKey::TypeEdgeOwns(_))
                | Some(DecodableKey::TypeEdgeOwnsReverse(_))
                | Some(DecodableKey::TypeEdgePlays(_))
                | Some(DecodableKey::TypeEdgePlaysReverse(_))
                | Some(DecodableKey::TypeEdgeRelates(_))
                | Some(DecodableKey::TypeEdgeRelatesReverse(_))
                | Some(DecodableKey::PropertyTypeVertex(_))
                | Some(DecodableKey::PropertyTypeEdge(_))
                | Some(DecodableKey::PropertyObjectVertex(_))
                | Some(DecodableKey::PropertyFunction(_))
                | Some(DecodableKey::IndexLabelToType(_))
                | Some(DecodableKey::IndexNameToDefinitionStruct(_))
                | Some(DecodableKey::IndexNameToDefinitionFunction(_))
                | Some(DecodableKey::IndexValueToStruct(_)) => (),
            }
        }

        let total_entity_count = entity_counts.values().sum();
        let total_relation_count = relation_counts.values().sum();
        let total_attribute_count = attribute_counts.values().sum();
        let total_thing_count = total_entity_count + total_relation_count + total_attribute_count;
        let total_role_count = role_counts.values().sum();
        let total_has_count = has_attribute_counts.values().flat_map(|x| x.values()).sum();

        // attribute counters and links index counts are not included in the total count
        let total_count = total_entity_count + total_relation_count + total_has_count + total_role_count;

        Ok(Self {
            encoding_version: Self::ENCODING_VERSION,
            sequence_number: snapshot.open_sequence_number(),
            last_durable_write_sequence_number: SequenceNumber::MIN,
            last_durable_write_total_count: Counts::new(),
            total_count,
            total_thing_count,
            total_entity_count,
            total_relation_count,
            total_attribute_count,
            total_role_count,
            total_has_count,
            entity_counts,
            relation_counts,
            attribute_counts,
            role_counts,
            has_attribute_counts,
            attribute_owner_counts,
            role_player_counts,
            relation_role_counts,
            relation_role_player_counts,
            player_role_relation_counts,
            links_index_counts,
        })
    }

    pub fn synchronise_from_wal_and_persist(
        &mut self,
        storage: &MVCCStorage<impl DurabilityClient>,
    ) -> Result<(), StatisticsError> {
        self.may_synchronise_from_wal(storage)?;
        if self.sequence_number > self.last_durable_write_sequence_number {
            self.durably_write(storage.durability())
                .map_err(|typedb_source| StatisticsError::DurablyWrite { typedb_source })?;
        }
        Ok(())
    }

    pub fn may_synchronise_from_wal(
        &mut self,
        storage: &MVCCStorage<impl DurabilityClient>,
    ) -> Result<(), StatisticsError> {
        use StatisticsError::DurablyWrite;

        let storage_watermark = storage.snapshot_watermark();
        debug_assert!(self.sequence_number <= storage_watermark);
        if self.sequence_number == storage_watermark {
            return Ok(());
        }

        let start = Instant::now();

        let wal_sync_records = load_sync_records(self.sequence_number.next(), storage.durability())?;

        for (seq, sync_record) in wal_sync_records {
            debug_assert_eq!(seq, self.sequence_number.next());
            match sync_record {
                SyncRecord::Rejected => {
                    self.sequence_number = seq;
                }
                SyncRecord::Deltas(deltas) => {
                    self.update_deltas(&deltas, storage.durability())
                        .map_err(|typedb_source| DurablyWrite { typedb_source })?;
                }
                SyncRecord::Commit(record) => {
                    let commit_type = record.commit_type();
                    match commit_type {
                        CommitType::Data => (),
                        CommitType::Schema => {
                            if self.sequence_number < seq {
                                // If last write was at Seq[11] and this schema commit is at Seq[12],
                                // no changes need to be applied or persisted.
                                if self.last_durable_write_sequence_number.next() < seq {
                                    self.durably_write(storage.durability())
                                        .map_err(|err| DurablyWrite { typedb_source: err })?;
                                }
                            }
                        }
                    };
                    let delta = self
                        .update_write(
                            seq,
                            &CommittedWrites {
                                open_sequence_number: record.open_sequence_number(),
                                operations: record.into_operations(),
                            },
                            storage,
                        )
                        .map_err(|source| StatisticsError::DataRead { source })?;
                    self.total_count.saturating_add_assign(delta, "total_count");
                    self.sequence_number = seq;
                }
            }
        }

        self.may_durably_write(storage.durability()).map_err(|err| DurablyWrite { typedb_source: err })?;

        let millis = Instant::now().duration_since(start).as_millis();
        event!(
            Level::TRACE,
            "Statistics sync finished in {} ms. Storage watermark was initially: {}. Current statistics sequence is from: {}",
            millis,
            storage_watermark,
            self.sequence_number
        );
        Ok(())
    }

    fn may_durably_write(&mut self, durability: &impl DurabilityClient) -> Result<(), DurabilityClientError> {
        let count_change_since_last_durable_write =
            u64::abs_diff(self.total_count.visible, self.last_durable_write_total_count.visible);
        let sequence_numbers_since_last_durable_write = self.sequence_number - self.last_durable_write_sequence_number;

        if count_change_since_last_durable_write > STATISTICS_DURABLE_WRITE_CHANGE_COUNT
            || sequence_numbers_since_last_durable_write > STATISTICS_DURABLE_WRITE_SEQ_NUMBERS
        {
            self.durably_write(durability)?;
        }

        Ok(())
    }

    pub fn durably_write(&mut self, durability: &impl DurabilityClient) -> Result<(), DurabilityClientError> {
        durability.unsequenced_write(self)?;
        self.last_durable_write_sequence_number = self.sequence_number;
        self.last_durable_write_total_count = self.total_count;
        Ok(())
    }

    pub fn update_deltas(
        &mut self,
        commit_deltas: &CommitDeltas,
        durability: &impl DurabilityClient,
    ) -> Result<(), DurabilityClientError> {
        let CommitDeltas {
            encoding_version: _,
            commit_type,
            commit_sequence_number,
            undefined_entities,
            undefined_relations,
            undefined_attributes,
            undefined_roles,
            entity_deltas,
            relation_deltas,
            attribute_deltas,
            has_attribute_deltas,
            relation_role_player_deltas,
            links_index_deltas,
        } = commit_deltas;

        if *commit_sequence_number <= self.sequence_number {
            return Ok(());
        }

        if *commit_type == CommitType::Schema
            && self.last_durable_write_sequence_number.next() < *commit_sequence_number
        {
            self.durably_write(durability)?;
        }

        let mut total_delta = Delta::default();

        for (&entity_type, &delta) in entity_deltas {
            self.update_entities(entity_type, delta);
            total_delta += delta;
        }
        for (&relation_type, &delta) in relation_deltas {
            self.update_relations(relation_type, delta);
            total_delta += delta;
        }
        for (&attribute_type, &delta) in attribute_deltas {
            self.update_attributes(attribute_type, delta);
        }

        for (&owner_type, attribute_deltas) in has_attribute_deltas {
            for (&attribute_type, &delta) in attribute_deltas {
                self.update_has(owner_type, attribute_type, delta);
                total_delta += delta;
            }
        }

        for (&relation_type, role_player_deltas) in relation_role_player_deltas {
            for (&role_type, player_deltas) in role_player_deltas {
                for (&player_type, &delta) in player_deltas {
                    self.update_role_player(player_type, role_type, relation_type, delta);
                    total_delta += delta;
                }
            }
        }

        for (&player_1_type, player_2_type_deltas) in links_index_deltas {
            for (&player_2_type, &delta) in player_2_type_deltas {
                self.update_indexed_player(player_1_type, player_2_type, delta);
            }
        }

        for &entity_type in undefined_entities {
            self.entity_counts.remove(&entity_type);
            self.clear_object_type(ObjectType::Entity(entity_type));
        }

        for &relation_type in undefined_relations {
            self.relation_counts.remove(&relation_type);
            self.relation_role_counts.remove(&relation_type);
            self.clear_object_type(ObjectType::Relation(relation_type));
        }

        for &attribute_type in undefined_attributes {
            self.attribute_counts.remove(&attribute_type);
            self.attribute_owner_counts.remove(&attribute_type);
            for map in self.has_attribute_counts.values_mut() {
                map.remove(&attribute_type);
            }
            self.has_attribute_counts.retain(|_, map| !map.is_empty());
        }

        for &role_type in undefined_roles {
            self.role_counts.remove(&role_type);
            for map in self.role_player_counts.values_mut() {
                map.remove(&role_type);
            }
            self.role_player_counts.retain(|_, map| !map.is_empty());
            for map in self.relation_role_counts.values_mut() {
                map.remove(&role_type);
            }
            self.relation_role_counts.retain(|_, map| !map.is_empty());
        }

        self.total_count.saturating_add_assign(total_delta, "total_count");

        self.sequence_number = *commit_sequence_number;

        self.may_durably_write(durability)?;

        Ok(())
    }

    fn update_write<D>(
        &mut self,
        commit_sequence_number: SequenceNumber,
        writes: &CommittedWrites,
        storage: &MVCCStorage<D>,
    ) -> Result<Delta, MVCCReadError> {
        type CleanupFn = Box<dyn FnOnce(&mut Statistics)>;

        if commit_sequence_number <= self.sequence_number {
            return Ok(Delta::default());
        }

        let mut total_delta = Delta::default();
        let mut deferred_type_cleanups: Vec<CleanupFn> = Vec::new();

        for (key, write) in writes.operations.iterate_writes() {
            let delta = write_to_delta(key, write, writes.open_sequence_number, commit_sequence_number, storage)?;
            match DecodableKey::try_decode(key.bytes()) {
                Some(DecodableKey::EntityVertex(entity_vertex)) => {
                    let type_ = Entity::new(entity_vertex).type_();
                    self.update_entities(type_, delta);
                    total_delta += delta;
                }
                Some(DecodableKey::RelationVertex(relation_vertex)) => {
                    let type_ = Relation::new(relation_vertex).type_();
                    self.update_relations(type_, delta);
                    total_delta += delta;
                }
                Some(DecodableKey::AttributeVertex(attribute_vertex)) => {
                    let type_ = Attribute::new(attribute_vertex).type_();
                    self.update_attributes(type_, delta);
                }

                Some(DecodableKey::ThingEdgeHas(has_edge)) => {
                    self.update_has(Object::new(has_edge.from()).type_(), Attribute::new(has_edge.to()).type_(), delta);
                    total_delta += delta;
                }
                Some(DecodableKey::ThingEdgeHasReverse(_)) => (),
                Some(DecodableKey::ThingEdgeLinks(links_edge)) => {
                    if !links_edge.is_reverse() {
                        let role_type = RoleType::build_from_type_id(links_edge.role_id());
                        self.update_role_player(
                            Object::new(links_edge.to()).type_(),
                            role_type,
                            Relation::new(links_edge.from()).type_(),
                            delta,
                        );
                        total_delta += delta;
                    }
                }
                Some(DecodableKey::ThingEdgeIndexedRelation(edge)) => {
                    self.update_indexed_player(Object::new(edge.from()).type_(), Object::new(edge.to()).type_(), delta);
                    // note: don't update total count based on index
                }

                Some(DecodableKey::VertexEntityType(entity_type_vertex)) => {
                    if write.is_delete() {
                        let type_ = EntityType::new(entity_type_vertex);
                        deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                            this.entity_counts.remove(&type_);
                            this.clear_object_type(ObjectType::Entity(type_));
                        }));
                    }
                    // note: don't update total count based on type updates
                }
                Some(DecodableKey::VertexRelationType(relation_type_vertex)) => {
                    if write.is_delete() {
                        let type_ = RelationType::new(relation_type_vertex);
                        deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                            this.relation_counts.remove(&type_);
                            this.relation_role_counts.remove(&type_);
                            this.clear_object_type(ObjectType::Relation(type_));
                        }));
                    }
                    // note: don't update total count based on type updates
                }
                Some(DecodableKey::VertexAttributeType(attribute_type_vertex)) => {
                    if write.is_delete() {
                        let type_ = AttributeType::new(attribute_type_vertex);
                        deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                            this.attribute_counts.remove(&type_);
                            this.attribute_owner_counts.remove(&type_);
                            for map in this.has_attribute_counts.values_mut() {
                                map.remove(&type_);
                            }
                            this.has_attribute_counts.retain(|_, map| !map.is_empty());
                        }));
                    }
                    // note: don't update total count based on type updates
                }
                Some(DecodableKey::VertexRoleType(role_type_vertex)) => {
                    if write.is_delete() {
                        let type_ = RoleType::new(role_type_vertex);
                        deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                            this.role_counts.remove(&type_);
                            for map in this.role_player_counts.values_mut() {
                                map.remove(&type_);
                            }
                            this.role_player_counts.retain(|_, map| !map.is_empty());
                            for map in this.relation_role_counts.values_mut() {
                                map.remove(&type_);
                            }
                            this.relation_role_counts.retain(|_, map| !map.is_empty());
                        }));
                    }
                    // note: don't update total count based on type updates
                }

                None
                | Some(DecodableKey::DefinitionStruct(_))
                | Some(DecodableKey::DefinitionFunction(_))
                | Some(DecodableKey::TypeEdgeSub(_))
                | Some(DecodableKey::TypeEdgeSubReverse(_))
                | Some(DecodableKey::TypeEdgeOwns(_))
                | Some(DecodableKey::TypeEdgeOwnsReverse(_))
                | Some(DecodableKey::TypeEdgePlays(_))
                | Some(DecodableKey::TypeEdgePlaysReverse(_))
                | Some(DecodableKey::TypeEdgeRelates(_))
                | Some(DecodableKey::TypeEdgeRelatesReverse(_))
                | Some(DecodableKey::PropertyTypeVertex(_))
                | Some(DecodableKey::PropertyTypeEdge(_))
                | Some(DecodableKey::PropertyObjectVertex(_))
                | Some(DecodableKey::PropertyFunction(_))
                | Some(DecodableKey::IndexLabelToType(_))
                | Some(DecodableKey::IndexNameToDefinitionStruct(_))
                | Some(DecodableKey::IndexNameToDefinitionFunction(_))
                | Some(DecodableKey::IndexValueToStruct(_)) => (),
            }
        }

        for cleanup in deferred_type_cleanups {
            cleanup(self);
        }

        Ok(total_delta)
    }

    fn clear_object_type(&mut self, object_type: ObjectType) {
        self.has_attribute_counts.remove(&object_type);
        for map in self.attribute_owner_counts.values_mut() {
            map.remove(&object_type);
        }
        self.attribute_owner_counts.retain(|_, map| !map.is_empty());

        self.role_player_counts.remove(&object_type);

        self.links_index_counts.remove(&object_type);
        for map in self.links_index_counts.values_mut() {
            map.remove(&object_type);
        }
        self.links_index_counts.retain(|_, map| !map.is_empty());
    }

    fn update_entities(&mut self, entity_type: EntityType, delta: Delta) {
        let count = self.entity_counts.entry(entity_type).or_default();
        count.saturating_add_assign(delta, "entity");
        self.total_entity_count.saturating_add_assign(delta, "total_entity");
        self.total_thing_count.saturating_add_assign(delta, "total_thing");
    }

    fn update_relations(&mut self, relation_type: RelationType, delta: Delta) {
        let count = self.relation_counts.entry(relation_type).or_default();
        count.saturating_add_assign(delta, "relation");
        self.total_relation_count.saturating_add_assign(delta, "total_relation");
        self.total_thing_count.saturating_add_assign(delta, "total_thing");
    }

    fn update_attributes(&mut self, attribute_type: AttributeType, delta: Delta) {
        let count = self.attribute_counts.entry(attribute_type).or_default();
        count.saturating_add_assign(delta, "attribute");
        self.total_attribute_count.saturating_add_assign(delta, "total_attribute");
        self.total_thing_count.saturating_add_assign(delta, "total_thing");
    }

    fn update_has(&mut self, owner_type: ObjectType, attribute_type: AttributeType, delta: Delta) {
        let attribute_count = self.has_attribute_counts.double_entry(owner_type, attribute_type).or_default();
        attribute_count.saturating_add_assign(delta, "has_attribute");
        let owner_count = self.attribute_owner_counts.double_entry(attribute_type, owner_type).or_default();
        owner_count.saturating_add_assign(delta, "attribute_owner");
        self.total_has_count.saturating_add_assign(delta, "total_has");
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType,
        role_type: RoleType,
        relation_type: RelationType,
        delta: Delta,
    ) {
        let role_count = self.role_counts.entry(role_type).or_default();
        role_count.saturating_add_assign(delta, "role");
        self.total_role_count.saturating_add_assign(delta, "total_role");
        let role_player_count = self.role_player_counts.double_entry(player_type, role_type).or_default();
        role_player_count.saturating_add_assign(delta, "role_player");
        let relation_role_count = self.relation_role_counts.double_entry(relation_type, role_type).or_default();
        relation_role_count.saturating_add_assign(delta, "relation_role");
        let relation_role_player_count =
            self.relation_role_player_counts.triple_entry(relation_type, role_type, player_type).or_default();
        relation_role_player_count.saturating_add_assign(delta, "relation_role_player");
        let player_role_relation_count =
            self.player_role_relation_counts.triple_entry(player_type, role_type, relation_type).or_default();
        player_role_relation_count.saturating_add_assign(delta, "player_role_relation");
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType, player_2_type: ObjectType, delta: Delta) {
        let player_1_to_2_index_count = self.links_index_counts.double_entry(player_1_type, player_2_type).or_default();
        player_1_to_2_index_count.saturating_add_assign(delta, "player_1_to_2_index");
    }

    /// Compute the largest fractional difference of any individual statistic
    pub fn largest_difference_frac(&self, other: &Statistics) -> f64 {
        let mut largest: f64 = 0.0;
        largest = largest.max(Self::largest_difference_frac_maps(&self.entity_counts, &other.entity_counts));
        largest = largest.max(Self::largest_difference_frac_maps(&self.relation_counts, &other.relation_counts));
        largest = largest.max(Self::largest_difference_frac_maps(&self.attribute_counts, &other.attribute_counts));
        largest = largest
            .max(Self::largest_difference_frac_map_maps(&self.has_attribute_counts, &other.has_attribute_counts));
        largest = largest
            .max(Self::largest_difference_frac_map_maps(&self.relation_role_counts, &other.relation_role_counts));
        largest =
            largest.max(Self::largest_difference_frac_map_maps(&self.role_player_counts, &other.role_player_counts));
        largest
    }

    // compute largest abs(value_1 - value_2) / min(value_1, value_2)
    fn largest_difference_frac_maps<T: Hash + Eq>(first: &HashMap<T, Counts>, second: &HashMap<T, Counts>) -> f64 {
        let mut largest = 0.0;
        for (key, first_value) in first {
            let second_value = second.get(key).copied().unwrap_or(Counts::new());

            let first_value = first_value.visible;
            let second_value = second_value.visible;

            if first_value == 0 && second_value == 0 {
                continue;
            } else if second_value == 0 || first_value == 0 {
                return f64::MAX;
            }
            let difference = (first_value as f64 - second_value as f64).abs();
            let frac = difference / (min(first_value, second_value) as f64);
            if frac > largest {
                largest = frac;
            }
        }

        for (key, second_value) in second {
            let first_value = first.get(key).copied().unwrap_or(Counts::new());

            let first_value = first_value.visible;
            let second_value = second_value.visible;

            if first_value == 0 && second_value == 0 {
                continue;
            } else if second_value == 0 || first_value == 0 {
                return f64::MAX;
            }
            // if both maps have a non-zero value, the first loop must have handled it
        }
        largest
    }

    fn largest_difference_frac_map_maps<T: Hash + Eq, U: Hash + Eq>(
        first: &DoubleHashMap<T, U, Counts>,
        second: &DoubleHashMap<T, U, Counts>,
    ) -> f64 {
        let mut largest = 0.0;
        let empty_map = HashMap::new();
        for (key, first_map) in first {
            let second_map = second.get(key).unwrap_or(&empty_map);
            let largest_map_diff = Self::largest_difference_frac_maps(first_map, second_map);
            if largest_map_diff > largest {
                largest = largest_map_diff;
            }
        }
        for (key, second_map) in second {
            match first.get(key) {
                None => {
                    let largest_map_diff = Self::largest_difference_frac_maps(second_map, &empty_map);
                    if largest_map_diff > largest {
                        largest = largest_map_diff;
                    }
                }
                Some(_) => {
                    continue;
                }
            };
            // if both maps have the value, the first loop would have handled it
        }
        largest
    }

    pub fn reset(&mut self, sequence_number: SequenceNumber) {
        self.sequence_number = sequence_number;
        self.total_count = Counts::new();
        self.total_thing_count = Counts::new();
        self.total_entity_count = Counts::new();
        self.total_relation_count = Counts::new();
        self.total_attribute_count = Counts::new();
        self.total_role_count = Counts::new();
        self.total_has_count = Counts::new();
        self.entity_counts.clear();
        self.relation_counts.clear();
        self.attribute_counts.clear();
        self.role_counts.clear();
        self.has_attribute_counts.clear();
        self.attribute_owner_counts.clear();
        self.role_player_counts.clear();
        self.relation_role_counts.clear();
        self.links_index_counts.clear();
    }
}

fn write_to_delta<D>(
    write_key: StorageKeyReference<'_>,
    write: &Write,
    open_sequence_number: SequenceNumber,
    commit_sequence_number: SequenceNumber,
    storage: &MVCCStorage<D>,
) -> Result<Delta, MVCCReadError> {
    match write {
        Write::Insert { .. } => Ok(Delta::new_insert()),
        Write::Delete { .. } => {
            if open_sequence_number.next() >= commit_sequence_number {
                // no concurrent commit could have occurred - this is a real DELETE
                Ok(Delta::new_delete())
            } else {
                if storage
                    .get::<0>(
                        &IteratorPool::new(),
                        write_key,
                        commit_sequence_number.previous(),
                        StorageCounters::DISABLED,
                    )?
                    .is_some()
                {
                    // exists in storage before DELETE is committed
                    Ok(Delta::new_delete())
                } else {
                    // does not exist in storage before DELETE is committed
                    Ok(Delta::default())
                }
            }
        }
        Write::Put { action, value, .. } => {
            if open_sequence_number.next() >= commit_sequence_number {
                // no concurrent commit could have occurred - fall back to the flag
                match action.load(std::sync::atomic::Ordering::Relaxed) {
                    PutAction::Nop => Ok(Delta::new_nop()),
                    PutAction::Insert => Ok(Delta::new_insert()),
                    PutAction::Overwrite => Ok(Delta::new_overwrite()),
                }
            } else {
                match storage.get::<0>(
                    &IteratorPool::new(),
                    write_key,
                    commit_sequence_number.previous(),
                    StorageCounters::DISABLED,
                )? {
                    Some(stored_value) if *stored_value == **value => Ok(Delta::new_nop()), // exists in storage before PUT is committed
                    Some(_) => Ok(Delta::new_overwrite()), // exists in storage with a different value before PUT is committed
                    None => Ok(Delta::new_insert()),       // does not exist in storage before PUT is committed
                }
            }
        }
    }
}

struct CommittedWrites {
    open_sequence_number: SequenceNumber,
    operations: OperationsBuffer,
}

impl fmt::Debug for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const INDENT: usize = 4;

        let pretty = f.alternate();

        writeln!(f, "Statistics {{")?;

        macro_rules! write_field {
            ($name:expr, $value:expr) => {
                if pretty {
                    writeln!(f, "{:INDENT$}{}: {:?},", "", $name, $value)?;
                } else {
                    write!(f, " {}: {:?},", $name, $value)?;
                }
            };
        }

        macro_rules! write_hashmap {
            ($name:expr, $map:expr) => {
                if pretty {
                    write!(f, "{:INDENT$}{}: {{", "", $name)?;
                    if $map.is_empty() {
                        writeln!(f, "}}")?;
                    } else {
                        writeln!(f)?;
                        for (key, value) in &$map {
                            writeln!(f, "{:indent$}{:?}: {:?},", "", key, value, indent = INDENT * 2)?;
                        }
                        writeln!(f, "{:INDENT$}}},", "")?;
                    }
                } else {
                    write!(f, " {}: {{", $name)?;
                    for (key, value) in &$map {
                        write!(f, " {:?}: {:?},", key, value)?;
                    }
                    write!(f, " }},")?;
                }
            };
        }

        write_field!("encoding_version", self.encoding_version);
        write_field!("sequence_number", self.sequence_number.number());
        write_field!("last_durable_write_sequence_number", self.last_durable_write_sequence_number);
        write_field!("last_durable_write_total_count", self.last_durable_write_total_count);
        write_field!("total_count", self.total_count);
        write_field!("total_thing_count", self.total_thing_count);
        write_field!("total_entity_count", self.total_entity_count);
        write_field!("total_relation_count", self.total_relation_count);
        write_field!("total_attribute_count", self.total_attribute_count);
        write_field!("total_role_count", self.total_role_count);
        write_field!("total_has_count", self.total_has_count);

        write_hashmap!("entity_counts", self.entity_counts);
        write_hashmap!("relation_counts", self.relation_counts);
        write_hashmap!("attribute_counts", self.attribute_counts);
        write_hashmap!("role_counts", self.role_counts);
        write_hashmap!("has_attribute_counts", self.has_attribute_counts);
        write_hashmap!("attribute_owner_counts", self.attribute_owner_counts);
        write_hashmap!("role_player_counts", self.role_player_counts);
        write_hashmap!("relation_role_counts", self.relation_role_counts);
        write_hashmap!("relation_role_player_counts", self.relation_role_player_counts);
        write_hashmap!("player_role_relation_counts", self.player_role_relation_counts);
        write_hashmap!("links_index_counts", self.links_index_counts);

        if pretty {
            write!(f, "}}")?;
        } else {
            write!(f, " }}")?;
        }

        Ok(())
    }
}

typedb_error!(
    pub StatisticsError(component = "Statistics", prefix = "STA") {
        DurablyWrite(1, "Error writing statistics summary WAL record.", typedb_source: DurabilityClientError),
        ReloadCommitData(2, "Failed to update statistics due to error reading commit records.", typedb_source: StorageRecoveryError),
        DataRead(3, "Error updating statistics due error reading MVCC storage layer.", source: MVCCReadError),
        Durability(4, "Error reading statistics from durability client.", typedb_source: DurabilityClientError),
        DurabilityRecordDeserialize(5, "Failed to deserialise durability record.", source: Arc<bincode::Error>),
    }
);

impl DurabilityRecord for Statistics {
    const RECORD_TYPE: DurabilityRecordType = 10;
    const RECORD_NAME: &'static str = "thing_statistics";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        bincode::serialize_into(writer, self)
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        bincode::deserialize_from(reader)
    }
}

impl UnsequencedDurabilityRecord for Statistics {}
