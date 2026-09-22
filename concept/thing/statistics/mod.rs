/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, hash_map},
    fmt,
    hash::Hash,
    ops::Bound,
    time::Instant,
};

use durability::DurabilityRecordType;
use encoding::{DecodableKey, graph::type_::vertex::PrefixedTypeVertexEncoding};
use error::typedb_error;
use resource::{
    constants::{
        database::{STATISTICS_DURABLE_WRITE_CHANGE_COUNT, STATISTICS_DURABLE_WRITE_SEQ_NUMBERS},
        snapshot::BUFFER_KEY_INLINE,
    },
    profile::StorageCounters,
};
use storage::{
    MVCCStorage,
    durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord, UnsequencedDurabilityRecord},
    iterator::MVCCReadError,
    key_value::StorageKeyArray,
    keyspace::IteratorPool,
    record::CommitType,
    recovery::commit_recovery::{RecoveryCommitStatus, StorageRecoveryError, load_commit_data_from_with_context},
    sequence_number::SequenceNumber,
    snapshot::{
        ReadableSnapshot,
        buffer::OperationsBuffer,
        write::{PutAction, Write},
    },
};
use tracing::{Level, event};

use crate::{
    error::ConceptReadError,
    thing::{
        ThingAPI, attribute::Attribute, entity::Entity, object::Object, relation::Relation,
        statistics::deltas::CommitDeltas, thing_manager::ThingManager,
    },
    type_::{
        TypeAPI, attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
        relation_type::RelationType, role_type::RoleType,
    },
};

pub mod deltas;

#[derive(Debug, Clone, Copy)]
#[repr(u64)]
pub enum StatisticsEncodingVersion {
    V0 = 0,
}

impl From<StatisticsEncodingVersion> for u64 {
    fn from(value: StatisticsEncodingVersion) -> u64 {
        value as u64
    }
}

impl TryFrom<u64> for StatisticsEncodingVersion {
    type Error = (); // TODO

    fn try_from(u64: u64) -> Result<Self, ()> {
        match u64 {
            0 => Ok(Self::V0),
            _ => Err(()),
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

/// Thing statistics, reflecting a snapshot of statistics accurate as of a particular sequence number
/// When types are undefined, we retain the last count of the instances of the type
/// Invariant: all undefined types are
#[derive(Clone)]
pub struct Statistics {
    encoding_version: StatisticsEncodingVersion,
    pub sequence_number: SequenceNumber,

    pub last_durable_write_sequence_number: SequenceNumber,
    pub last_durable_write_total_count: u64,

    pub total_count: u64,

    pub total_thing_count: u64,
    pub total_entity_count: u64,
    pub total_relation_count: u64,
    pub total_attribute_count: u64,
    pub total_role_count: u64,
    pub total_has_count: u64,

    pub entity_counts: HashMap<EntityType, u64>,
    pub relation_counts: HashMap<RelationType, u64>,
    pub attribute_counts: HashMap<AttributeType, u64>,
    pub role_counts: HashMap<RoleType, u64>,

    pub has_attribute_counts: DoubleHashMap<ObjectType, AttributeType, u64>,
    pub attribute_owner_counts: DoubleHashMap<AttributeType, ObjectType, u64>,
    pub role_player_counts: DoubleHashMap<ObjectType, RoleType, u64>,
    pub relation_role_counts: DoubleHashMap<RelationType, RoleType, u64>,
    pub relation_role_player_counts: TripleHashMap<RelationType, RoleType, ObjectType, u64>,
    pub player_role_relation_counts: TripleHashMap<ObjectType, RoleType, RelationType, u64>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub links_index_counts: DoubleHashMap<ObjectType, ObjectType, u64>,
    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl Statistics {
    const ENCODING_VERSION: StatisticsEncodingVersion = StatisticsEncodingVersion::V0;
    const COMMIT_CONTEXT_SIZE: u64 = 8;
    const COMMIT_CONTEXT_MEMORY_LIMIT: usize = 1 << 30; // 1 GiB

    pub fn new(sequence_number: SequenceNumber) -> Self {
        Statistics {
            encoding_version: Self::ENCODING_VERSION,
            sequence_number,
            last_durable_write_total_count: 0,
            last_durable_write_sequence_number: sequence_number,
            total_count: 0,
            total_thing_count: 0,
            total_entity_count: 0,
            total_relation_count: 0,
            total_attribute_count: 0,
            total_role_count: 0,
            total_has_count: 0,
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
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<Self, Box<ConceptReadError>> {
        let mut entity_counts = HashMap::new();
        for entity in thing_manager.get_entities(snapshot, storage_counters.clone()) {
            *entity_counts.entry(entity?.type_()).or_default() += 1;
        }

        let mut relation_counts = HashMap::new();
        for relation in thing_manager.get_relations(snapshot, storage_counters.clone()) {
            *relation_counts.entry(relation?.type_()).or_default() += 1;
        }

        let mut attribute_counts = HashMap::new();
        for attribute in thing_manager.get_attributes(snapshot, storage_counters.clone())? {
            *attribute_counts.entry(attribute?.type_()).or_default() += 1;
        }

        let mut has_attribute_counts = DoubleHashMap::new();
        let mut attribute_owner_counts = DoubleHashMap::new();
        for has in thing_manager.get_has(snapshot, storage_counters.clone()) {
            let (has, _) = has?;
            let owner = has.owner().type_();
            let attribute = has.attribute().type_();
            *has_attribute_counts.double_entry(owner, attribute).or_default() += 1;
            *attribute_owner_counts.double_entry(attribute, owner).or_default() += 1;
        }

        let mut role_counts = HashMap::new();
        let mut role_player_counts = DoubleHashMap::new();
        let mut relation_role_counts = DoubleHashMap::new();
        let mut relation_role_player_counts = TripleHashMap::new();
        let mut player_role_relation_counts = TripleHashMap::new();
        for links in thing_manager.get_links(snapshot, storage_counters.clone()) {
            let (links, _) = links?;
            let relation = links.relation().type_();
            let role = links.role_type();
            let player = links.player().type_();
            *role_counts.entry(role).or_default() += 1;
            *role_player_counts.double_entry(player, role).or_default() += 1;
            *relation_role_counts.double_entry(relation, role).or_default() += 1;
            *relation_role_player_counts.triple_entry(relation, role, player).or_default() += 1;
            *player_role_relation_counts.triple_entry(player, role, relation).or_default() += 1;
        }

        let mut links_index_counts = DoubleHashMap::new();
        for links_index in thing_manager.iterate_all_indexed_relations(snapshot, storage_counters)? {
            let ((player1, player2, ..), _) = links_index?;
            *links_index_counts.double_entry(player1.type_(), player2.type_()).or_default() += 1;
        }

        let total_entity_count = entity_counts.values().sum();
        let total_relation_count = relation_counts.values().sum();
        let total_attribute_count = attribute_counts.values().sum();
        let total_thing_count = total_entity_count + total_relation_count + total_attribute_count;

        let total_role_count = role_counts.values().sum();
        let total_has_count = has_attribute_counts.values().flat_map(|x| x.values()).sum();

        // attribute countrs and links index counts are not included in the total count
        let total_count = total_entity_count + total_relation_count + total_has_count + total_role_count;

        Ok(Self {
            encoding_version: Self::ENCODING_VERSION,
            sequence_number: snapshot.open_sequence_number(),
            last_durable_write_sequence_number: SequenceNumber::MIN,
            last_durable_write_total_count: 0,
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

    pub fn update(
        &mut self,
        commit_deltas: &CommitDeltas,
        durability: &impl DurabilityClient,
    ) -> Result<(), DurabilityClientError> {
        let CommitDeltas {
            encoding_version: _,
            commit_sequence_number,
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

        let mut total_delta = 0;

        for (&entity_type, delta) in entity_deltas {
            self.update_entities(entity_type, delta.net_change());
            total_delta += delta.net_change();
        }
        for (&relation_type, delta) in relation_deltas {
            self.update_relations(relation_type, delta.net_change());
            total_delta += delta.net_change();
        }
        for (&attribute_type, delta) in attribute_deltas {
            self.update_attributes(attribute_type, delta.net_change());
        }

        for (&owner_type, attribute_deltas) in has_attribute_deltas {
            for (&attribute_type, delta) in attribute_deltas {
                self.update_has(owner_type, attribute_type, delta.net_change());
                total_delta += delta.net_change();
            }
        }

        for (&relation_type, role_player_deltas) in relation_role_player_deltas {
            for (&role_type, player_deltas) in role_player_deltas {
                for (&player_type, delta) in player_deltas {
                    self.update_role_player(player_type, role_type, relation_type, delta.net_change());
                    total_delta += delta.net_change();
                }
            }
        }

        for (&player_1_type, player_2_type_deltas) in links_index_deltas {
            for (&player_2_type, delta) in player_2_type_deltas {
                self.update_indexed_player(player_1_type, player_2_type, delta.net_change());
            }
        }

        self.total_count = self.total_count.checked_add_signed(total_delta).unwrap();

        self.sequence_number = *commit_sequence_number;

        self.may_durably_write(durability)?;

        Ok(())
    }

    pub fn may_synchronise(&mut self, storage: &MVCCStorage<impl DurabilityClient>) -> Result<(), StatisticsError> {
        use StatisticsError::{DataRead, DurablyWrite, ReloadCommitData};

        let storage_watermark = storage.snapshot_watermark();
        debug_assert!(self.sequence_number <= storage_watermark);
        if self.sequence_number == storage_watermark {
            return Ok(());
        }

        let start = Instant::now();

        let mut data_commits = BTreeMap::new();

        let wal_commit_records = load_commit_data_from_with_context(
            self.sequence_number,
            Self::COMMIT_CONTEXT_SIZE,
            storage.durability(),
            Self::COMMIT_CONTEXT_MEMORY_LIMIT,
        )
        .map_err(|err| ReloadCommitData { typedb_source: err })?;

        let mut last_included = None;

        for (seq, status) in wal_commit_records {
            match status {
                RecoveryCommitStatus::Pending(_) => {
                    // there's a gap/incomplete data in the log that means we can't apply beyond this sequence number
                    break;
                }
                RecoveryCommitStatus::Validated(record) => {
                    let commit_type = record.commit_type();
                    let writes = CommittedWrites {
                        open_sequence_number: record.open_sequence_number(),
                        operations: record.into_operations(),
                    };
                    match commit_type {
                        CommitType::Data => _ = data_commits.insert(seq, writes),
                        CommitType::Schema => {
                            if self.sequence_number < seq {
                                // If last write was at Seq[11] and this schema commit is at Seq[12],
                                // no changes need to be applied or persisted.
                                if self.last_durable_write_sequence_number.next() < seq {
                                    self.update_writes(&data_commits, storage)
                                        .map_err(|err| DataRead { source: err })?;
                                    self.durably_write(storage.durability())
                                        .map_err(|err| DurablyWrite { typedb_source: err })?;
                                }
                                self.update_writes(&BTreeMap::from([(seq, writes)]), storage)
                                    .map_err(|err| DataRead { source: err })?;
                            }
                            data_commits.clear();
                        }
                    };
                    last_included = Some(seq);
                }
                RecoveryCommitStatus::Rejected => {
                    last_included = Some(seq);
                }
            }
        }

        self.update_writes(&data_commits, storage).map_err(|err| DataRead { source: err })?;

        self.may_durably_write(storage.durability()).map_err(|err| DurablyWrite { typedb_source: err })?;

        if let Some(last_included) = last_included {
            self.sequence_number = last_included;
        }

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
            u64::abs_diff(self.total_count, self.last_durable_write_total_count);
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

    fn update_writes<D>(
        &mut self,
        commits: &BTreeMap<SequenceNumber, CommittedWrites>,
        storage: &MVCCStorage<D>,
    ) -> Result<(), MVCCReadError> {
        for (sequence_number, writes) in commits.range(self.sequence_number.next()..) {
            let delta = self.update_write(*sequence_number, writes, commits, storage)?;
            self.total_count = self.total_count.checked_add_signed(delta).unwrap();
            self.sequence_number = *sequence_number;
        }
        Ok(())
    }

    fn update_write<D>(
        &mut self,
        commit_sequence_number: SequenceNumber,
        writes: &CommittedWrites,
        commits: &BTreeMap<SequenceNumber, CommittedWrites>,
        storage: &MVCCStorage<D>,
    ) -> Result<i64, MVCCReadError> {
        type CleanupFn = Box<dyn FnOnce(&mut Statistics)>;

        let mut total_delta = 0;
        let mut deferred_type_cleanups: Vec<CleanupFn> = Vec::new();

        for (key, write) in writes.operations.iterate_writes() {
            let delta =
                write_to_delta(&key, &write, writes.open_sequence_number, commit_sequence_number, commits, storage)?;
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

    fn saturating_add(count: &mut u64, delta: i64, label: &str) {
        match count.checked_add_signed(delta) {
            Some(value) => *count = value,
            None => {
                diagnostics::error_with_report!(
                    "Unexpected underflow in statistics {} count: {} + {}",
                    label,
                    *count,
                    delta
                );
                *count = 0;
            }
        }
    }

    fn update_entities(&mut self, entity_type: EntityType, delta: i64) {
        let count = self.entity_counts.entry(entity_type).or_default();
        Self::saturating_add(count, delta, "entity");
        Self::saturating_add(&mut self.total_entity_count, delta, "total_entity");
        Self::saturating_add(&mut self.total_thing_count, delta, "total_thing");
    }

    fn update_relations(&mut self, relation_type: RelationType, delta: i64) {
        let count = self.relation_counts.entry(relation_type).or_default();
        Self::saturating_add(count, delta, "relation");
        Self::saturating_add(&mut self.total_relation_count, delta, "total_relation");
        Self::saturating_add(&mut self.total_thing_count, delta, "total_thing");
    }

    fn update_attributes(&mut self, attribute_type: AttributeType, delta: i64) {
        let count = self.attribute_counts.entry(attribute_type).or_default();
        Self::saturating_add(count, delta, "attribute");
        Self::saturating_add(&mut self.total_attribute_count, delta, "total_attribute");
        Self::saturating_add(&mut self.total_thing_count, delta, "total_thing");
    }

    fn update_has(&mut self, owner_type: ObjectType, attribute_type: AttributeType, delta: i64) {
        let attribute_count = self.has_attribute_counts.double_entry(owner_type, attribute_type).or_default();
        Self::saturating_add(attribute_count, delta, "has_attribute");
        let owner_count = self.attribute_owner_counts.double_entry(attribute_type, owner_type).or_default();
        Self::saturating_add(owner_count, delta, "attribute_owner");
        Self::saturating_add(&mut self.total_has_count, delta, "total_has");
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType,
        role_type: RoleType,
        relation_type: RelationType,
        delta: i64,
    ) {
        let role_count = self.role_counts.entry(role_type).or_default();
        Self::saturating_add(role_count, delta, "role");
        Self::saturating_add(&mut self.total_role_count, delta, "total_role");
        let role_player_count = self.role_player_counts.double_entry(player_type, role_type).or_default();
        Self::saturating_add(role_player_count, delta, "role_player");
        let relation_role_count = self.relation_role_counts.double_entry(relation_type, role_type).or_default();
        Self::saturating_add(relation_role_count, delta, "relation_role");
        let relation_role_player_count =
            self.relation_role_player_counts.triple_entry(relation_type, role_type, player_type).or_default();
        Self::saturating_add(relation_role_player_count, delta, "relation_role_player");
        let player_role_relation_count =
            self.player_role_relation_counts.triple_entry(player_type, role_type, relation_type).or_default();
        Self::saturating_add(player_role_relation_count, delta, "player_role_relation");
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType, player_2_type: ObjectType, delta: i64) {
        let player_1_to_2_index_count = self.links_index_counts.double_entry(player_1_type, player_2_type).or_default();
        Self::saturating_add(player_1_to_2_index_count, delta, "player_1_to_2_index");
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
    fn largest_difference_frac_maps<T: Hash + Eq>(first: &HashMap<T, u64>, second: &HashMap<T, u64>) -> f64 {
        let mut largest = 0.0;
        for (key, first_value) in first {
            let second_value = second.get(key).copied().unwrap_or(0);
            if *first_value == 0 && second_value == 0 {
                continue;
            } else if second_value == 0 || *first_value == 0 {
                return f64::MAX;
            }
            let difference = (*first_value as f64 - second_value as f64).abs();
            let frac = difference / (min(*first_value, second_value) as f64);
            if frac > largest {
                largest = frac;
            }
        }
        for (key, second_value) in second {
            let first_value = first.get(key).copied().unwrap_or(0);
            if first_value == 0 && *second_value == 0 {
                continue;
            } else if *second_value == 0 || first_value == 0 {
                return f64::MAX;
            }
            // if both maps have a non-zero value, the first loop must have handled it
        }
        largest
    }

    fn largest_difference_frac_map_maps<T: Hash + Eq, U: Hash + Eq>(
        first: &HashMap<T, HashMap<U, u64>>,
        second: &HashMap<T, HashMap<U, u64>>,
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
        self.total_count = 0;
        self.total_thing_count = 0;
        self.total_entity_count = 0;
        self.total_relation_count = 0;
        self.total_attribute_count = 0;
        self.total_role_count = 0;
        self.total_has_count = 0;
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
    write_key: &StorageKeyArray<{ BUFFER_KEY_INLINE }>,
    write: &Write,
    open_sequence_number: SequenceNumber,
    commit_sequence_number: SequenceNumber,
    commits: &BTreeMap<SequenceNumber, CommittedWrites>,
    storage: &MVCCStorage<D>,
) -> Result<i64, MVCCReadError> {
    let concurrent_commit_range = (Bound::Excluded(open_sequence_number), Bound::Excluded(commit_sequence_number));
    match write {
        Write::Insert { .. } => Ok(1),
        Write::Delete => {
            if commits.range(concurrent_commit_range).any(|(_, writes)| {
                matches!(
                    writes.operations.writes_in(write_key.keyspace_id()).writes_get(write_key.bytes()),
                    Some(Write::Delete)
                )
            }) {
                Ok(0)
            } else {
                Ok(-1)
            }
        }
        Write::Put { action, .. } => {
            // PUT operation which we may have a concurrent commit and may or may not be inserted in the end
            // The easiest way to check whether it was ultimately committed or not is to open the storage at
            // CommitSequenceNumber - 1, and check if it exists. If it exists, we don't count. If it does, we do.
            // However, this induces a read for every PUT, even though 99% of time there is no concurrent put.

            // We only read from storage, if we can't tell from the current set of commits whether a predecessor
            // could have written the same key (open < commits start)

            let first_commit_sequence_number = *commits.first_key_value().unwrap().0;

            if let Some(write) = commits.range(concurrent_commit_range).rev().find_map(|(_, writes)| {
                writes.operations.writes_in(write_key.keyspace_id()).writes_get(write_key.bytes())
            }) {
                match write {
                    Write::Insert { .. } | Write::Put { .. } => Ok(0),
                    Write::Delete => Ok(1),
                }
            } else if open_sequence_number.next() < first_commit_sequence_number {
                if storage
                    .get::<0>(
                        &IteratorPool::new(),
                        write_key,
                        commit_sequence_number.previous(),
                        StorageCounters::DISABLED,
                    )?
                    .is_some()
                {
                    // exists in storage before PUT is committed
                    Ok(0)
                } else {
                    // does not exist in storage before PUT is committed
                    Ok(1)
                }
            } else {
                // no concurrent commit could have occurred - fall back to the flag
                if action.load(std::sync::atomic::Ordering::Relaxed) != PutAction::Nop { Ok(1) } else { Ok(0) }
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

mod serialise {
    use std::{collections::HashMap, fmt};

    use encoding::graph::{
        Typed,
        type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
    };
    use serde::{
        Deserialize, Deserializer, Serialize, Serializer, de,
        de::{MapAccess, SeqAccess, Visitor},
        ser::SerializeStruct,
    };

    use crate::{
        thing::statistics::{Statistics, StatisticsEncodingVersion},
        type_::{
            attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
            relation_type::RelationType, role_type::RoleType,
        },
    };

    enum Field {
        StatisticsVersion,
        OpenSequenceNumber,
        LastDurableWriteTotalCount,
        TotalCount,
        TotalThingCount,
        TotalEntityCount,
        TotalRelationCount,
        TotalAttributeCount,
        TotalRoleCount,
        TotalHasCount,
        EntityCounts,
        RelationCounts,
        AttributeCounts,
        RoleCounts,
        HasAttributeCounts,
        AttributeOwnerCounts,
        RolePlayerCounts,
        RelationRoleCounts,
        RelationRolePlayerCounts,
        PlayerRoleRelationCounts,
        LinksIndexCounts,
    }

    impl Field {
        const NAMES: [&'static str; 21] = [
            Self::StatisticsVersion.name(),
            Self::OpenSequenceNumber.name(),
            Self::LastDurableWriteTotalCount.name(),
            Self::TotalCount.name(),
            Self::TotalThingCount.name(),
            Self::TotalEntityCount.name(),
            Self::TotalRelationCount.name(),
            Self::TotalAttributeCount.name(),
            Self::TotalRoleCount.name(),
            Self::TotalHasCount.name(),
            Self::EntityCounts.name(),
            Self::RelationCounts.name(),
            Self::AttributeCounts.name(),
            Self::RoleCounts.name(),
            Self::HasAttributeCounts.name(),
            Self::AttributeOwnerCounts.name(),
            Self::RolePlayerCounts.name(),
            Self::RelationRoleCounts.name(),
            Self::RelationRolePlayerCounts.name(),
            Self::PlayerRoleRelationCounts.name(),
            Self::LinksIndexCounts.name(),
        ];

        const fn name(&self) -> &str {
            match self {
                Field::StatisticsVersion => "StatisticsVersion",
                Field::OpenSequenceNumber => "OpenSequenceNumber",
                Field::LastDurableWriteTotalCount => "LastDurableWriteTotalCount",
                Field::TotalCount => "TotalCount",
                Field::TotalThingCount => "TotalThingCount",
                Field::TotalEntityCount => "TotalEntityCount",
                Field::TotalRelationCount => "TotalRelationCount",
                Field::TotalAttributeCount => "TotalAttributeCount",
                Field::TotalRoleCount => "TotalRoleCount",
                Field::TotalHasCount => "TotalHasCount",
                Field::EntityCounts => "EntityCounts",
                Field::RelationCounts => "RelationCounts",
                Field::AttributeCounts => "AttributeCounts",
                Field::RoleCounts => "RoleCounts",
                Field::HasAttributeCounts => "HasAttributeCounts",
                Field::AttributeOwnerCounts => "AttributeOwnerCounts",
                Field::RolePlayerCounts => "RolePlayerCounts",
                Field::RelationRoleCounts => "RelationRoleCounts",
                Field::RelationRolePlayerCounts => "RelationRolePlayerCounts",
                Field::PlayerRoleRelationCounts => "RolePlayerRelationCounts",
                Field::LinksIndexCounts => "PlayerIndexCounts",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "StatisticsVersion" => Some(Field::StatisticsVersion),
                "OpenSequenceNumber" => Some(Field::OpenSequenceNumber),
                "LastDurableWriteTotalCount" => Some(Field::LastDurableWriteTotalCount),
                "TotalCount" => Some(Field::TotalCount),
                "TotalThingCount" => Some(Field::TotalThingCount),
                "TotalEntityCount" => Some(Field::TotalEntityCount),
                "TotalRelationCount" => Some(Field::TotalRelationCount),
                "TotalAttributeCount" => Some(Field::TotalAttributeCount),
                "TotalRoleCount" => Some(Field::TotalRoleCount),
                "TotalHasCount" => Some(Field::TotalHasCount),
                "EntityCounts" => Some(Field::EntityCounts),
                "RelationCounts" => Some(Field::RelationCounts),
                "AttributeCounts" => Some(Field::AttributeCounts),
                "RoleCounts" => Some(Field::RoleCounts),
                "HasAttributeCounts" => Some(Field::HasAttributeCounts),
                "AttributeOwnerCounts" => Some(Field::AttributeOwnerCounts),
                "RolePlayerCounts" => Some(Field::RolePlayerCounts),
                "RelationRoleCounts" => Some(Field::RelationRoleCounts),
                "RelationRolePlayerCounts" => Some(Field::RelationRolePlayerCounts),
                "RolePlayerRelationCounts" => Some(Field::PlayerRoleRelationCounts),
                "PlayerIndexCounts" => Some(Field::LinksIndexCounts),
                _ => None,
            }
        }
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
    pub(super) enum SerialisableType {
        Entity(TypeIDUInt),
        Relation(TypeIDUInt),
        Attribute(TypeIDUInt),
        Role(TypeIDUInt),
    }

    impl SerialisableType {
        pub(crate) fn into_entity_type(self) -> EntityType {
            match self {
                Self::Entity(id) => EntityType::build_from_type_id(TypeID::new(id)),
                _ => panic!("Incompatible conversion."),
            }
        }

        pub(crate) fn into_relation_type(self) -> RelationType {
            match self {
                Self::Relation(id) => RelationType::build_from_type_id(TypeID::new(id)),
                _ => panic!("Incompatible conversion."),
            }
        }

        pub(crate) fn into_object_type(self) -> ObjectType {
            match self {
                Self::Entity(id) => ObjectType::Entity(EntityType::build_from_type_id(TypeID::new(id))),
                Self::Relation(id) => ObjectType::Relation(RelationType::build_from_type_id(TypeID::new(id))),
                _ => panic!("Incompatible conversion."),
            }
        }

        pub(crate) fn into_attribute_type(self) -> AttributeType {
            match self {
                Self::Attribute(id) => AttributeType::build_from_type_id(TypeID::new(id)),
                _ => panic!("Incompatible conversion."),
            }
        }

        pub(crate) fn into_role_type(self) -> RoleType {
            match self {
                Self::Role(id) => RoleType::build_from_type_id(TypeID::new(id)),
                _ => panic!("Incompatible conversion."),
            }
        }
    }

    impl From<ObjectType> for SerialisableType {
        fn from(object: ObjectType) -> Self {
            match object {
                ObjectType::Entity(entity) => Self::from(entity),
                ObjectType::Relation(relation) => Self::from(relation),
            }
        }
    }

    impl From<EntityType> for SerialisableType {
        fn from(entity: EntityType) -> Self {
            Self::Entity(entity.vertex().type_id_().as_u16())
        }
    }

    impl From<RelationType> for SerialisableType {
        fn from(relation: RelationType) -> Self {
            Self::Relation(relation.vertex().type_id_().as_u16())
        }
    }

    impl From<AttributeType> for SerialisableType {
        fn from(attribute: AttributeType) -> Self {
            Self::Attribute(attribute.vertex().type_id_().as_u16())
        }
    }

    impl From<RoleType> for SerialisableType {
        fn from(role_type: RoleType) -> Self {
            Self::Role(role_type.vertex().type_id_().as_u16())
        }
    }

    impl Serialize for StatisticsEncodingVersion {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            u64::serialize(&(*self).into(), serializer)
        }
    }

    impl<'de> Deserialize<'de> for StatisticsEncodingVersion {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let u64 = u64::deserialize(deserializer)?;
            Self::try_from(u64).map_err(|_| de::Error::invalid_value(de::Unexpected::Unsigned(u64), &"0"))
        }
    }

    impl Serialize for Statistics {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Statistics", Field::NAMES.len())?;
            state.serialize_field(Field::StatisticsVersion.name(), &self.encoding_version)?;

            state.serialize_field(Field::OpenSequenceNumber.name(), &self.sequence_number)?;
            state.serialize_field(Field::LastDurableWriteTotalCount.name(), &self.last_durable_write_total_count)?;

            state.serialize_field(Field::TotalCount.name(), &self.total_count)?;
            state.serialize_field(Field::TotalThingCount.name(), &self.total_thing_count)?;
            state.serialize_field(Field::TotalEntityCount.name(), &self.total_entity_count)?;
            state.serialize_field(Field::TotalRelationCount.name(), &self.total_relation_count)?;
            state.serialize_field(Field::TotalAttributeCount.name(), &self.total_attribute_count)?;
            state.serialize_field(Field::TotalRoleCount.name(), &self.total_role_count)?;
            state.serialize_field(Field::TotalHasCount.name(), &self.total_has_count)?;

            state.serialize_field(Field::EntityCounts.name(), &to_serialisable_map(&self.entity_counts))?;
            state.serialize_field(Field::RelationCounts.name(), &to_serialisable_map(&self.relation_counts))?;
            state.serialize_field(Field::AttributeCounts.name(), &to_serialisable_map(&self.attribute_counts))?;
            state.serialize_field(Field::RoleCounts.name(), &to_serialisable_map(&self.role_counts))?;

            state.serialize_field(
                Field::HasAttributeCounts.name(),
                &to_serialisable_map_map(&self.has_attribute_counts),
            )?;

            state.serialize_field(
                Field::AttributeOwnerCounts.name(),
                &to_serialisable_map_map(&self.attribute_owner_counts),
            )?;

            state
                .serialize_field(Field::RolePlayerCounts.name(), &to_serialisable_map_map(&self.role_player_counts))?;

            state.serialize_field(
                Field::RelationRoleCounts.name(),
                &to_serialisable_map_map(&self.relation_role_counts),
            )?;

            state.serialize_field(
                Field::RelationRolePlayerCounts.name(),
                &to_serialisable_map_map_map(&self.relation_role_player_counts),
            )?;

            state.serialize_field(
                Field::PlayerRoleRelationCounts.name(),
                &to_serialisable_map_map_map(&self.player_role_relation_counts),
            )?;

            state
                .serialize_field(Field::LinksIndexCounts.name(), &to_serialisable_map_map(&self.links_index_counts))?;

            state.end()
        }
    }

    pub(super) fn to_serialisable_map_map<Type1, Type2, Value>(
        map: &HashMap<Type1, HashMap<Type2, Value>>,
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, Value>>
    where
        Type1: Into<SerialisableType> + Clone,
        Type2: Into<SerialisableType> + Clone,
        Value: Copy,
    {
        map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map(value))).collect()
    }

    pub(super) fn to_serialisable_map_map_map<Type1, Type2, Type3, Value>(
        map: &HashMap<Type1, HashMap<Type2, HashMap<Type3, Value>>>,
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, HashMap<SerialisableType, Value>>>
    where
        Type1: Into<SerialisableType> + Clone,
        Type2: Into<SerialisableType> + Clone,
        Type3: Into<SerialisableType> + Clone,
        Value: Copy,
    {
        map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map_map(value))).collect()
    }

    pub(super) fn to_serialisable_map<Type_: Into<SerialisableType> + Clone, Value: Copy>(
        map: &HashMap<Type_, Value>,
    ) -> HashMap<SerialisableType, Value> {
        map.iter().map(|(type_, value)| (type_.clone().into(), *value)).collect()
    }

    pub(super) fn into_entity_map<Value: Copy>(map: HashMap<SerialisableType, Value>) -> HashMap<EntityType, Value> {
        map.into_iter().map(|(type_, value)| (type_.into_entity_type(), value)).collect()
    }

    pub(super) fn into_relation_map<Value: Copy>(
        map: HashMap<SerialisableType, Value>,
    ) -> HashMap<RelationType, Value> {
        map.into_iter().map(|(type_, value)| (type_.into_relation_type(), value)).collect()
    }

    pub(super) fn into_attribute_map<Value: Copy>(
        map: HashMap<SerialisableType, Value>,
    ) -> HashMap<AttributeType, Value> {
        map.into_iter().map(|(type_, value)| (type_.into_attribute_type(), value)).collect()
    }

    pub(super) fn into_role_map<Value: Copy>(map: HashMap<SerialisableType, Value>) -> HashMap<RoleType, Value> {
        map.into_iter().map(|(type_, value)| (type_.into_role_type(), value)).collect()
    }

    pub(super) fn into_object_map<Value: Copy>(map: HashMap<SerialisableType, Value>) -> HashMap<ObjectType, Value> {
        map.into_iter().map(|(type_, value)| (type_.into_object_type(), value)).collect()
    }

    impl<'de> Deserialize<'de> for Field {
        fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct FieldVisitor;

            impl Visitor<'_> for FieldVisitor {
                type Value = Field;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("Unrecognised field")
                }

                fn visit_str<E>(self, value: &str) -> Result<Field, E>
                where
                    E: de::Error,
                {
                    Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
                }
            }

            deserializer.deserialize_identifier(FieldVisitor)
        }
    }

    impl<'de> Deserialize<'de> for Statistics {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct StatisticsVisitor;

            impl<'de> Visitor<'de> for StatisticsVisitor {
                type Value = Statistics;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct StatisticsVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<Statistics, V::Error>
                where
                    V: SeqAccess<'de>,
                {
                    let statistics_version = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let sequence_number = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    let last_durable_write_total_count =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
                    let total_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?;
                    let total_thing_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?;
                    let total_entity_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(5, &self))?;
                    let total_relation_count =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(6, &self))?;
                    let total_attribute_count =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(7, &self))?;
                    let total_role_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(8, &self))?;
                    let total_has_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(9, &self))?;
                    let encoded_entity_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(10, &self))?;
                    let entity_counts = into_entity_map(encoded_entity_counts);
                    let encoded_relation_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(11, &self))?;
                    let relation_counts = into_relation_map(encoded_relation_counts);
                    let encoded_attribute_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(12, &self))?;
                    let attribute_counts = into_attribute_map(encoded_attribute_counts);
                    let encoded_role_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(13, &self))?;
                    let role_counts = into_role_map(encoded_role_counts);
                    let encoded_has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(14, &self))?;
                    let has_attribute_counts = encoded_has_attribute_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                        .collect();
                    let encoded_attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(15, &self))?;
                    let attribute_owner_counts = encoded_attribute_owner_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                        .collect();
                    let encoded_role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(16, &self))?;
                    let role_player_counts = encoded_role_player_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                        .collect();
                    let encoded_relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(17, &self))?;
                    let relation_role_counts = encoded_relation_role_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                        .collect();
                    let encoded_relation_role_player_counts: HashMap<
                        SerialisableType,
                        HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                    > = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(18, &self))?;
                    let relation_role_player_counts = encoded_relation_role_player_counts
                        .into_iter()
                        .map(|(type_1, map)| {
                            (
                                type_1.into_relation_type(),
                                map.into_iter()
                                    .map(|(type_1, map)| (type_1.into_role_type(), into_object_map(map)))
                                    .collect(),
                            )
                        })
                        .collect();
                    let encoded_player_role_relation_counts: HashMap<
                        SerialisableType,
                        HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                    > = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(19, &self))?;
                    let player_role_relation_counts = encoded_player_role_relation_counts
                        .into_iter()
                        .map(|(type_1, map)| {
                            (
                                type_1.into_object_type(),
                                map.into_iter()
                                    .map(|(type_1, map)| (type_1.into_role_type(), into_relation_map(map)))
                                    .collect(),
                            )
                        })
                        .collect();
                    let encoded_links_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(20, &self))?;
                    let links_index_counts = encoded_links_index_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                        .collect();
                    Ok(Statistics {
                        encoding_version: statistics_version,
                        sequence_number,
                        last_durable_write_sequence_number: sequence_number,
                        last_durable_write_total_count,
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

                fn visit_map<V>(self, mut map: V) -> Result<Statistics, V::Error>
                where
                    V: MapAccess<'de>,
                {
                    let mut statistics_version = None;
                    let mut open_sequence_number = None;
                    let mut last_durable_write_total_count = None;
                    let mut total_count = None;
                    let mut total_thing_count = None;
                    let mut total_entity_count = None;
                    let mut total_relation_count = None;
                    let mut total_attribute_count = None;
                    let mut total_role_count = None;
                    let mut total_has_count = None;
                    let mut entity_counts = None;
                    let mut relation_counts = None;
                    let mut attribute_counts = None;
                    let mut role_counts = None;
                    let mut has_attribute_counts = None;
                    let mut attribute_owner_counts = None;
                    let mut role_player_counts = None;
                    let mut relation_role_counts = None;
                    let mut relation_role_player_counts = None;
                    let mut player_role_relation_counts = None;
                    let mut links_indexs_counts = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::StatisticsVersion => {
                                if statistics_version.is_some() {
                                    return Err(de::Error::duplicate_field(Field::StatisticsVersion.name()));
                                }
                                statistics_version = Some(map.next_value()?);
                            }
                            Field::OpenSequenceNumber => {
                                if open_sequence_number.is_some() {
                                    return Err(de::Error::duplicate_field(Field::OpenSequenceNumber.name()));
                                }
                                open_sequence_number = Some(map.next_value()?);
                            }
                            Field::LastDurableWriteTotalCount => {
                                if total_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::LastDurableWriteTotalCount.name()));
                                }
                                last_durable_write_total_count = Some(map.next_value()?);
                            }
                            Field::TotalCount => {
                                if total_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalCount.name()));
                                }
                                total_count = Some(map.next_value()?);
                            }
                            Field::TotalThingCount => {
                                if total_thing_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalThingCount.name()));
                                }
                                total_thing_count = Some(map.next_value()?);
                            }
                            Field::TotalEntityCount => {
                                if total_entity_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalEntityCount.name()));
                                }
                                total_entity_count = Some(map.next_value()?);
                            }
                            Field::TotalRelationCount => {
                                if total_relation_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRelationCount.name()));
                                }
                                total_relation_count = Some(map.next_value()?);
                            }
                            Field::TotalAttributeCount => {
                                if total_attribute_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalAttributeCount.name()));
                                }
                                total_attribute_count = Some(map.next_value()?);
                            }
                            Field::TotalRoleCount => {
                                if total_role_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_role_count = Some(map.next_value()?);
                            }
                            Field::TotalHasCount => {
                                if total_has_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_has_count = Some(map.next_value()?);
                            }
                            Field::EntityCounts => {
                                if entity_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                entity_counts = Some(into_entity_map(map.next_value()?));
                            }
                            Field::RelationCounts => {
                                if relation_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationCounts.name()));
                                }
                                relation_counts = Some(into_relation_map(map.next_value()?));
                            }
                            Field::AttributeCounts => {
                                if attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeCounts.name()));
                                }
                                attribute_counts = Some(into_attribute_map(map.next_value()?));
                            }
                            Field::RoleCounts => {
                                if role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RoleCounts.name()));
                                }
                                role_counts = Some(into_role_map(map.next_value()?));
                            }
                            Field::HasAttributeCounts => {
                                if has_attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::HasAttributeCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                has_attribute_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                                        .collect(),
                                );
                            }
                            Field::AttributeOwnerCounts => {
                                if attribute_owner_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeOwnerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                attribute_owner_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                                        .collect(),
                                );
                            }
                            Field::RolePlayerCounts => {
                                if role_player_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RolePlayerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                role_player_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                                        .collect(),
                                );
                            }
                            Field::RelationRoleCounts => {
                                if relation_role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationRoleCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                relation_role_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                                        .collect(),
                                );
                            }
                            Field::RelationRolePlayerCounts => {
                                if relation_role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationRolePlayerCounts.name()));
                                }
                                let encoded: HashMap<
                                    SerialisableType,
                                    HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                                > = map.next_value()?;
                                relation_role_player_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| {
                                            (
                                                type_1.into_relation_type(),
                                                map.into_iter()
                                                    .map(|(type_1, map)| {
                                                        (type_1.into_role_type(), into_object_map(map))
                                                    })
                                                    .collect(),
                                            )
                                        })
                                        .collect(),
                                );
                            }
                            Field::PlayerRoleRelationCounts => {
                                if relation_role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::PlayerRoleRelationCounts.name()));
                                }
                                let encoded: HashMap<
                                    SerialisableType,
                                    HashMap<SerialisableType, HashMap<SerialisableType, u64>>,
                                > = map.next_value()?;
                                player_role_relation_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| {
                                            (
                                                type_1.into_object_type(),
                                                map.into_iter()
                                                    .map(|(type_1, map)| {
                                                        (type_1.into_role_type(), into_relation_map(map))
                                                    })
                                                    .collect(),
                                            )
                                        })
                                        .collect(),
                                );
                            }
                            Field::LinksIndexCounts => {
                                if links_indexs_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::LinksIndexCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                links_indexs_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                                        .collect(),
                                );
                            }
                        }
                    }

                    Ok(Statistics {
                        encoding_version: statistics_version
                            .ok_or_else(|| de::Error::missing_field(Field::StatisticsVersion.name()))?,
                        sequence_number: open_sequence_number
                            .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                        last_durable_write_total_count: last_durable_write_total_count
                            .ok_or_else(|| de::Error::missing_field(Field::LastDurableWriteTotalCount.name()))?,
                        last_durable_write_sequence_number: open_sequence_number
                            .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                        total_count: total_count.ok_or_else(|| de::Error::missing_field(Field::TotalCount.name()))?,
                        total_thing_count: total_thing_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalThingCount.name()))?,
                        total_entity_count: total_entity_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalEntityCount.name()))?,
                        total_relation_count: total_relation_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRelationCount.name()))?,
                        total_attribute_count: total_attribute_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalAttributeCount.name()))?,
                        total_role_count: total_role_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRoleCount.name()))?,
                        total_has_count: total_has_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalHasCount.name()))?,
                        entity_counts: entity_counts
                            .ok_or_else(|| de::Error::missing_field(Field::EntityCounts.name()))?,
                        relation_counts: relation_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationCounts.name()))?,
                        attribute_counts: attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeCounts.name()))?,
                        role_counts: role_counts.ok_or_else(|| de::Error::missing_field(Field::RoleCounts.name()))?,
                        has_attribute_counts: has_attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::HasAttributeCounts.name()))?,
                        attribute_owner_counts: attribute_owner_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeOwnerCounts.name()))?,
                        role_player_counts: role_player_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RolePlayerCounts.name()))?,
                        relation_role_counts: relation_role_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationRoleCounts.name()))?,
                        relation_role_player_counts: relation_role_player_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationRolePlayerCounts.name()))?,
                        player_role_relation_counts: player_role_relation_counts
                            .ok_or_else(|| de::Error::missing_field(Field::PlayerRoleRelationCounts.name()))?,
                        links_index_counts: links_indexs_counts
                            .ok_or_else(|| de::Error::missing_field(Field::LinksIndexCounts.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("Statistics", &Field::NAMES, StatisticsVisitor)
        }
    }
}
