/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fmt,
    hash::Hash,
    ops::Bound,
    time::Instant,
};

use bytes::Bytes;
use durability::{DurabilityRecordType, DurabilitySequenceNumber};
use encoding::graph::{
    thing::{
        edge::{ThingEdgeHas, ThingEdgeIndexedRelation, ThingEdgeLinks},
        vertex_attribute::AttributeVertex,
        vertex_object::ObjectVertex,
        ThingVertex,
    },
    type_::vertex::{PrefixedTypeVertexEncoding, TypeID, TypeIDUInt, TypeVertexEncoding},
    Typed,
};
use error::typedb_error;
use resource::{
    constants::{database::STATISTICS_DURABLE_WRITE_CHANGE_PERCENT, snapshot::BUFFER_KEY_INLINE},
    profile::StorageCounters,
};
use serde::{Deserialize, Serialize};
use storage::{
    durability_client::{DurabilityClient, DurabilityClientError, DurabilityRecord, UnsequencedDurabilityRecord},
    isolation_manager::CommitType,
    iterator::MVCCReadError,
    key_value::{StorageKeyArray, StorageKeyReference},
    keyspace::IteratorPool,
    recovery::commit_recovery::{load_commit_data_from, RecoveryCommitStatus, StorageRecoveryError},
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write},
    MVCCStorage,
};
use tracing::{event, trace, Level};

use crate::{
    thing::{attribute::Attribute, entity::Entity, object::Object, relation::Relation, ThingAPI},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, TypeAPI,
    },
};

type StatisticsEncodingVersion = u64;

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

    pub has_attribute_counts: HashMap<ObjectType, HashMap<AttributeType, u64>>,
    pub attribute_owner_counts: HashMap<AttributeType, HashMap<ObjectType, u64>>,
    pub role_player_counts: HashMap<ObjectType, HashMap<RoleType, u64>>,
    pub relation_role_counts: HashMap<RelationType, HashMap<RoleType, u64>>,
    pub relation_role_player_counts: HashMap<RelationType, HashMap<RoleType, HashMap<ObjectType, u64>>>,
    pub player_role_relation_counts: HashMap<ObjectType, HashMap<RoleType, HashMap<RelationType, u64>>>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub links_index_counts: HashMap<ObjectType, HashMap<ObjectType, u64>>,
    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl Statistics {
    const ENCODING_VERSION: StatisticsEncodingVersion = 0;
    const COMMIT_CONTEXT_SIZE: u64 = 8;

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

    pub fn may_synchronise(&mut self, storage: &MVCCStorage<impl DurabilityClient>) -> Result<(), StatisticsError> {
        use StatisticsError::{DataRead, ReloadCommitData};

        let storage_watermark = storage.snapshot_watermark();
        debug_assert!(self.sequence_number <= storage_watermark);
        if self.sequence_number == storage_watermark {
            return Ok(());
        }

        let start = Instant::now();

        // make it a little more likely that we capture concurrent commits
        let load_start = DurabilitySequenceNumber::new(
            self.sequence_number.number().saturating_sub(Self::COMMIT_CONTEXT_SIZE).max(1),
        );

        let mut data_commits = BTreeMap::new();
        for (seq, status) in load_commit_data_from(load_start, storage.durability(), usize::MAX)
            .map_err(|err| ReloadCommitData { typedb_source: err })?
        {
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
                                    self.durably_write(storage.durability())?;
                                }
                                self.update_writes(&BTreeMap::from([(seq, writes)]), storage)
                                    .map_err(|err| DataRead { source: err })?;
                            }
                            data_commits.clear();
                        }
                    }
                }
                RecoveryCommitStatus::Rejected => {}
            }
        }

        self.update_writes(&data_commits, storage).map_err(|err| DataRead { source: err })?;

        let change_since_last_durable_write = self.total_count as f64 - self.last_durable_write_total_count as f64;
        if change_since_last_durable_write.abs() / self.last_durable_write_total_count as f64
            > STATISTICS_DURABLE_WRITE_CHANGE_PERCENT
        {
            self.durably_write(storage.durability())?;
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

    pub fn durably_write(&mut self, durability: &impl DurabilityClient) -> Result<(), StatisticsError> {
        use StatisticsError::DurablyWrite;
        durability.unsequenced_write(self).map_err(|err| DurablyWrite { typedb_source: err })?;
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
        let mut total_delta = 0;
        for (key, write) in writes.operations.iterate_writes() {
            let delta =
                write_to_delta(&key, &write, writes.open_sequence_number, commit_sequence_number, commits, storage)?;
            if ObjectVertex::is_entity_vertex(StorageKeyReference::from(&key)) {
                let type_ = Entity::new(ObjectVertex::decode(key.bytes())).type_();
                self.update_entities(type_, delta);
                total_delta += delta;
            } else if ObjectVertex::is_relation_vertex(StorageKeyReference::from(&key)) {
                let type_ = Relation::new(ObjectVertex::decode(key.bytes())).type_();
                self.update_relations(type_, delta);
                total_delta += delta;
            } else if AttributeVertex::is_attribute_vertex(StorageKeyReference::from(&key)) {
                let type_ = Attribute::new(AttributeVertex::decode(key.bytes())).type_();
                self.update_attributes(type_, delta);
            } else if ThingEdgeHas::is_has(&key) {
                let edge = ThingEdgeHas::decode(Bytes::Reference(key.bytes()));
                self.update_has(Object::new(edge.from()).type_(), Attribute::new(edge.to()).type_(), delta);
                total_delta += delta;
            } else if ThingEdgeLinks::is_links(&key) {
                let edge = ThingEdgeLinks::decode(Bytes::Reference(key.bytes()));
                let role_type = RoleType::build_from_type_id(edge.role_id());
                self.update_role_player(
                    Object::new(edge.to()).type_(),
                    role_type,
                    Relation::new(edge.from()).type_(),
                    delta,
                );
                total_delta += delta;
            } else if ThingEdgeIndexedRelation::is_index(&key) {
                let edge = ThingEdgeIndexedRelation::decode(Bytes::Reference(key.bytes()));
                self.update_indexed_player(Object::new(edge.from()).type_(), Object::new(edge.to()).type_(), delta);
                // note: don't update total count based on index
            } else if EntityType::is_decodable_from_key(&key) {
                let type_ = EntityType::read_from(Bytes::Reference(key.bytes()).into_owned());
                if matches!(write, Write::Delete) {
                    self.entity_counts.remove(&type_);
                    self.clear_object_type(ObjectType::Entity(type_));
                }
                // note: don't update total count based on type updates
            } else if RelationType::is_decodable_from_key(&key) {
                let type_ = RelationType::read_from(Bytes::Reference(key.bytes()).into_owned());
                if matches!(write, Write::Delete) {
                    self.relation_counts.remove(&type_);
                    self.relation_role_counts.remove(&type_);
                    let as_object_type = ObjectType::Relation(type_);
                    self.clear_object_type(as_object_type);
                }
                // note: don't update total count based on type updates
            } else if AttributeType::is_decodable_from_key(&key) {
                let type_ = AttributeType::read_from(Bytes::Reference(key.bytes()).into_owned());
                if matches!(write, Write::Delete) {
                    self.attribute_counts.remove(&type_);
                    self.attribute_owner_counts.remove(&type_);
                    for map in self.has_attribute_counts.values_mut() {
                        map.remove(&type_);
                    }
                    self.has_attribute_counts.retain(|_, map| !map.is_empty());
                }
                // note: don't update total count based on type updates
            } else if RoleType::is_decodable_from_key(&key) {
                let type_ = RoleType::read_from(Bytes::Reference(key.bytes()).into_owned());
                if matches!(write, Write::Delete) {
                    self.role_counts.remove(&type_);
                    for map in self.role_player_counts.values_mut() {
                        map.remove(&type_);
                    }
                    self.role_player_counts.retain(|_, map| !map.is_empty());
                    for map in self.relation_role_counts.values_mut() {
                        map.remove(&type_);
                    }
                    self.relation_role_counts.retain(|_, map| !map.is_empty());
                }
                // note: don't update total count based on type updates
            }
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

    fn update_entities(&mut self, entity_type: EntityType, delta: i64) {
        let count = self.entity_counts.entry(entity_type).or_default();
        *count = count.checked_add_signed(delta).unwrap();
        self.total_entity_count = self.total_entity_count.checked_add_signed(delta).unwrap();
        self.total_thing_count = self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_relations(&mut self, relation_type: RelationType, delta: i64) {
        let count = self.relation_counts.entry(relation_type).or_default();
        *count = count.checked_add_signed(delta).unwrap();
        self.total_relation_count = self.total_relation_count.checked_add_signed(delta).unwrap();
        self.total_thing_count = self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_attributes(&mut self, attribute_type: AttributeType, delta: i64) {
        let count = self.attribute_counts.entry(attribute_type).or_default();
        *count = count.checked_add_signed(delta).unwrap();
        self.total_attribute_count = self.total_attribute_count.checked_add_signed(delta).unwrap();
        self.total_thing_count = self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_has(&mut self, owner_type: ObjectType, attribute_type: AttributeType, delta: i64) {
        let attribute_count =
            self.has_attribute_counts.entry(owner_type).or_default().entry(attribute_type).or_default();
        *attribute_count = attribute_count.checked_add_signed(delta).unwrap();
        let owner_count = self.attribute_owner_counts.entry(attribute_type).or_default().entry(owner_type).or_default();
        *owner_count = owner_count.checked_add_signed(delta).unwrap();
        self.total_has_count = self.total_has_count.checked_add_signed(delta).unwrap();
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType,
        role_type: RoleType,
        relation_type: RelationType,
        delta: i64,
    ) {
        let role_count = self.role_counts.entry(role_type).or_default();
        *role_count = role_count.checked_add_signed(delta).unwrap();
        self.total_role_count = self.total_role_count.checked_add_signed(delta).unwrap();
        let role_player_count = self.role_player_counts.entry(player_type).or_default().entry(role_type).or_default();
        *role_player_count = role_player_count.checked_add_signed(delta).unwrap();
        let relation_role_count =
            self.relation_role_counts.entry(relation_type).or_default().entry(role_type).or_default();
        *relation_role_count = relation_role_count.checked_add_signed(delta).unwrap();
        let relation_role_player_count = self
            .relation_role_player_counts
            .entry(relation_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(player_type)
            .or_default();
        *relation_role_player_count = relation_role_player_count.checked_add_signed(delta).unwrap();
        let player_role_relation_count = self
            .player_role_relation_counts
            .entry(player_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(relation_type)
            .or_default();
        *player_role_relation_count = player_role_relation_count.checked_add_signed(delta).unwrap();
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType, player_2_type: ObjectType, delta: i64) {
        let player_1_to_2_index_count =
            self.links_index_counts.entry(player_1_type).or_default().entry(player_2_type).or_default();
        *player_1_to_2_index_count = match player_1_to_2_index_count.checked_add_signed(delta) {
            None => panic!(
                "Error with unsigned add player_1_to_2_index_count + delta: {} + {}",
                player_1_to_2_index_count, delta
            ),
            Some(value) => value,
        };
        if player_1_type != player_2_type {
            let player_2_to_1_index_count =
                self.links_index_counts.entry(player_2_type).or_default().entry(player_1_type).or_default();
            *player_2_to_1_index_count = match player_2_to_1_index_count.checked_add_signed(delta) {
                None => panic!(
                    "Error with unsigned add player_2_to_1_index_count: {} + {}",
                    player_2_to_1_index_count, delta
                ),
                Some(value) => value,
            };
        }
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
                    writes.operations.writes_in(write_key.keyspace_id()).get_write(write_key.bytes()),
                    Some(Write::Delete)
                )
            }) {
                Ok(0)
            } else {
                Ok(-1)
            }
        }
        Write::Put { reinsert, .. } => {
            // PUT operation which we may have a concurrent commit and may or may not be inserted in the end
            // The easiest way to check whether it was ultimately committed or not is to open the storage at
            // CommitSequenceNumber - 1, and check if it exists. If it exists, we don't count. If it does, we do.
            // However, this induces a read for every PUT, even though 99% of time there is no concurrent put.

            // We only read from storage, if we can't tell from the current set of commits whether a predecessor
            // could have written the same key (open < commits start)

            let first_commit_sequence_number = *commits.first_key_value().unwrap().0;

            if let Some(write) = commits.range(concurrent_commit_range).rev().find_map(|(_, writes)| {
                writes.operations.writes_in(write_key.keyspace_id()).get_write(write_key.bytes())
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
                if reinsert.load(std::sync::atomic::Ordering::Relaxed) {
                    Ok(1)
                } else {
                    Ok(0)
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
    }
);

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
enum SerialisableType {
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

    use serde::{
        de,
        de::{MapAccess, SeqAccess, Visitor},
        ser::SerializeStruct,
        Deserialize, Deserializer, Serialize, Serializer,
    };

    use crate::{
        thing::statistics::{SerialisableType, Statistics},
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

    fn to_serialisable_map_map<Type1: Into<SerialisableType> + Clone, Type2: Into<SerialisableType> + Clone>(
        map: &HashMap<Type1, HashMap<Type2, u64>>,
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, u64>> {
        map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map(value))).collect()
    }

    fn to_serialisable_map_map_map<
        Type1: Into<SerialisableType> + Clone,
        Type2: Into<SerialisableType> + Clone,
        Type3: Into<SerialisableType> + Clone,
    >(
        map: &HashMap<Type1, HashMap<Type2, HashMap<Type3, u64>>>,
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, HashMap<SerialisableType, u64>>> {
        map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map_map(value))).collect()
    }

    fn to_serialisable_map<Type_: Into<SerialisableType> + Clone>(
        map: &HashMap<Type_, u64>,
    ) -> HashMap<SerialisableType, u64> {
        map.iter().map(|(type_, value)| (type_.clone().into(), *value)).collect()
    }

    fn into_entity_map(map: HashMap<SerialisableType, u64>) -> HashMap<EntityType, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_entity_type(), value)).collect()
    }

    fn into_relation_map(map: HashMap<SerialisableType, u64>) -> HashMap<RelationType, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_relation_type(), value)).collect()
    }

    fn into_attribute_map(map: HashMap<SerialisableType, u64>) -> HashMap<AttributeType, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_attribute_type(), value)).collect()
    }

    fn into_role_map(map: HashMap<SerialisableType, u64>) -> HashMap<RoleType, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_role_type(), value)).collect()
    }

    fn into_object_map(map: HashMap<SerialisableType, u64>) -> HashMap<ObjectType, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_object_type(), value)).collect()
    }

    impl<'de> Deserialize<'de> for Statistics {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
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
