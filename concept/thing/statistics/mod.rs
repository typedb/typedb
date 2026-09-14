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
    snapshot::{buffer::OperationsBuffer, write::Write},
};
use tracing::{Level, event};

use crate::{
    thing::{ThingAPI, attribute::Attribute, entity::Entity, object::Object, relation::Relation},
    type_::{
        TypeAPI, attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
        relation_type::RelationType, role_type::RoleType,
    },
};

mod serialise;

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

    pub fn may_synchronise(&mut self, storage: &MVCCStorage<impl DurabilityClient>) -> Result<(), StatisticsError> {
        use StatisticsError::{DataRead, ReloadCommitData};

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
                                    self.durably_write(storage.durability())?;
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

        // checkpoint statistics on a huge change/a few large commits, or worst case on K commits
        // TODO: ideally, we'd want to check if the total number of changes in absolute terms is large or K commits
        let count_change_since_last_durable_write =
            self.total_count as i64 - self.last_durable_write_total_count as i64;
        let sequence_numbers_since_last_durable_write = self.sequence_number - self.last_durable_write_sequence_number;
        if count_change_since_last_durable_write.abs() > STATISTICS_DURABLE_WRITE_CHANGE_COUNT as i64
            || sequence_numbers_since_last_durable_write > STATISTICS_DURABLE_WRITE_SEQ_NUMBERS
        {
            self.durably_write(storage.durability())?;
        }

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
                    if matches!(write, Write::Delete) {
                        let type_ = EntityType::new(entity_type_vertex);
                        deferred_type_cleanups.push(Box::new(move |this: &mut Self| {
                            this.entity_counts.remove(&type_);
                            this.clear_object_type(ObjectType::Entity(type_));
                        }));
                    }
                    // note: don't update total count based on type updates
                }
                Some(DecodableKey::VertexRelationType(relation_type_vertex)) => {
                    if matches!(write, Write::Delete) {
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
                    if matches!(write, Write::Delete) {
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
                    if matches!(write, Write::Delete) {
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
        let attribute_count =
            self.has_attribute_counts.entry(owner_type).or_default().entry(attribute_type).or_default();
        Self::saturating_add(attribute_count, delta, "has_attribute");
        let owner_count = self.attribute_owner_counts.entry(attribute_type).or_default().entry(owner_type).or_default();
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
        let role_player_count = self.role_player_counts.entry(player_type).or_default().entry(role_type).or_default();
        Self::saturating_add(role_player_count, delta, "role_player");
        let relation_role_count =
            self.relation_role_counts.entry(relation_type).or_default().entry(role_type).or_default();
        Self::saturating_add(relation_role_count, delta, "relation_role");
        let relation_role_player_count = self
            .relation_role_player_counts
            .entry(relation_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(player_type)
            .or_default();
        Self::saturating_add(relation_role_player_count, delta, "relation_role_player");
        let player_role_relation_count = self
            .player_role_relation_counts
            .entry(player_type)
            .or_default()
            .entry(role_type)
            .or_default()
            .entry(relation_type)
            .or_default();
        Self::saturating_add(player_role_relation_count, delta, "player_role_relation");
    }

    fn update_indexed_player(&mut self, player_1_type: ObjectType, player_2_type: ObjectType, delta: i64) {
        let player_1_to_2_index_count =
            self.links_index_counts.entry(player_1_type).or_default().entry(player_2_type).or_default();
        Self::saturating_add(player_1_to_2_index_count, delta, "player_1_to_2_index");
        if player_1_type != player_2_type {
            let player_2_to_1_index_count =
                self.links_index_counts.entry(player_2_type).or_default().entry(player_1_type).or_default();
            Self::saturating_add(player_2_to_1_index_count, delta, "player_2_to_1_index");
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
                    writes.operations.writes_in(write_key.keyspace_id()).writes_get(write_key.bytes()),
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
                if reinsert.load(std::sync::atomic::Ordering::Relaxed) { Ok(1) } else { Ok(0) }
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
