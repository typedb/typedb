/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    iter,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::constants::encoding::TypeIDUInt;

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum CounterId {
    EntityVertex { type_id: TypeIDUInt },
    RelationVertex { type_id: TypeIDUInt },
    SnapshotId,
}

const COUNTER_ID_MAX_SIZE: usize = 8;
const _: () = assert!(size_of::<CounterId>() <= COUNTER_ID_MAX_SIZE);

pub struct StateCounters {
    entity_ids: PerTypeIdCounters,
    relation_ids: PerTypeIdCounters,
    snapshot_id: GlobalCounter,
}

impl std::fmt::Debug for StateCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateCounters").finish_non_exhaustive()
    }
}

impl Default for StateCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl StateCounters {
    pub fn new() -> Self {
        Self {
            entity_ids: PerTypeIdCounters::new(),
            relation_ids: PerTypeIdCounters::new(),
            snapshot_id: GlobalCounter::new(SnapshotIdCounter::INITIAL),
        }
    }

    pub fn allocate(&self, id: CounterId) -> u64 {
        match id {
            CounterId::EntityVertex { type_id } => self.entity_ids.allocate(type_id),
            CounterId::RelationVertex { type_id } => self.relation_ids.allocate(type_id),
            CounterId::SnapshotId => self.snapshot_id.allocate(),
        }
    }

    pub fn next_value(&self, id: CounterId) -> u64 {
        match id {
            CounterId::EntityVertex { type_id } => self.entity_ids.next_value(type_id),
            CounterId::RelationVertex { type_id } => self.relation_ids.next_value(type_id),
            CounterId::SnapshotId => self.snapshot_id.next_value(),
        }
    }

    pub fn update_to_at_least(&self, id: CounterId, value: u64) {
        match id {
            CounterId::EntityVertex { type_id } => self.entity_ids.update_to_at_least(type_id, value),
            CounterId::RelationVertex { type_id } => self.relation_ids.update_to_at_least(type_id, value),
            CounterId::SnapshotId => self.snapshot_id.update_to_at_least(value),
        }
    }

    pub fn apply_advances(&self, advances: &[(CounterId, u64)]) {
        for (id, value) in advances {
            self.update_to_at_least(*id, *value);
        }
    }

    pub fn reset(&self) {
        self.entity_ids.reset();
        self.relation_ids.reset();
        self.snapshot_id.reset();
    }
}

/// One [`AtomicU64`] per [`TypeIDUInt`].
struct PerTypeIdCounters {
    atomics: Box<[AtomicU64]>,
}

impl PerTypeIdCounters {
    const INITIAL: u64 = 0;
    const COUNT: usize = TypeIDUInt::MAX as usize + 1;

    fn new() -> Self {
        Self {
            atomics: iter::repeat_with(|| AtomicU64::new(Self::INITIAL)).take(Self::COUNT).collect(),
        }
    }

    fn allocate(&self, key: TypeIDUInt) -> u64 {
        self.atomics[key as usize].fetch_add(1, Ordering::Relaxed)
    }

    fn next_value(&self, key: TypeIDUInt) -> u64 {
        self.atomics[key as usize].load(Ordering::Relaxed)
    }

    fn update_to_at_least(&self, key: TypeIDUInt, value: u64) {
        self.atomics[key as usize].fetch_max(value, Ordering::Relaxed);
    }

    fn reset(&self) {
        for atomic in self.atomics.iter() {
            atomic.store(Self::INITIAL, Ordering::SeqCst);
        }
    }
}

struct GlobalCounter {
    atomic: AtomicU64,
    initial: u64,
}

impl GlobalCounter {
    fn new(initial: u64) -> Self {
        Self { atomic: AtomicU64::new(initial), initial }
    }

    fn allocate(&self) -> u64 {
        self.atomic.fetch_add(1, Ordering::Relaxed)
    }

    fn next_value(&self) -> u64 {
        self.atomic.load(Ordering::Relaxed)
    }

    fn update_to_at_least(&self, value: u64) {
        self.atomic.fetch_max(value, Ordering::Relaxed);
    }

    fn reset(&self) {
        self.atomic.store(self.initial, Ordering::SeqCst);
    }
}

struct SnapshotIdCounter;
impl SnapshotIdCounter {
    const INITIAL: u64 = 1;
}

#[cfg(test)]
mod tests {
    use super::{CounterId, StateCounters, COUNTER_ID_MAX_SIZE};

    #[test]
    fn bounded_id_size() {
        assert!(size_of::<CounterId>() <= COUNTER_ID_MAX_SIZE);
    }

    #[test]
    fn counter_id_roundtrips_through_bincode() {
        for id in [
            CounterId::EntityVertex { type_id: 0 },
            CounterId::EntityVertex { type_id: u16::MAX },
            CounterId::RelationVertex { type_id: 12345 },
            CounterId::SnapshotId,
        ] {
            let bytes = bincode::serialize(&id).unwrap();
            assert_eq!(id, bincode::deserialize::<CounterId>(&bytes).unwrap());
        }
    }

    #[test]
    fn allocate_returns_pre_increment_value() {
        let counters = StateCounters::new();
        let id = CounterId::EntityVertex { type_id: 1 };
        assert_eq!(counters.allocate(id), 0);
        assert_eq!(counters.allocate(id), 1);
        assert_eq!(counters.allocate(id), 2);
        assert_eq!(counters.next_value(id), 3);
    }

    #[test]
    fn entity_and_relation_counters_are_independent() {
        let counters = StateCounters::new();
        let entity_3 = CounterId::EntityVertex { type_id: 3 };
        let relation_3 = CounterId::RelationVertex { type_id: 3 };
        assert_eq!(counters.allocate(entity_3), 0);
        assert_eq!(counters.allocate(entity_3), 1);
        assert_eq!(counters.next_value(relation_3), 0);
        assert_eq!(counters.allocate(relation_3), 0);
        assert_eq!(counters.next_value(entity_3), 2);
    }

    #[test]
    fn update_to_at_least_is_monotonic() {
        let counters = StateCounters::new();
        let id = CounterId::EntityVertex { type_id: 7 };
        counters.update_to_at_least(id, 100);
        assert_eq!(counters.next_value(id), 100);
        counters.update_to_at_least(id, 50);
        assert_eq!(counters.next_value(id), 100);
        counters.update_to_at_least(id, 200);
        assert_eq!(counters.next_value(id), 200);
    }

    #[test]
    fn apply_advances_batch() {
        let counters = StateCounters::new();
        counters.apply_advances(&[
            (CounterId::EntityVertex { type_id: 1 }, 50),
            (CounterId::RelationVertex { type_id: 2 }, 99),
            (CounterId::EntityVertex { type_id: 1 }, 25),
        ]);
        assert_eq!(counters.next_value(CounterId::EntityVertex { type_id: 1 }), 50);
        assert_eq!(counters.next_value(CounterId::RelationVertex { type_id: 2 }), 99);
    }

    #[test]
    fn reset_restores_initial_values() {
        let counters = StateCounters::new();
        let entity = CounterId::EntityVertex { type_id: 99 };
        counters.allocate(entity);
        counters.allocate(entity);
        counters.allocate(CounterId::SnapshotId);
        assert_eq!(counters.next_value(entity), 2);
        assert_eq!(counters.next_value(CounterId::SnapshotId), 2);
        counters.reset();
        assert_eq!(counters.next_value(entity), 0);
        assert_eq!(counters.next_value(CounterId::SnapshotId), 1);
    }

    #[test]
    fn snapshot_id_counter_skips_unset_sentinel() {
        let counters = StateCounters::new();
        assert_eq!(counters.allocate(CounterId::SnapshotId), 1);
        assert_eq!(counters.allocate(CounterId::SnapshotId), 2);
        counters.reset();
        assert_eq!(counters.allocate(CounterId::SnapshotId), 1);
    }
}
