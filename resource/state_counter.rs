/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use serde::{Deserialize, Serialize};


/// State counters: monotonically-advancing counters that are part of TypeDB's
/// persistent state and must stay consistent across server restarts.
pub trait StateCounter: Debug + Send + Sync {
    fn id(&self) -> CounterId;

    /// Largest value that has been allocated so far.
    fn high_watermark(&self) -> u64;

    /// Advance the counter to at least `value`. **Monotonic**: if the counter
    /// is already past `value`, this is a no-op.
    fn advance_to(&self, value: u64);
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum CounterId {
    EntityVertex { type_id: u16 },
    RelationVertex { type_id: u16 },
}

// Keep CounterId small: it's stored in every CommitRecord.
// If you intentionally exceed this, adjust the bound.
const COUNTER_ID_MAX_SIZE: usize = 8;
const _: () = assert!(size_of::<CounterId>() <= COUNTER_ID_MAX_SIZE);

#[derive(Debug)]
pub struct AtomicStateCounter {
    id: CounterId,
    value: AtomicU64,
}

impl AtomicStateCounter {
    pub fn new(id: CounterId, initial: u64) -> Self {
        Self { id, value: AtomicU64::new(initial) }
    }

    /// Allocate the next value and return the previous counter state.
    pub fn allocate(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }
}

impl StateCounter for AtomicStateCounter {
    fn id(&self) -> CounterId {
        self.id
    }

    fn high_watermark(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    fn advance_to(&self, value: u64) {
        // fetch_max gives us the monotonic guarantee for free: the counter
        // never moves backwards regardless of how many threads race.
        self.value.fetch_max(value, Ordering::Relaxed);
    }
}

/// Registry of every [`StateCounter`] scoped to a single database.
#[derive(Debug)]
pub struct DatabaseStateCounterRegistry {
    database_name: String,
    counters: RwLock<HashMap<CounterId, Arc<dyn StateCounter>>>,
}

impl DatabaseStateCounterRegistry {
    pub fn new(database_name: impl Into<String>) -> Self {
        Self { database_name: database_name.into(), counters: RwLock::new(HashMap::new()) }
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Register a counter. If one with the same id already exists, it is **replaced**.
    pub fn register(&self, counter: Arc<dyn StateCounter>) {
        let id = counter.id();
        let mut counters = self.counters.write().expect("state-counter registry lock poisoned");
        assert!(counters.get(&id).is_none(), "Unexpected double counter registration");
        counters.insert(id, counter);
    }

    /// Apply a batch of post-commit watermarks to the registered counters.
    pub fn advance_from_commit(&self, counter_advances: &[(CounterId, u64)]) {
        let counters = self.counters.read().expect("state-counter registry lock poisoned");
        for (id, value) in counter_advances {
            match counters.get(id) {
                Some(counter) => counter.advance_to(*value),
                None => {
                    // Forward-compatibility: a server may be one version behind
                    // the primary server and not know this counter id yet.
                    tracing::warn!(
                        target: "state_counter",
                        ?id,
                        database = %self.database_name,
                        "advance_from_commit: unknown CounterId; skipping"
                    );
                }
            }
        }
    }

    /// Read the current high-watermark for `id`, if registered.
    pub fn high_watermark(&self, id: CounterId) -> Option<u64> {
        self.counters.read().expect("state-counter registry lock poisoned").get(&id).map(|c| c.high_watermark())
    }

    /// Number of registered counters.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.counters.read().expect("state-counter registry lock poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{AtomicStateCounter, CounterId, DatabaseStateCounterRegistry, StateCounter, COUNTER_ID_MAX_SIZE};

    #[test]
    fn bounded_size() {
        // CounterId rides inside every commit's counter_advances vector.
        // Keep it tight; if this fails, audit the new variant for its
        // RAM-per-entry impact before bumping the bound.
        assert!(size_of::<CounterId>() <= COUNTER_ID_MAX_SIZE);
    }

    #[test]
    fn allocate_returns_pre_increment_value() {
        let c = AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 0);
        assert_eq!(c.allocate(), 0);
        assert_eq!(c.allocate(), 1);
        assert_eq!(c.allocate(), 2);
        assert_eq!(c.high_watermark(), 3);
    }

    #[test]
    fn advance_to_is_monotonic() {
        let c = AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 0);
        c.advance_to(10);
        assert_eq!(c.high_watermark(), 10);
        // Going backwards is a no-op, not a rollback.
        c.advance_to(5);
        assert_eq!(c.high_watermark(), 10);
        // Equal is also fine.
        c.advance_to(10);
        assert_eq!(c.high_watermark(), 10);
        // Forward works as expected.
        c.advance_to(42);
        assert_eq!(c.high_watermark(), 42);
    }

    #[test]
    fn registry_register_and_advance() {
        let registry = DatabaseStateCounterRegistry::new("db_a");
        let c1: Arc<dyn StateCounter> =
            Arc::new(AtomicStateCounter::new(CounterId::EntityVertex { type_id: 7 }, 0));
        let c2: Arc<dyn StateCounter> =
            Arc::new(AtomicStateCounter::new(CounterId::RelationVertex { type_id: 7 }, 0));
        registry.register(c1.clone());
        registry.register(c2.clone());
        assert_eq!(registry.len(), 2);

        registry.advance_from_commit(
            &[(CounterId::EntityVertex { type_id: 7 }, 100), (CounterId::RelationVertex { type_id: 7 }, 250)],
        );

        assert_eq!(registry.high_watermark(CounterId::EntityVertex { type_id: 7 }), Some(100));
        assert_eq!(registry.high_watermark(CounterId::RelationVertex { type_id: 7 }), Some(250));
    }

    #[test]
    fn registry_advance_respects_monotonicity() {
        let registry = DatabaseStateCounterRegistry::new("db");
        registry.register(Arc::new(AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 500)));
        // A stale commit (older value) must not roll the counter back.
        registry.advance_from_commit(&[(CounterId::EntityVertex { type_id: 1 }, 100)]);
        assert_eq!(registry.high_watermark(CounterId::EntityVertex { type_id: 1 }), Some(500));
    }

    #[test]
    fn unknown_counter_id_is_logged_and_skipped() {
        let registry = DatabaseStateCounterRegistry::new("db");
        registry.register(Arc::new(AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 0)));
        // Forward-compat: the writer knew about a counter this node doesn't.
        // Must not panic, must not affect the known counter.
        registry.advance_from_commit(
            &[
                (CounterId::EntityVertex { type_id: 1 }, 42),
                (CounterId::RelationVertex { type_id: 999 }, 17), // not registered
            ],
        );
        assert_eq!(registry.high_watermark(CounterId::EntityVertex { type_id: 1 }), Some(42));
        assert_eq!(registry.high_watermark(CounterId::RelationVertex { type_id: 999 }), None);
    }

    #[test]
    #[should_panic(expected = "crossed database boundary")]
    fn cross_database_advance_panics() {
        // If routing ever misroutes a commit, we want a loud failure rather
        // than silent corruption: counter advances govern IID allocation.
        let registry = DatabaseStateCounterRegistry::new("db_a");
        registry.register(Arc::new(AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 0)));
        registry.advance_from_commit(&[(CounterId::EntityVertex { type_id: 1 }, 1)]);
    }

    #[test]
    fn counter_id_roundtrips_through_bincode() {
        // CommitRecord serialises with bincode; CounterId must round-trip.
        let ids = vec![
            CounterId::EntityVertex { type_id: 0 },
            CounterId::EntityVertex { type_id: u16::MAX },
            CounterId::RelationVertex { type_id: 12345 },
        ];
        for id in ids {
            let bytes = bincode::serialize(&id).unwrap();
            let back: CounterId = bincode::deserialize(&bytes).unwrap();
            assert_eq!(id, back);
        }
    }

    #[test]
    fn empty_advance_list_is_noop() {
        let registry = DatabaseStateCounterRegistry::new("db");
        registry.register(Arc::new(AtomicStateCounter::new(CounterId::EntityVertex { type_id: 1 }, 7)));
        registry.advance_from_commit(&[]);
        assert_eq!(registry.high_watermark(CounterId::EntityVertex { type_id: 1 }), Some(7));
    }
}
