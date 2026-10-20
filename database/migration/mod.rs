/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    fmt::Formatter,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use error::TypeDBError;

pub mod database_exporter;
pub mod database_importer;
pub mod importing_database_handle;
pub mod item;

pub type MigrationOperationError = Arc<dyn TypeDBError + Send + Sync>;

// Counters are independent tallies, so Relaxed ordering is sufficient; readers that compare
// totals must synchronise with the writers (e.g. by joining them) before loading
#[derive(Debug)]
pub struct Checksums {
    pub entity_count: AtomicI64,
    pub attribute_count: AtomicI64,
    pub relation_count: AtomicI64,
    pub role_count: AtomicI64,
    pub ownership_count: AtomicI64,
}

impl Checksums {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_counts(
        entity_count: i64,
        attribute_count: i64,
        relation_count: i64,
        role_count: i64,
        ownership_count: i64,
    ) -> Self {
        Self {
            entity_count: AtomicI64::new(entity_count),
            attribute_count: AtomicI64::new(attribute_count),
            relation_count: AtomicI64::new(relation_count),
            role_count: AtomicI64::new(role_count),
            ownership_count: AtomicI64::new(ownership_count),
        }
    }

    pub fn entity_count(&self) -> i64 {
        self.entity_count.load(Ordering::Relaxed)
    }

    pub fn attribute_count(&self) -> i64 {
        self.attribute_count.load(Ordering::Relaxed)
    }

    pub fn relation_count(&self) -> i64 {
        self.relation_count.load(Ordering::Relaxed)
    }

    pub fn role_count(&self) -> i64 {
        self.role_count.load(Ordering::Relaxed)
    }

    pub fn ownership_count(&self) -> i64 {
        self.ownership_count.load(Ordering::Relaxed)
    }

    pub fn increment_entity_count(&self) {
        self.entity_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_attribute_count(&self) {
        self.attribute_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_relation_count(&self) {
        self.relation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_role_count(&self) {
        self.role_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_ownership_count(&self) {
        self.ownership_count.fetch_add(1, Ordering::Relaxed);
    }

    fn counts(&self) -> (i64, i64, i64, i64, i64) {
        (self.entity_count(), self.attribute_count(), self.relation_count(), self.role_count(), self.ownership_count())
    }
}

impl Default for Checksums {
    fn default() -> Self {
        Self::from_counts(0, 0, 0, 0, 0)
    }
}

impl Clone for Checksums {
    fn clone(&self) -> Self {
        let (entity_count, attribute_count, relation_count, role_count, ownership_count) = self.counts();
        Self::from_counts(entity_count, attribute_count, relation_count, role_count, ownership_count)
    }
}

impl PartialEq for Checksums {
    fn eq(&self, other: &Self) -> bool {
        self.counts() == other.counts()
    }
}

impl Eq for Checksums {}

impl fmt::Display for Checksums {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (entity_count, attribute_count, relation_count, role_count, ownership_count) = self.counts();
        write!(
            f,
            "[{entity_count} entities, {attribute_count} attributes, {relation_count} relations, {role_count} roles, {ownership_count} ownerships]"
        )
    }
}
