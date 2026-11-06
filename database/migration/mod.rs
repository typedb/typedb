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

#[derive(Debug)]
pub struct Checksums {
    entity_count: AtomicI64,
    attribute_count: AtomicI64,
    relation_count: AtomicI64,
    role_count: AtomicI64,
    ownership_count: AtomicI64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Counts {
    pub entity_count: i64,
    pub attribute_count: i64,
    pub relation_count: i64,
    pub role_count: i64,
    pub ownership_count: i64,
}

impl Checksums {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_counts(counts: Counts) -> Self {
        let Counts { entity_count, attribute_count, relation_count, role_count, ownership_count } = counts;
        Self {
            entity_count: AtomicI64::new(entity_count),
            attribute_count: AtomicI64::new(attribute_count),
            relation_count: AtomicI64::new(relation_count),
            role_count: AtomicI64::new(role_count),
            ownership_count: AtomicI64::new(ownership_count),
        }
    }

    pub fn counts(&self) -> Counts {
        Counts {
            entity_count: self.entity_count.load(Ordering::Relaxed),
            attribute_count: self.attribute_count.load(Ordering::Relaxed),
            relation_count: self.relation_count.load(Ordering::Relaxed),
            role_count: self.role_count.load(Ordering::Relaxed),
            ownership_count: self.ownership_count.load(Ordering::Relaxed),
        }
    }

    pub fn add(&self, counts: Counts) {
        let Counts { entity_count, attribute_count, relation_count, role_count, ownership_count } = counts;
        self.entity_count.fetch_add(entity_count, Ordering::Relaxed);
        self.attribute_count.fetch_add(attribute_count, Ordering::Relaxed);
        self.relation_count.fetch_add(relation_count, Ordering::Relaxed);
        self.role_count.fetch_add(role_count, Ordering::Relaxed);
        self.ownership_count.fetch_add(ownership_count, Ordering::Relaxed);
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
}

impl Default for Checksums {
    fn default() -> Self {
        Self::from_counts(Counts::default())
    }
}

impl Clone for Checksums {
    fn clone(&self) -> Self {
        Self::from_counts(self.counts())
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
        let Counts { entity_count, attribute_count, relation_count, role_count, ownership_count } = self.counts();
        write!(
            f,
            "[{entity_count} entities, {attribute_count} attributes, {relation_count} relations, {role_count} roles, {ownership_count} ownerships]"
        )
    }
}
