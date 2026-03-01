/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! In-memory projection catalog backed by [`ProjectionDefinition`]s.
//!
//! [`MaterializedCatalog`] implements the [`ProjectionCatalog`] trait from
//! `query_executor` and holds the column metadata + materialized row data
//! for each registered projection. It supports:
//!
//! - Registering/deregistering projections
//! - Atomic row updates via [`replace_rows`](MaterializedCatalog::replace_rows)
//!   using `Arc` swap: rows are wrapped in `Arc<Vec<…>>` so that replacing
//!   data is a pointer swap. Readers who cloned the old `Arc` still see
//!   consistent stale data until they drop their snapshot.
//! - Thread-safe concurrent reads via `Arc<RwLock<…>>`

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    definition::ProjectionDefinition,
    pgwire::query_executor::{CatalogColumn, ProjectionCatalog, ProjectionInfo},
    type_mapping::{pg_oid_type_size, value_type_to_pg_oid},
};

// ── Materialized projection ────────────────────────────────────────

/// A single projection's column schema plus current materialized rows.
///
/// Row data is wrapped in [`Arc`] so that [`replace_rows`](MaterializedCatalog::replace_rows)
/// is an atomic pointer swap. Concurrent readers who cloned the `Arc`
/// before the swap retain a consistent snapshot of the old data.
#[derive(Debug, Clone)]
struct MaterializedProjection {
    /// Column metadata derived from the definition.
    columns: Vec<CatalogColumn>,
    /// Current row data behind an `Arc` for atomic swap-on-write.
    rows: Arc<Vec<Vec<Option<String>>>>,
}

impl MaterializedProjection {
    fn from_definition(def: &ProjectionDefinition) -> Self {
        let columns = def
            .columns()
            .iter()
            .map(|col| {
                let oid = value_type_to_pg_oid(col.value_type);
                let size = pg_oid_type_size(oid).unwrap_or(-1);
                CatalogColumn { name: col.name.clone(), type_oid: oid, type_size: size }
            })
            .collect();
        Self { columns, rows: Arc::new(Vec::new()) }
    }
}

// ── Catalog implementation ─────────────────────────────────────────

/// Thread-safe in-memory catalog of materialized projections.
///
/// Projections are registered from [`ProjectionDefinition`]s and their
/// row data is populated/refreshed via [`replace_rows`](Self::replace_rows).
/// The catalog is cloneable (cheap `Arc` clone) for sharing across tasks.
#[derive(Debug, Clone)]
pub struct MaterializedCatalog {
    inner: Arc<RwLock<CatalogInner>>,
}

#[derive(Debug)]
struct CatalogInner {
    /// name (lowercased) → materialized projection
    projections: HashMap<String, MaterializedProjection>,
    /// name (lowercased) → original definition
    definitions: HashMap<String, ProjectionDefinition>,
}

impl MaterializedCatalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(CatalogInner { projections: HashMap::new(), definitions: HashMap::new() })) }
    }

    /// Register a projection definition. Replaces any existing projection
    /// with the same name (case-insensitive). Starts with zero rows.
    pub fn register(&self, definition: ProjectionDefinition) {
        let key = definition.name().to_ascii_lowercase();
        let materialized = MaterializedProjection::from_definition(&definition);
        let mut inner = self.inner.write().unwrap();
        inner.definitions.insert(key.clone(), definition);
        inner.projections.insert(key, materialized);
    }

    /// Remove a projection by name (case-insensitive).
    /// Returns `true` if it existed.
    pub fn deregister(&self, name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        let mut inner = self.inner.write().unwrap();
        inner.definitions.remove(&key);
        inner.projections.remove(&key).is_some()
    }

    /// Replace the materialized rows for a projection (atomic `Arc` swap).
    ///
    /// Creates a new `Arc<Vec<…>>` and swaps the pointer. Readers who
    /// previously cloned the old `Arc` retain a consistent snapshot.
    ///
    /// Returns `false` if the projection is not registered.
    pub fn replace_rows(&self, name: &str, rows: Vec<Vec<Option<String>>>) -> bool {
        let key = name.to_ascii_lowercase();
        let mut inner = self.inner.write().unwrap();
        if let Some(proj) = inner.projections.get_mut(&key) {
            proj.rows = Arc::new(rows);
            true
        } else {
            false
        }
    }

    /// Get a cheap `Arc` snapshot of a projection's current row data.
    ///
    /// The returned `Arc` gives a consistent view of the rows at the time
    /// of the call. Even if [`replace_rows`](Self::replace_rows) is called
    /// afterwards, the snapshot remains valid.
    pub fn get_row_snapshot(&self, name: &str) -> Option<Arc<Vec<Vec<Option<String>>>>> {
        let key = name.to_ascii_lowercase();
        let inner = self.inner.read().unwrap();
        inner.projections.get(&key).map(|proj| Arc::clone(&proj.rows))
    }

    /// Get the definition for a projection (case-insensitive).
    pub fn get_definition(&self, name: &str) -> Option<ProjectionDefinition> {
        let key = name.to_ascii_lowercase();
        let inner = self.inner.read().unwrap();
        inner.definitions.get(&key).cloned()
    }

    /// Number of registered projections.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().projections.len()
    }

    /// Whether the catalog has no projections.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MaterializedCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionCatalog for MaterializedCatalog {
    fn list_projections(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut names: Vec<String> = inner.projections.keys().cloned().collect();
        names.sort();
        names
    }

    fn get_projection(&self, name: &str) -> Option<ProjectionInfo> {
        let key = name.to_ascii_lowercase();
        // Grab Arc snapshots under the lock, then release before cloning data.
        let (columns, rows_arc) = {
            let inner = self.inner.read().unwrap();
            match inner.projections.get(&key) {
                Some(proj) => (proj.columns.clone(), Arc::clone(&proj.rows)),
                None => return None,
            }
        }; // read lock released here
        Some(ProjectionInfo { name: key, columns, rows: rows_arc.as_ref().clone() })
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use encoding::value::value_type::ValueTypeCategory;

    use super::*;
    use crate::{
        definition::ColumnDefinition,
        type_mapping::{PG_OID_BOOL, PG_OID_INT8, PG_OID_TEXT},
    };

    fn people_definition() -> ProjectionDefinition {
        ProjectionDefinition::new(
            "People",
            vec![
                ColumnDefinition::new("id", ValueTypeCategory::Integer),
                ColumnDefinition::new("name", ValueTypeCategory::String),
                ColumnDefinition::new("active", ValueTypeCategory::Boolean),
            ],
            "match $p isa person; fetch { $p.id; $p.name; $p.active; };",
        )
        .unwrap()
    }

    fn sample_rows() -> Vec<Vec<Option<String>>> {
        vec![
            vec![Some("1".into()), Some("Alice".into()), Some("t".into())],
            vec![Some("2".into()), Some("Bob".into()), Some("f".into())],
        ]
    }

    // ── New catalog ──────────────────────────────────────────────

    #[test]
    fn new_catalog_is_empty() {
        let catalog = MaterializedCatalog::new();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);
    }

    #[test]
    fn default_catalog_is_empty() {
        let catalog = MaterializedCatalog::default();
        assert!(catalog.is_empty());
    }

    // ── Register ─────────────────────────────────────────────────

    #[test]
    fn register_adds_projection() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        assert_eq!(catalog.len(), 1);
        assert!(!catalog.is_empty());
    }

    #[test]
    fn register_is_case_insensitive() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition()); // name = "People"
        assert!(catalog.get_projection("people").is_some());
        assert!(catalog.get_projection("PEOPLE").is_some());
        assert!(catalog.get_projection("People").is_some());
    }

    #[test]
    fn register_replaces_existing() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        catalog.replace_rows("people", sample_rows());

        // Re-register with different columns (rows should reset to empty).
        let new_def = ProjectionDefinition::new(
            "People",
            vec![ColumnDefinition::new("id", ValueTypeCategory::Integer)],
            "match $p isa person; fetch { $p.id; };",
        )
        .unwrap();
        catalog.register(new_def);

        let info = catalog.get_projection("people").unwrap();
        assert_eq!(info.columns.len(), 1);
        assert!(info.rows.is_empty(), "Rows should be cleared after re-register");
    }

    // ── Deregister ───────────────────────────────────────────────

    #[test]
    fn deregister_removes_projection() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        assert!(catalog.deregister("people"));
        assert!(catalog.is_empty());
        assert!(catalog.get_projection("people").is_none());
    }

    #[test]
    fn deregister_nonexistent_returns_false() {
        let catalog = MaterializedCatalog::new();
        assert!(!catalog.deregister("nonexistent"));
    }

    #[test]
    fn deregister_is_case_insensitive() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        assert!(catalog.deregister("PEOPLE"));
        assert!(catalog.is_empty());
    }

    // ── Rows ─────────────────────────────────────────────────────

    #[test]
    fn registered_projection_starts_with_no_rows() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        let info = catalog.get_projection("people").unwrap();
        assert!(info.rows.is_empty());
    }

    #[test]
    fn replace_rows_populates_data() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        assert!(catalog.replace_rows("people", sample_rows()));

        let info = catalog.get_projection("people").unwrap();
        assert_eq!(info.rows.len(), 2);
        assert_eq!(info.rows[0][1], Some("Alice".into()));
    }

    #[test]
    fn replace_rows_is_atomic_swap() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        catalog.replace_rows("people", sample_rows());

        // Replace with new data.
        let new_rows = vec![vec![Some("3".into()), Some("Carol".into()), Some("t".into())]];
        catalog.replace_rows("people", new_rows);

        let info = catalog.get_projection("people").unwrap();
        assert_eq!(info.rows.len(), 1);
        assert_eq!(info.rows[0][1], Some("Carol".into()));
    }

    #[test]
    fn replace_rows_unregistered_returns_false() {
        let catalog = MaterializedCatalog::new();
        assert!(!catalog.replace_rows("nonexistent", vec![]));
    }

    #[test]
    fn replace_rows_is_case_insensitive() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        assert!(catalog.replace_rows("PEOPLE", sample_rows()));
        let info = catalog.get_projection("people").unwrap();
        assert_eq!(info.rows.len(), 2);
    }

    // ── Column metadata ──────────────────────────────────────────

    #[test]
    fn columns_have_correct_oids() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        let info = catalog.get_projection("people").unwrap();

        assert_eq!(info.columns[0].name, "id");
        assert_eq!(info.columns[0].type_oid, PG_OID_INT8);
        assert_eq!(info.columns[1].name, "name");
        assert_eq!(info.columns[1].type_oid, PG_OID_TEXT);
        assert_eq!(info.columns[2].name, "active");
        assert_eq!(info.columns[2].type_oid, PG_OID_BOOL);
    }

    #[test]
    fn columns_have_correct_sizes() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        let info = catalog.get_projection("people").unwrap();

        assert_eq!(info.columns[0].type_size, 8); // int8
        assert_eq!(info.columns[1].type_size, -1); // text (variable)
        assert_eq!(info.columns[2].type_size, 1); // bool
    }

    // ── list_projections ─────────────────────────────────────────

    #[test]
    fn list_projections_empty() {
        let catalog = MaterializedCatalog::new();
        assert!(catalog.list_projections().is_empty());
    }

    #[test]
    fn list_projections_returns_sorted_names() {
        let catalog = MaterializedCatalog::new();
        let def_b = ProjectionDefinition::new(
            "beta",
            vec![ColumnDefinition::new("x", ValueTypeCategory::Integer)],
            "match $b;",
        )
        .unwrap();
        let def_a = ProjectionDefinition::new(
            "alpha",
            vec![ColumnDefinition::new("x", ValueTypeCategory::Integer)],
            "match $a;",
        )
        .unwrap();
        catalog.register(def_b);
        catalog.register(def_a);

        assert_eq!(catalog.list_projections(), vec!["alpha", "beta"]);
    }

    // ── get_projection ───────────────────────────────────────────

    #[test]
    fn get_projection_returns_none_for_missing() {
        let catalog = MaterializedCatalog::new();
        assert!(catalog.get_projection("nosuch").is_none());
    }

    #[test]
    fn get_projection_name_is_lowercased() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        let info = catalog.get_projection("People").unwrap();
        assert_eq!(info.name, "people");
    }

    // ── get_definition ───────────────────────────────────────────

    #[test]
    fn get_definition_returns_original() {
        let catalog = MaterializedCatalog::new();
        let def = people_definition();
        catalog.register(def.clone());
        let retrieved = catalog.get_definition("people").unwrap();
        assert_eq!(retrieved, def);
    }

    #[test]
    fn get_definition_missing_returns_none() {
        let catalog = MaterializedCatalog::new();
        assert!(catalog.get_definition("missing").is_none());
    }

    // ── Clone (Arc-based) ────────────────────────────────────────

    #[test]
    fn clone_shares_state() {
        let catalog = MaterializedCatalog::new();
        let catalog2 = catalog.clone();
        catalog.register(people_definition());

        // Clone sees the registration.
        assert_eq!(catalog2.len(), 1);
        assert!(catalog2.get_projection("people").is_some());
    }

    // ── Thread safety ────────────────────────────────────────────

    #[test]
    fn catalog_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MaterializedCatalog>();
    }

    // ── Arc snapshot isolation ────────────────────────────────────

    #[test]
    fn get_row_snapshot_returns_arc() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        catalog.replace_rows("people", sample_rows());

        let snapshot = catalog.get_row_snapshot("people").unwrap();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0][1], Some("Alice".into()));
    }

    #[test]
    fn get_row_snapshot_missing_returns_none() {
        let catalog = MaterializedCatalog::new();
        assert!(catalog.get_row_snapshot("nope").is_none());
    }

    #[test]
    fn snapshot_survives_replace() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        catalog.replace_rows("people", sample_rows());

        // Take a snapshot before replacing.
        let snapshot_before = catalog.get_row_snapshot("people").unwrap();
        assert_eq!(snapshot_before.len(), 2);

        // Replace rows with new data.
        let new_rows = vec![vec![Some("3".into()), Some("Carol".into()), Some("t".into())]];
        catalog.replace_rows("people", new_rows);

        // Old snapshot still holds the old data (Arc isolation).
        assert_eq!(snapshot_before.len(), 2);
        assert_eq!(snapshot_before[0][1], Some("Alice".into()));

        // New read sees the updated data.
        let snapshot_after = catalog.get_row_snapshot("people").unwrap();
        assert_eq!(snapshot_after.len(), 1);
        assert_eq!(snapshot_after[0][1], Some("Carol".into()));
    }

    #[test]
    fn snapshot_is_case_insensitive() {
        let catalog = MaterializedCatalog::new();
        catalog.register(people_definition());
        catalog.replace_rows("people", sample_rows());
        assert!(catalog.get_row_snapshot("PEOPLE").is_some());
    }
}
