/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! In-memory store for projection definitions with define-time validation.
//!
//! [`ProjectionStore`] manages the lifecycle of projection definitions:
//! define (create), undefine (remove), get (lookup), and list (scan).
//!
//! **Define-time validation** enforces that projections produce flat
//! relational rows only — no [`Struct`](encoding::value::value_type::ValueTypeCategory::Struct)
//! columns are permitted. This ensures that every cell value maps
//! directly to a Postgres scalar type, which is required by the pgwire
//! layer for `RowDescription` + `DataRow` encoding.
//!
//! The store is the self-contained equivalent of TypeDB's `FunctionManager`
//! for projection definitions. Since the `typeql` crate is an external
//! dependency and its `Definable` enum cannot be extended, projections
//! bypass the standard `query/define.rs` path and are managed through
//! this dedicated store.

use std::collections::HashMap;

use encoding::value::value_type::ValueTypeCategory;

use crate::definition::ProjectionDefinition;

// ── Error type ─────────────────────────────────────────────────────

/// Errors from projection store operations.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionStoreError {
    /// A projection with this name already exists.
    AlreadyExists(String),
    /// No projection found with this name.
    NotFound(String),
    /// Column type not allowed for flat projections (e.g., `Struct`).
    NonFlatColumnType { column: String, type_name: String },
}

impl std::fmt::Display for ProjectionStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(name) => write!(f, "projection '{name}' already exists"),
            Self::NotFound(name) => write!(f, "projection '{name}' not found"),
            Self::NonFlatColumnType { column, type_name } => {
                write!(f, "column '{column}' has non-flat type '{type_name}' (only scalar types allowed)")
            }
        }
    }
}

impl std::error::Error for ProjectionStoreError {}

// ── Flat-row validation ────────────────────────────────────────────

/// Returns `true` if the value type is a flat (scalar) type suitable
/// for relational projection columns.
///
/// Currently rejects [`Struct`](ValueTypeCategory::Struct) since structs
/// represent nested/composite values that cannot be encoded as scalar
/// Postgres wire-format cells.
pub fn is_flat_type(vt: ValueTypeCategory) -> bool {
    !matches!(vt, ValueTypeCategory::Struct)
}

// ── Store implementation ───────────────────────────────────────────

/// In-memory store for projection definitions.
///
/// Enforces:
/// - **Unique names** (case-insensitive)
/// - **Flat row types** only — no `Struct` columns at define-time
///
/// Thread safety: wrap in `Arc<RwLock<…>>` for concurrent access.
#[derive(Debug, Clone)]
pub struct ProjectionStore {
    /// name (lowercased) → definition
    definitions: HashMap<String, ProjectionDefinition>,
}

impl ProjectionStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self { definitions: HashMap::new() }
    }

    /// Define a new projection.
    ///
    /// Returns an error if:
    /// - A projection with the same name (case-insensitive) already exists
    /// - Any column has a non-flat type (e.g., `Struct`)
    pub fn define(&mut self, definition: ProjectionDefinition) -> Result<(), ProjectionStoreError> {
        // Validate flat columns.
        for col in definition.columns() {
            if !is_flat_type(col.value_type) {
                return Err(ProjectionStoreError::NonFlatColumnType {
                    column: col.name.clone(),
                    type_name: format!("{:?}", col.value_type),
                });
            }
        }

        let key = definition.name().to_lowercase();
        if self.definitions.contains_key(&key) {
            return Err(ProjectionStoreError::AlreadyExists(definition.name().to_string()));
        }
        self.definitions.insert(key, definition);
        Ok(())
    }

    /// Remove a projection by name (case-insensitive).
    ///
    /// Returns the removed definition, or an error if not found.
    pub fn undefine(&mut self, name: &str) -> Result<ProjectionDefinition, ProjectionStoreError> {
        let key = name.to_lowercase();
        self.definitions.remove(&key).ok_or_else(|| ProjectionStoreError::NotFound(name.to_string()))
    }

    /// Look up a projection by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&ProjectionDefinition> {
        self.definitions.get(&name.to_lowercase())
    }

    /// Check if a projection exists by name (case-insensitive).
    pub fn contains(&self, name: &str) -> bool {
        self.definitions.contains_key(&name.to_lowercase())
    }

    /// List all stored projection definitions.
    ///
    /// Order is not guaranteed (HashMap iteration order).
    pub fn list(&self) -> Vec<&ProjectionDefinition> {
        self.definitions.values().collect()
    }

    /// Number of stored projections.
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }
}

impl Default for ProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::ColumnDefinition;

    fn def(name: &str, cols: &[(&str, ValueTypeCategory)]) -> ProjectionDefinition {
        let columns: Vec<_> = cols.iter().map(|(n, t)| ColumnDefinition::new(*n, *t)).collect();
        ProjectionDefinition::new(name, columns, "match $x isa thing;").unwrap()
    }

    fn simple_def(name: &str) -> ProjectionDefinition {
        def(name, &[("id", ValueTypeCategory::Integer), ("name", ValueTypeCategory::String)])
    }

    // ── Define ───────────────────────────────────────────────────

    #[test]
    fn define_and_retrieve() {
        let mut store = ProjectionStore::new();
        let d = simple_def("Employees");
        store.define(d.clone()).unwrap();
        let got = store.get("Employees").unwrap();
        assert_eq!(got, &d);
    }

    #[test]
    fn define_is_case_insensitive() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("Employees")).unwrap();
        assert!(store.get("employees").is_some());
        assert!(store.get("EMPLOYEES").is_some());
    }

    #[test]
    fn define_multiple_projections() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("A")).unwrap();
        store.define(simple_def("B")).unwrap();
        store.define(simple_def("C")).unwrap();
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn define_error_duplicate_name() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("X")).unwrap();
        let err = store.define(simple_def("X")).unwrap_err();
        assert_eq!(err, ProjectionStoreError::AlreadyExists("X".into()));
    }

    #[test]
    fn define_error_duplicate_case_insensitive() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("People")).unwrap();
        let err = store.define(simple_def("people")).unwrap_err();
        assert_eq!(err, ProjectionStoreError::AlreadyExists("people".into()));
    }

    // ── Flat-row validation ──────────────────────────────────────

    #[test]
    fn define_rejects_struct_column() {
        let mut store = ProjectionStore::new();
        let d = def("Bad", &[("data", ValueTypeCategory::Struct)]);
        let err = store.define(d).unwrap_err();
        match err {
            ProjectionStoreError::NonFlatColumnType { column, type_name } => {
                assert_eq!(column, "data");
                assert_eq!(type_name, "Struct");
            }
            _ => panic!("expected NonFlatColumnType, got {err:?}"),
        }
    }

    #[test]
    fn define_rejects_struct_among_scalars() {
        let mut store = ProjectionStore::new();
        let d = def(
            "Mixed",
            &[
                ("id", ValueTypeCategory::Integer),
                ("nested", ValueTypeCategory::Struct),
                ("name", ValueTypeCategory::String),
            ],
        );
        let err = store.define(d).unwrap_err();
        match err {
            ProjectionStoreError::NonFlatColumnType { column, .. } => {
                assert_eq!(column, "nested");
            }
            _ => panic!("expected NonFlatColumnType, got {err:?}"),
        }
    }

    #[test]
    fn define_accepts_all_scalar_types() {
        let mut store = ProjectionStore::new();
        let d = def(
            "AllScalar",
            &[
                ("a", ValueTypeCategory::Boolean),
                ("b", ValueTypeCategory::Integer),
                ("c", ValueTypeCategory::Double),
                ("d", ValueTypeCategory::Decimal),
                ("e", ValueTypeCategory::Date),
                ("f", ValueTypeCategory::DateTime),
                ("g", ValueTypeCategory::DateTimeTZ),
                ("h", ValueTypeCategory::Duration),
                ("i", ValueTypeCategory::String),
            ],
        );
        store.define(d).unwrap();
        assert_eq!(store.get("AllScalar").unwrap().column_count(), 9);
    }

    // ── Undefine ─────────────────────────────────────────────────

    #[test]
    fn undefine_removes_projection() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("X")).unwrap();
        let removed = store.undefine("X").unwrap();
        assert_eq!(removed.name(), "X");
        assert!(store.get("X").is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn undefine_is_case_insensitive() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("People")).unwrap();
        store.undefine("PEOPLE").unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn undefine_error_not_found() {
        let mut store = ProjectionStore::new();
        let err = store.undefine("Ghost").unwrap_err();
        assert_eq!(err, ProjectionStoreError::NotFound("Ghost".into()));
    }

    #[test]
    fn define_undefine_redefine() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("X")).unwrap();
        store.undefine("X").unwrap();
        // Re-define with same name should succeed.
        store.define(simple_def("X")).unwrap();
        assert!(store.contains("X"));
    }

    // ── List / contains / len ────────────────────────────────────

    #[test]
    fn initially_empty() {
        let store = ProjectionStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.list().is_empty());
    }

    #[test]
    fn list_returns_all_definitions() {
        let mut store = ProjectionStore::new();
        store.define(simple_def("A")).unwrap();
        store.define(simple_def("B")).unwrap();
        let mut names: Vec<_> = store.list().iter().map(|d| d.name().to_string()).collect();
        names.sort();
        assert_eq!(names, vec!["A", "B"]);
    }

    #[test]
    fn contains_returns_false_for_unknown() {
        let store = ProjectionStore::new();
        assert!(!store.contains("nope"));
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let store = ProjectionStore::new();
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn default_is_empty() {
        let store = ProjectionStore::default();
        assert!(store.is_empty());
    }

    // ── is_flat_type unit tests ──────────────────────────────────

    #[test]
    fn flat_type_accepts_scalars() {
        assert!(is_flat_type(ValueTypeCategory::Boolean));
        assert!(is_flat_type(ValueTypeCategory::Integer));
        assert!(is_flat_type(ValueTypeCategory::Double));
        assert!(is_flat_type(ValueTypeCategory::Decimal));
        assert!(is_flat_type(ValueTypeCategory::Date));
        assert!(is_flat_type(ValueTypeCategory::DateTime));
        assert!(is_flat_type(ValueTypeCategory::DateTimeTZ));
        assert!(is_flat_type(ValueTypeCategory::Duration));
        assert!(is_flat_type(ValueTypeCategory::String));
    }

    #[test]
    fn flat_type_rejects_struct() {
        assert!(!is_flat_type(ValueTypeCategory::Struct));
    }

    // ── Error Display ────────────────────────────────────────────

    #[test]
    fn error_display_already_exists() {
        let err = ProjectionStoreError::AlreadyExists("X".into());
        assert_eq!(err.to_string(), "projection 'X' already exists");
    }

    #[test]
    fn error_display_not_found() {
        let err = ProjectionStoreError::NotFound("Y".into());
        assert_eq!(err.to_string(), "projection 'Y' not found");
    }

    #[test]
    fn error_display_non_flat() {
        let err = ProjectionStoreError::NonFlatColumnType { column: "data".into(), type_name: "Struct".into() };
        assert!(err.to_string().contains("non-flat type"));
        assert!(err.to_string().contains("data"));
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(ProjectionStoreError::NotFound("Z".into()));
        assert_eq!(err.to_string(), "projection 'Z' not found");
    }
}
