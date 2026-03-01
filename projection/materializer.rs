/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Projection materializer — orchestrates TypeQL query execution and
//! populates the in-memory [`MaterializedCatalog`] with row data.
//!
//! The materializer bridges between TypeQL and the pgwire layer:
//! 1. Reads projection definitions from the catalog
//! 2. Delegates query execution to a [`SourceQueryExecutor`] (implemented
//!    by the server layer)
//! 3. Validates result shapes against column definitions
//! 4. Atomically replaces catalog row data via
//!    [`MaterializedCatalog::replace_rows`]
//!
//! # Server integration
//!
//! The server implements [`SourceQueryExecutor`] by opening a read
//! transaction and running the TypeQL source query through the
//! `QueryManager` pipeline. The returned rows are already formatted as
//! Postgres text-protocol strings.

use crate::{
    catalog::MaterializedCatalog, definition::ProjectionDefinition, pgwire::query_executor::ProjectionCatalog,
};

// ── Source query executor trait ─────────────────────────────────────

/// Abstraction over TypeQL query execution for projection materialization.
///
/// The server layer implements this trait to bridge from the materializer
/// to the TypeDB query engine. Each call opens a read transaction,
/// executes the source query, and returns rows as Postgres text-format
/// strings matching the projection's column order.
pub trait SourceQueryExecutor: Send + Sync {
    /// Execute a TypeQL source query against the named database.
    ///
    /// Returns rows where each inner `Vec` has one `Option<String>` per
    /// column (in definition order). `None` represents a NULL value.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error message if the query fails.
    fn execute(&self, database_name: &str, source_query: &str) -> Result<Vec<Vec<Option<String>>>, String>;
}

// ── Error types ────────────────────────────────────────────────────

/// Errors that can occur during projection materialization.
#[derive(Debug, Clone, PartialEq)]
pub enum MaterializationError {
    /// The named projection does not exist in the catalog.
    ProjectionNotFound(String),
    /// The source query execution failed.
    QueryExecutionFailed { projection: String, message: String },
    /// A result row has a different number of columns than the definition.
    ColumnCountMismatch { projection: String, expected: usize, actual: usize },
}

impl std::fmt::Display for MaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectionNotFound(name) => {
                write!(f, "projection not found: '{name}'")
            }
            Self::QueryExecutionFailed { projection, message } => {
                write!(f, "query execution failed for projection '{projection}': {message}")
            }
            Self::ColumnCountMismatch { projection, expected, actual } => {
                write!(
                    f,
                    "column count mismatch for projection '{projection}': \
                     expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for MaterializationError {}

// ── Result types ───────────────────────────────────────────────────

/// Result of refreshing a single projection.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshResult {
    /// Name of the refreshed projection (as stored in the definition).
    pub projection_name: String,
    /// Number of rows materialized.
    pub row_count: usize,
}

/// Aggregate result of refreshing all projections.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshAllResult {
    /// Projections that were successfully refreshed.
    pub successes: Vec<RefreshResult>,
    /// Projections that failed to refresh.
    pub failures: Vec<MaterializationError>,
}

impl RefreshAllResult {
    /// Total number of projections attempted.
    pub fn total(&self) -> usize {
        self.successes.len() + self.failures.len()
    }

    /// Whether all projections refreshed successfully.
    pub fn all_succeeded(&self) -> bool {
        self.failures.is_empty()
    }
}

// ── Materializer ───────────────────────────────────────────────────

/// Orchestrates projection materialization by executing TypeQL queries
/// and populating the catalog with results.
///
/// The materializer does not own any query infrastructure — it delegates
/// to a [`SourceQueryExecutor`] provided by the caller. This keeps the
/// `projection` crate decoupled from the TypeDB query engine.
pub struct Materializer {
    catalog: MaterializedCatalog,
    database_name: String,
}

impl Materializer {
    /// Create a new materializer for the given catalog and database.
    pub fn new(catalog: MaterializedCatalog, database_name: impl Into<String>) -> Self {
        Self { catalog, database_name: database_name.into() }
    }

    /// Reference to the underlying catalog.
    pub fn catalog(&self) -> &MaterializedCatalog {
        &self.catalog
    }

    /// Database name this materializer operates on.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Refresh a single projection by name.
    ///
    /// 1. Looks up the projection definition in the catalog
    /// 2. Executes the source query via the executor
    /// 3. Validates column count consistency
    /// 4. Atomically replaces the catalog's row data
    pub fn refresh_one(
        &self,
        name: &str,
        executor: &dyn SourceQueryExecutor,
    ) -> Result<RefreshResult, MaterializationError> {
        let definition = self
            .catalog
            .get_definition(name)
            .ok_or_else(|| MaterializationError::ProjectionNotFound(name.to_string()))?;

        let rows = executor
            .execute(&self.database_name, definition.source_query())
            .map_err(|message| MaterializationError::QueryExecutionFailed { projection: name.to_string(), message })?;

        // Validate column count for every row.
        let expected = definition.column_count();
        for row in &rows {
            if row.len() != expected {
                return Err(MaterializationError::ColumnCountMismatch {
                    projection: name.to_string(),
                    expected,
                    actual: row.len(),
                });
            }
        }

        let row_count = rows.len();
        self.catalog.replace_rows(name, rows);

        Ok(RefreshResult { projection_name: definition.name().to_string(), row_count })
    }

    /// Refresh all projections in the catalog.
    ///
    /// Iterates over every registered projection and attempts to refresh
    /// it. Failures for individual projections do not abort the entire
    /// operation.
    pub fn refresh_all(&self, executor: &dyn SourceQueryExecutor) -> RefreshAllResult {
        let names = self.catalog.list_projections();
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for name in &names {
            match self.refresh_one(name, executor) {
                Ok(result) => successes.push(result),
                Err(err) => failures.push(err),
            }
        }

        RefreshAllResult { successes, failures }
    }

    // ── Database-open trigger ──────────────────────────────────

    /// Register projection definitions and materialize them all.
    ///
    /// Intended to be called when a database is opened: the server loads
    /// projection definitions from the schema and passes them here. Each
    /// definition is registered in the catalog (replacing any existing
    /// projection with the same name), then all projections are refreshed
    /// via the executor.
    ///
    /// Definitions that were previously registered but are *not* in the
    /// supplied list remain in the catalog (they are not deregistered).
    pub fn register_and_refresh_all(
        &self,
        definitions: Vec<ProjectionDefinition>,
        executor: &dyn SourceQueryExecutor,
    ) -> RefreshAllResult {
        for def in definitions {
            self.catalog.register(def);
        }
        self.refresh_all(executor)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use encoding::value::value_type::ValueTypeCategory;

    use super::*;
    use crate::{
        definition::{ColumnDefinition, ProjectionDefinition},
        pgwire::query_executor::ProjectionCatalog,
    };

    // ── Mock executor ──────────────────────────────────────────

    /// Records calls and returns pre-configured results by source query.
    struct MockExecutor {
        /// source_query → result
        results: HashMap<String, Result<Vec<Vec<Option<String>>>, String>>,
        /// Recorded (database_name, source_query) pairs.
        calls: Mutex<Vec<(String, String)>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self { results: HashMap::new(), calls: Mutex::new(Vec::new()) }
        }

        fn with_result(mut self, source_query: &str, result: Result<Vec<Vec<Option<String>>>, String>) -> Self {
            self.results.insert(source_query.to_string(), result);
            self
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl SourceQueryExecutor for MockExecutor {
        fn execute(&self, database_name: &str, source_query: &str) -> Result<Vec<Vec<Option<String>>>, String> {
            self.calls.lock().unwrap().push((database_name.to_string(), source_query.to_string()));
            self.results.get(source_query).cloned().unwrap_or_else(|| Ok(Vec::new()))
        }
    }

    // ── Helpers ────────────────────────────────────────────────

    fn sample_definition(name: &str, query: &str) -> ProjectionDefinition {
        ProjectionDefinition::new(
            name,
            vec![
                ColumnDefinition::new("id", ValueTypeCategory::Integer),
                ColumnDefinition::new("name", ValueTypeCategory::String),
            ],
            query,
        )
        .unwrap()
    }

    fn sample_rows() -> Vec<Vec<Option<String>>> {
        vec![
            vec![Some("1".to_string()), Some("Alice".to_string())],
            vec![Some("2".to_string()), Some("Bob".to_string())],
        ]
    }

    fn make_materializer(defs: &[(&str, &str)]) -> Materializer {
        let catalog = MaterializedCatalog::new();
        for (name, query) in defs {
            catalog.register(sample_definition(name, query));
        }
        Materializer::new(catalog, "test_db")
    }

    // ── Constructor / accessor tests ───────────────────────────

    #[test]
    fn new_creates_materializer_with_catalog() {
        let catalog = MaterializedCatalog::new();
        catalog.register(sample_definition("users", "match $u isa user; fetch { id, name };"));
        let mat = Materializer::new(catalog, "mydb");
        assert_eq!(mat.catalog().len(), 1);
    }

    #[test]
    fn catalog_accessor_returns_shared_catalog() {
        let mat = make_materializer(&[("p1", "q1")]);
        assert_eq!(mat.catalog().len(), 1);
        assert!(!mat.catalog().is_empty());
    }

    #[test]
    fn database_name_accessor() {
        let mat = make_materializer(&[]);
        assert_eq!(mat.database_name(), "test_db");
    }

    // ── refresh_one: success paths ─────────────────────────────

    #[test]
    fn refresh_one_populates_catalog() {
        let query = "match $u isa user; fetch { id, name };";
        let mat = make_materializer(&[("users", query)]);
        let exec = MockExecutor::new().with_result(query, Ok(sample_rows()));

        let result = mat.refresh_one("users", &exec).unwrap();
        assert_eq!(result.row_count, 2);

        let proj = mat.catalog().get_projection("users").unwrap();
        assert_eq!(proj.rows.len(), 2);
        assert_eq!(proj.rows[0][0], Some("1".to_string()));
    }

    #[test]
    fn refresh_one_returns_correct_projection_name() {
        let query = "q";
        let mat = make_materializer(&[("People", query)]);
        let exec = MockExecutor::new().with_result(query, Ok(sample_rows()));

        let result = mat.refresh_one("People", &exec).unwrap();
        assert_eq!(result.projection_name, "People");
    }

    #[test]
    fn refresh_one_replaces_previous_rows() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]);
        let rows_v1 = vec![vec![Some("1".to_string()), Some("Old".to_string())]];
        let rows_v2 = vec![vec![Some("2".to_string()), Some("New".to_string())]];

        let exec1 = MockExecutor::new().with_result(query, Ok(rows_v1));
        mat.refresh_one("t", &exec1).unwrap();

        let exec2 = MockExecutor::new().with_result(query, Ok(rows_v2));
        mat.refresh_one("t", &exec2).unwrap();

        let proj = mat.catalog().get_projection("t").unwrap();
        assert_eq!(proj.rows.len(), 1);
        assert_eq!(proj.rows[0][1], Some("New".to_string()));
    }

    #[test]
    fn refresh_one_zero_rows_clears_data() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]);

        // Populate first.
        let exec1 = MockExecutor::new().with_result(query, Ok(sample_rows()));
        mat.refresh_one("t", &exec1).unwrap();
        assert_eq!(mat.catalog().get_projection("t").unwrap().rows.len(), 2);

        // Refresh with empty result.
        let exec2 = MockExecutor::new().with_result(query, Ok(Vec::new()));
        let result = mat.refresh_one("t", &exec2).unwrap();
        assert_eq!(result.row_count, 0);
        assert!(mat.catalog().get_projection("t").unwrap().rows.is_empty());
    }

    #[test]
    fn refresh_one_preserves_null_values() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]);
        let rows = vec![vec![Some("1".to_string()), None]];
        let exec = MockExecutor::new().with_result(query, Ok(rows));

        mat.refresh_one("t", &exec).unwrap();

        let proj = mat.catalog().get_projection("t").unwrap();
        assert_eq!(proj.rows[0][0], Some("1".to_string()));
        assert_eq!(proj.rows[0][1], None);
    }

    #[test]
    fn refresh_one_case_insensitive_lookup() {
        let query = "q";
        let mat = make_materializer(&[("Users", query)]);
        let exec = MockExecutor::new().with_result(query, Ok(sample_rows()));

        let result = mat.refresh_one("users", &exec).unwrap();
        assert_eq!(result.row_count, 2);
    }

    #[test]
    fn refresh_one_passes_correct_args_to_executor() {
        let query = "match $u isa user; fetch { id, name };";
        let mat = make_materializer(&[("users", query)]);
        let exec = MockExecutor::new().with_result(query, Ok(sample_rows()));

        mat.refresh_one("users", &exec).unwrap();

        let calls = exec.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "test_db");
        assert_eq!(calls[0].1, query);
    }

    // ── refresh_one: error paths ───────────────────────────────

    #[test]
    fn refresh_one_projection_not_found() {
        let mat = make_materializer(&[]);
        let exec = MockExecutor::new();

        let err = mat.refresh_one("nonexistent", &exec).unwrap_err();
        assert_eq!(err, MaterializationError::ProjectionNotFound("nonexistent".to_string()));
    }

    #[test]
    fn refresh_one_executor_error() {
        let query = "bad query";
        let mat = make_materializer(&[("t", query)]);
        let exec = MockExecutor::new().with_result(query, Err("syntax error".to_string()));

        let err = mat.refresh_one("t", &exec).unwrap_err();
        assert_eq!(
            err,
            MaterializationError::QueryExecutionFailed {
                projection: "t".to_string(),
                message: "syntax error".to_string(),
            }
        );
    }

    #[test]
    fn refresh_one_column_count_mismatch() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]); // 2 columns in definition
        let rows = vec![vec![Some("1".to_string())]]; // only 1 column in data
        let exec = MockExecutor::new().with_result(query, Ok(rows));

        let err = mat.refresh_one("t", &exec).unwrap_err();
        assert_eq!(
            err,
            MaterializationError::ColumnCountMismatch { projection: "t".to_string(), expected: 2, actual: 1 }
        );
    }

    #[test]
    fn refresh_one_column_mismatch_leaves_catalog_unchanged() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]);

        // Populate with valid data.
        let exec1 = MockExecutor::new().with_result(query, Ok(sample_rows()));
        mat.refresh_one("t", &exec1).unwrap();

        // Attempt refresh with wrong column count.
        let bad_rows = vec![vec![Some("x".to_string())]];
        let exec2 = MockExecutor::new().with_result(query, Ok(bad_rows));
        assert!(mat.refresh_one("t", &exec2).is_err());

        // Original data untouched.
        let proj = mat.catalog().get_projection("t").unwrap();
        assert_eq!(proj.rows.len(), 2);
    }

    #[test]
    fn refresh_one_extra_columns_rejected() {
        let query = "q";
        let mat = make_materializer(&[("t", query)]); // 2 columns
        let rows = vec![vec![Some("1".to_string()), Some("a".to_string()), Some("extra".to_string())]]; // 3 columns
        let exec = MockExecutor::new().with_result(query, Ok(rows));

        let err = mat.refresh_one("t", &exec).unwrap_err();
        assert_eq!(
            err,
            MaterializationError::ColumnCountMismatch { projection: "t".to_string(), expected: 2, actual: 3 }
        );
    }

    // ── refresh_all ────────────────────────────────────────────

    #[test]
    fn refresh_all_empty_catalog() {
        let mat = make_materializer(&[]);
        let exec = MockExecutor::new();

        let result = mat.refresh_all(&exec);
        assert!(result.successes.is_empty());
        assert!(result.failures.is_empty());
        assert_eq!(result.total(), 0);
        assert!(result.all_succeeded());
    }

    #[test]
    fn refresh_all_refreshes_all_projections() {
        let q1 = "query1";
        let q2 = "query2";
        let mat = make_materializer(&[("p1", q1), ("p2", q2)]);
        let exec = MockExecutor::new()
            .with_result(q1, Ok(vec![vec![Some("1".to_string()), Some("A".to_string())]]))
            .with_result(q2, Ok(vec![vec![Some("2".to_string()), Some("B".to_string())]]));

        let result = mat.refresh_all(&exec);
        assert_eq!(result.successes.len(), 2);
        assert!(result.failures.is_empty());
        assert!(result.all_succeeded());

        assert_eq!(mat.catalog().get_projection("p1").unwrap().rows.len(), 1);
        assert_eq!(mat.catalog().get_projection("p2").unwrap().rows.len(), 1);
    }

    #[test]
    fn refresh_all_mixed_success_and_failure() {
        let q1 = "good_query";
        let q2 = "bad_query";
        let mat = make_materializer(&[("good", q1), ("bad", q2)]);
        let exec =
            MockExecutor::new().with_result(q1, Ok(sample_rows())).with_result(q2, Err("execution failed".to_string()));

        let result = mat.refresh_all(&exec);
        assert_eq!(result.successes.len(), 1);
        assert_eq!(result.failures.len(), 1);
        assert!(!result.all_succeeded());
        assert_eq!(result.total(), 2);
    }

    #[test]
    fn refresh_all_reports_correct_row_counts() {
        let q1 = "q1";
        let q2 = "q2";
        let mat = make_materializer(&[("a", q1), ("b", q2)]);
        let exec = MockExecutor::new()
            .with_result(
                q1,
                Ok(vec![
                    vec![Some("1".to_string()), Some("X".to_string())],
                    vec![Some("2".to_string()), Some("Y".to_string())],
                    vec![Some("3".to_string()), Some("Z".to_string())],
                ]),
            )
            .with_result(q2, Ok(vec![vec![Some("10".to_string()), Some("W".to_string())]]));

        let result = mat.refresh_all(&exec);
        let total_rows: usize = result.successes.iter().map(|r| r.row_count).sum();
        assert_eq!(total_rows, 4); // 3 + 1
    }

    // ── Display / Error trait ──────────────────────────────────

    #[test]
    fn materialization_error_display_not_found() {
        let err = MaterializationError::ProjectionNotFound("foo".to_string());
        assert_eq!(err.to_string(), "projection not found: 'foo'");
    }

    #[test]
    fn materialization_error_display_execution_failed() {
        let err = MaterializationError::QueryExecutionFailed {
            projection: "bar".to_string(),
            message: "timeout".to_string(),
        };
        assert_eq!(err.to_string(), "query execution failed for projection 'bar': timeout");
    }

    #[test]
    fn materialization_error_display_column_mismatch() {
        let err = MaterializationError::ColumnCountMismatch { projection: "baz".to_string(), expected: 3, actual: 5 };
        assert_eq!(err.to_string(), "column count mismatch for projection 'baz': expected 3, got 5");
    }

    #[test]
    fn materialization_error_is_std_error() {
        let err = MaterializationError::ProjectionNotFound("x".to_string());
        let _dyn_err: &dyn std::error::Error = &err;
    }

    // ── register_and_refresh_all (DB-open trigger) ─────────────

    #[test]
    fn register_and_refresh_all_registers_and_materializes() {
        let q1 = "query_a";
        let q2 = "query_b";
        let catalog = MaterializedCatalog::new();
        let mat = Materializer::new(catalog, "mydb");
        let exec = MockExecutor::new()
            .with_result(q1, Ok(vec![vec![Some("1".to_string()), Some("A".to_string())]]))
            .with_result(q2, Ok(vec![vec![Some("2".to_string()), Some("B".to_string())]]));

        let defs = vec![sample_definition("alpha", q1), sample_definition("beta", q2)];
        let result = mat.register_and_refresh_all(defs, &exec);

        assert!(result.all_succeeded());
        assert_eq!(result.successes.len(), 2);
        assert_eq!(mat.catalog().len(), 2);

        let p = mat.catalog().get_projection("alpha").unwrap();
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.rows[0][0], Some("1".to_string()));
    }

    #[test]
    fn register_and_refresh_all_empty_definitions() {
        let catalog = MaterializedCatalog::new();
        let mat = Materializer::new(catalog, "db");
        let exec = MockExecutor::new();

        let result = mat.register_and_refresh_all(Vec::new(), &exec);
        assert!(result.all_succeeded());
        assert_eq!(result.total(), 0);
        assert!(mat.catalog().is_empty());
    }

    #[test]
    fn register_and_refresh_all_partial_failure() {
        let q_good = "good";
        let q_bad = "bad";
        let catalog = MaterializedCatalog::new();
        let mat = Materializer::new(catalog, "db");
        let exec =
            MockExecutor::new().with_result(q_good, Ok(sample_rows())).with_result(q_bad, Err("boom".to_string()));

        let defs = vec![sample_definition("ok_proj", q_good), sample_definition("bad_proj", q_bad)];
        let result = mat.register_and_refresh_all(defs, &exec);

        assert!(!result.all_succeeded());
        assert_eq!(result.successes.len(), 1);
        assert_eq!(result.failures.len(), 1);

        // Successful projection has data.
        assert_eq!(mat.catalog().get_projection("ok_proj").unwrap().rows.len(), 2);
        // Failed projection was registered but has no rows.
        assert!(mat.catalog().get_projection("bad_proj").unwrap().rows.is_empty());
    }

    #[test]
    fn register_and_refresh_all_preserves_existing_projections() {
        let q_old = "old_query";
        let q_new = "new_query";
        let catalog = MaterializedCatalog::new();
        catalog.register(sample_definition("existing", q_old));
        catalog.replace_rows("existing", sample_rows());
        let mat = Materializer::new(catalog, "db");

        let exec = MockExecutor::new().with_result(q_new, Ok(vec![vec![Some("9".to_string()), Some("Z".to_string())]]));

        let defs = vec![sample_definition("newcomer", q_new)];
        let result = mat.register_and_refresh_all(defs, &exec);

        // New projection is materialized.
        assert_eq!(result.successes.len(), 2); // existing + newcomer both refreshed
        assert_eq!(mat.catalog().len(), 2);

        // Existing projection is still there.
        assert!(mat.catalog().get_projection("existing").is_some());
    }
}
