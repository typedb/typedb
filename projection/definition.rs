/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Projection definition — the bridge between a TypeQL schema definition
//! and the relational column model exposed through pgwire.
//!
//! A [`ProjectionDefinition`] records:
//! - the projection's **name** (used as the SQL table name),
//! - an ordered list of **columns** (name + TypeDB value type), and
//! - the **source query** text (the TypeQL `match … fetch …` that populates it).
//!
//! At define-time the system validates that the fetch produces flat rows
//! (no nested sub-fetches). At materialization-time the `materializer`
//! compiles and executes the source query, then populates a
//! [`ProjectionInfo`](crate::pgwire::query_executor::ProjectionInfo)
//! for use by the pgwire layer.

use encoding::value::value_type::ValueTypeCategory;

// ── Column definition ──────────────────────────────────────────────

/// A single column in a projection definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    /// Column name (used as the SQL column name).
    pub name: String,
    /// TypeDB value type category for this column.
    pub value_type: ValueTypeCategory,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, value_type: ValueTypeCategory) -> Self {
        Self { name: name.into(), value_type }
    }
}

// ── Projection definition ──────────────────────────────────────────

/// A projection definition stored in the TypeDB schema.
///
/// Records the flat relational shape of a TypeQL fetch query so that
/// the pgwire layer can advertise columns, OIDs, and wire-format metadata
/// without re-analysing the source query.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionDefinition {
    /// User-visible name (doubles as the SQL table name).
    name: String,
    /// Ordered column definitions.
    columns: Vec<ColumnDefinition>,
    /// Original TypeQL source query text (match + fetch).
    /// Stored for re-materialization and introspection.
    source_query: String,
}

/// Errors that can occur when building a projection definition.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionDefinitionError {
    /// Name must be a non-empty identifier.
    EmptyName,
    /// At least one column is required.
    NoColumns,
    /// Duplicate column name detected.
    DuplicateColumn(String),
    /// Source query must not be empty.
    EmptySourceQuery,
}

impl std::fmt::Display for ProjectionDefinitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "projection name must not be empty"),
            Self::NoColumns => write!(f, "projection must have at least one column"),
            Self::DuplicateColumn(name) => write!(f, "duplicate column name: '{name}'"),
            Self::EmptySourceQuery => write!(f, "source query must not be empty"),
        }
    }
}

impl std::error::Error for ProjectionDefinitionError {}

impl ProjectionDefinition {
    /// Build a new projection definition with validation.
    pub fn new(
        name: impl Into<String>,
        columns: Vec<ColumnDefinition>,
        source_query: impl Into<String>,
    ) -> Result<Self, ProjectionDefinitionError> {
        let name = name.into();
        let source_query = source_query.into();

        if name.is_empty() {
            return Err(ProjectionDefinitionError::EmptyName);
        }
        if columns.is_empty() {
            return Err(ProjectionDefinitionError::NoColumns);
        }
        if source_query.is_empty() {
            return Err(ProjectionDefinitionError::EmptySourceQuery);
        }

        // Check for duplicate column names.
        let mut seen = std::collections::HashSet::new();
        for col in &columns {
            if !seen.insert(&col.name) {
                return Err(ProjectionDefinitionError::DuplicateColumn(col.name.clone()));
            }
        }

        Ok(Self { name, columns, source_query })
    }

    // ── Accessors ──────────────────────────────────────────────

    /// Projection name (SQL table name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ordered column definitions.
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Column names in order.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Column value types in order.
    pub fn column_types(&self) -> Vec<ValueTypeCategory> {
        self.columns.iter().map(|c| c.value_type).collect()
    }

    /// The TypeQL source query text.
    pub fn source_query(&self) -> &str {
        &self.source_query
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_columns() -> Vec<ColumnDefinition> {
        vec![
            ColumnDefinition::new("id", ValueTypeCategory::Integer),
            ColumnDefinition::new("name", ValueTypeCategory::String),
            ColumnDefinition::new("active", ValueTypeCategory::Boolean),
        ]
    }

    const SAMPLE_QUERY: &str = "match $p isa person; fetch { $p.name; $p.age; };";

    // ── Construction ─────────────────────────────────────────────

    #[test]
    fn new_valid_definition() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        assert_eq!(def.name(), "people");
        assert_eq!(def.column_count(), 3);
        assert_eq!(def.source_query(), SAMPLE_QUERY);
    }

    #[test]
    fn new_rejects_empty_name() {
        let result = ProjectionDefinition::new("", sample_columns(), SAMPLE_QUERY);
        assert_eq!(result, Err(ProjectionDefinitionError::EmptyName));
    }

    #[test]
    fn new_rejects_no_columns() {
        let result = ProjectionDefinition::new("people", vec![], SAMPLE_QUERY);
        assert_eq!(result, Err(ProjectionDefinitionError::NoColumns));
    }

    #[test]
    fn new_rejects_duplicate_columns() {
        let cols = vec![
            ColumnDefinition::new("name", ValueTypeCategory::String),
            ColumnDefinition::new("name", ValueTypeCategory::String),
        ];
        let result = ProjectionDefinition::new("people", cols, SAMPLE_QUERY);
        assert_eq!(result, Err(ProjectionDefinitionError::DuplicateColumn("name".into())));
    }

    #[test]
    fn new_rejects_empty_source_query() {
        let result = ProjectionDefinition::new("people", sample_columns(), "");
        assert_eq!(result, Err(ProjectionDefinitionError::EmptySourceQuery));
    }

    // ── Accessors ────────────────────────────────────────────────

    #[test]
    fn column_names_in_order() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        assert_eq!(def.column_names(), vec!["id", "name", "active"]);
    }

    #[test]
    fn column_types_in_order() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        assert_eq!(
            def.column_types(),
            vec![ValueTypeCategory::Integer, ValueTypeCategory::String, ValueTypeCategory::Boolean]
        );
    }

    #[test]
    fn columns_returns_full_definitions() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        assert_eq!(def.columns()[1], ColumnDefinition { name: "name".into(), value_type: ValueTypeCategory::String });
    }

    #[test]
    fn column_count_matches() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        assert_eq!(def.column_count(), def.columns().len());
    }

    // ── Single column ────────────────────────────────────────────

    #[test]
    fn single_column_definition() {
        let cols = vec![ColumnDefinition::new("id", ValueTypeCategory::Integer)];
        let def = ProjectionDefinition::new("counters", cols, "match $c isa counter; fetch { $c.value; };").unwrap();
        assert_eq!(def.column_count(), 1);
        assert_eq!(def.column_names(), vec!["id"]);
    }

    // ── All value type categories ────────────────────────────────

    #[test]
    fn all_value_type_categories_accepted() {
        let cols = vec![
            ColumnDefinition::new("bool_col", ValueTypeCategory::Boolean),
            ColumnDefinition::new("int_col", ValueTypeCategory::Integer),
            ColumnDefinition::new("dbl_col", ValueTypeCategory::Double),
            ColumnDefinition::new("dec_col", ValueTypeCategory::Decimal),
            ColumnDefinition::new("dat_col", ValueTypeCategory::Date),
            ColumnDefinition::new("dt_col", ValueTypeCategory::DateTime),
            ColumnDefinition::new("dtz_col", ValueTypeCategory::DateTimeTZ),
            ColumnDefinition::new("dur_col", ValueTypeCategory::Duration),
            ColumnDefinition::new("str_col", ValueTypeCategory::String),
            ColumnDefinition::new("struct_col", ValueTypeCategory::Struct),
        ];
        let def = ProjectionDefinition::new("all_types", cols, "match $x isa thing;").unwrap();
        assert_eq!(def.column_count(), 10);
    }

    // ── Clone and Debug ──────────────────────────────────────────

    #[test]
    fn definition_is_cloneable() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        let cloned = def.clone();
        assert_eq!(def, cloned);
    }

    #[test]
    fn definition_is_debuggable() {
        let def = ProjectionDefinition::new("people", sample_columns(), SAMPLE_QUERY).unwrap();
        let debug = format!("{:?}", def);
        assert!(debug.contains("people"));
    }

    // ── Error Display ────────────────────────────────────────────

    #[test]
    fn error_display_empty_name() {
        let err = ProjectionDefinitionError::EmptyName;
        assert_eq!(err.to_string(), "projection name must not be empty");
    }

    #[test]
    fn error_display_duplicate_column() {
        let err = ProjectionDefinitionError::DuplicateColumn("name".into());
        assert!(err.to_string().contains("name"));
    }
}
