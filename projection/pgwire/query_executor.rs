/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Query executor: bridges parsed SQL to the projection catalog.
//!
//! Takes a [`ParsedQuery`] from `sql_parser`, resolves it against a
//! [`ProjectionCatalog`] trait, applies WHERE / ORDER BY / LIMIT / OFFSET
//! post-processing, and returns a [`QueryOutcome`] ready for the wire.

use crate::{
    pgwire::{
        connection::{QueryOutcome, QueryResult},
        messages::ColumnDescription,
        sql_parser::{
            ComparisonOp, LiteralValue, OrderByExpr, ParsedQuery, SelectColumn, SortDirection, WhereCondition,
        },
    },
    type_mapping::PgOid,
};

// ── Catalog trait ──────────────────────────────────────────────────

/// Metadata for a single column in a materialized projection.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogColumn {
    pub name: String,
    pub type_oid: PgOid,
    pub type_size: i16,
}

/// A materialized projection's metadata and data.
#[derive(Debug, Clone)]
pub struct ProjectionInfo {
    pub name: String,
    pub columns: Vec<CatalogColumn>,
    /// Row data: outer = rows, inner = columns, in the same order as `columns`.
    /// `None` represents SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
}

/// Trait for looking up materialized projections by name.
/// Implementations can be backed by a real catalog or an in-memory test stub.
pub trait ProjectionCatalog: Send + Sync {
    /// Return all available projection names.
    fn list_projections(&self) -> Vec<String>;

    /// Look up a projection by name (case-insensitive).
    fn get_projection(&self, name: &str) -> Option<ProjectionInfo>;
}

// ── Executor ───────────────────────────────────────────────────────

/// Execute a parsed query against the catalog, returning a wire-ready outcome.
pub fn execute_query(catalog: &dyn ProjectionCatalog, query: &ParsedQuery) -> QueryOutcome {
    match query {
        ParsedQuery::ShowTables => execute_show_tables(catalog),
        ParsedQuery::Select { table, schema, columns, where_clause, order_by, limit, offset } => {
            // Route information_schema queries.
            if let Some(s) = schema {
                if s.eq_ignore_ascii_case("information_schema") {
                    return execute_information_schema(
                        catalog,
                        table,
                        columns,
                        where_clause,
                        order_by,
                        *limit,
                        *offset,
                    );
                }
                // Unknown schema → treat as regular table lookup (will fail if not found).
            }
            execute_select(catalog, table, columns, where_clause, order_by, *limit, *offset)
        }
    }
}

// ── SHOW TABLES ────────────────────────────────────────────────────

fn execute_show_tables(catalog: &dyn ProjectionCatalog) -> QueryOutcome {
    let names = catalog.list_projections();
    let columns = vec![ColumnDescription {
        name: "table_name".to_string(),
        table_oid: 0,
        column_index: 0,
        type_oid: crate::type_mapping::PG_OID_TEXT,
        type_size: -1,
        type_modifier: -1,
        format_code: 0,
    }];
    let rows: Vec<Vec<Option<String>>> = names.into_iter().map(|n| vec![Some(n)]).collect();
    QueryOutcome::Result(QueryResult { columns, rows })
}

// ── SELECT from projection ─────────────────────────────────────────

fn execute_select(
    catalog: &dyn ProjectionCatalog,
    table: &str,
    columns: &[SelectColumn],
    where_clause: &[WhereCondition],
    order_by: &[OrderByExpr],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    // Look up projection.
    let projection = match catalog.get_projection(table) {
        Some(p) => p,
        None => {
            return QueryOutcome::Error {
                severity: "ERROR".into(),
                code: "42P01".into(),
                message: format!("relation \"{table}\" does not exist"),
            };
        }
    };

    // Resolve column indices for WHERE and ORDER BY first (before projection).
    // Validate WHERE columns.
    for cond in where_clause {
        if find_column_index(&projection.columns, &cond.column).is_none() {
            return QueryOutcome::Error {
                severity: "ERROR".into(),
                code: "42703".into(),
                message: format!("column \"{}\" does not exist", cond.column),
            };
        }
    }

    // Validate ORDER BY columns.
    for ob in order_by {
        if find_column_index(&projection.columns, &ob.column).is_none() {
            return QueryOutcome::Error {
                severity: "ERROR".into(),
                code: "42703".into(),
                message: format!("column \"{}\" does not exist", ob.column),
            };
        }
    }

    // Resolve output columns.
    let (out_columns, col_indices) = match resolve_columns(&projection.columns, columns) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Filter rows by WHERE.
    let mut rows: Vec<&Vec<Option<String>>> =
        projection.rows.iter().filter(|row| matches_where(row, &projection.columns, where_clause)).collect();

    // Sort by ORDER BY.
    if !order_by.is_empty() {
        let ob_indices: Vec<(usize, SortDirection)> = order_by
            .iter()
            .map(|ob| {
                let idx = find_column_index(&projection.columns, &ob.column).unwrap();
                (idx, ob.direction)
            })
            .collect();
        rows.sort_by(|a, b| compare_rows(a, b, &ob_indices));
    }

    // Apply OFFSET.
    let start = offset.unwrap_or(0) as usize;
    let rows = if start >= rows.len() { vec![] } else { rows[start..].to_vec() };

    // Apply LIMIT.
    let rows: Vec<&Vec<Option<String>>> = match limit {
        Some(n) => rows.into_iter().take(n as usize).collect(),
        None => rows,
    };

    // Project columns.
    let projected_rows: Vec<Vec<Option<String>>> =
        rows.iter().map(|row| col_indices.iter().map(|&idx| row[idx].clone()).collect()).collect();

    QueryOutcome::Result(QueryResult { columns: out_columns, rows: projected_rows })
}

// ── information_schema ─────────────────────────────────────────────

fn execute_information_schema(
    catalog: &dyn ProjectionCatalog,
    table: &str,
    _columns: &[SelectColumn],
    where_clause: &[WhereCondition],
    _order_by: &[OrderByExpr],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    if table.eq_ignore_ascii_case("tables") {
        execute_info_schema_tables(catalog, where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("columns") {
        execute_info_schema_columns(catalog, where_clause, limit, offset)
    } else {
        QueryOutcome::Error {
            severity: "ERROR".into(),
            code: "42P01".into(),
            message: format!("relation \"information_schema.{table}\" does not exist"),
        }
    }
}

fn info_schema_text_col(name: &str) -> ColumnDescription {
    ColumnDescription {
        name: name.to_string(),
        table_oid: 0,
        column_index: 0,
        type_oid: crate::type_mapping::PG_OID_TEXT,
        type_size: -1,
        type_modifier: -1,
        format_code: 0,
    }
}

fn execute_info_schema_tables(
    catalog: &dyn ProjectionCatalog,
    where_clause: &[WhereCondition],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    let columns = vec![
        info_schema_text_col("table_catalog"),
        info_schema_text_col("table_schema"),
        info_schema_text_col("table_name"),
        info_schema_text_col("table_type"),
    ];

    let names = catalog.list_projections();
    let all_rows: Vec<Vec<Option<String>>> = names
        .into_iter()
        .map(|n| vec![Some("typedb".to_string()), Some("public".to_string()), Some(n), Some("BASE TABLE".to_string())])
        .collect();

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);

    QueryOutcome::Result(QueryResult { columns, rows: paged })
}

fn execute_info_schema_columns(
    catalog: &dyn ProjectionCatalog,
    where_clause: &[WhereCondition],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    let columns = vec![
        info_schema_text_col("table_catalog"),
        info_schema_text_col("table_schema"),
        info_schema_text_col("table_name"),
        info_schema_text_col("column_name"),
        info_schema_text_col("ordinal_position"),
        info_schema_text_col("data_type"),
        info_schema_text_col("is_nullable"),
    ];

    let projections = catalog.list_projections();
    let mut all_rows: Vec<Vec<Option<String>>> = Vec::new();
    for proj_name in &projections {
        if let Some(proj) = catalog.get_projection(proj_name) {
            for (i, col) in proj.columns.iter().enumerate() {
                all_rows.push(vec![
                    Some("typedb".to_string()),
                    Some("public".to_string()),
                    Some(proj.name.clone()),
                    Some(col.name.clone()),
                    Some((i + 1).to_string()),
                    Some(crate::type_mapping::pg_oid_to_type_name(col.type_oid).unwrap_or("unknown").to_string()),
                    Some("YES".to_string()),
                ]);
            }
        }
    }

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);

    QueryOutcome::Result(QueryResult { columns, rows: paged })
}

// ── Helpers ────────────────────────────────────────────────────────

fn find_column_index(catalog_cols: &[CatalogColumn], name: &str) -> Option<usize> {
    catalog_cols.iter().position(|c| c.name.eq_ignore_ascii_case(name))
}

/// Resolve output columns from the catalog + SELECT list.
/// Returns (ColumnDescriptions for wire, indices into the original row).
fn resolve_columns(
    catalog_cols: &[CatalogColumn],
    select_cols: &[SelectColumn],
) -> Result<(Vec<ColumnDescription>, Vec<usize>), QueryOutcome> {
    let mut descriptions = Vec::new();
    let mut indices = Vec::new();

    for sc in select_cols {
        match sc {
            SelectColumn::Star => {
                for (i, cc) in catalog_cols.iter().enumerate() {
                    descriptions.push(catalog_col_to_desc(cc));
                    indices.push(i);
                }
            }
            SelectColumn::Named { name, alias } => {
                let idx = match find_column_index(catalog_cols, name) {
                    Some(i) => i,
                    None => {
                        return Err(QueryOutcome::Error {
                            severity: "ERROR".into(),
                            code: "42703".into(),
                            message: format!("column \"{name}\" does not exist"),
                        });
                    }
                };
                let mut desc = catalog_col_to_desc(&catalog_cols[idx]);
                if let Some(a) = alias {
                    desc.name = a.clone();
                }
                descriptions.push(desc);
                indices.push(idx);
            }
        }
    }

    Ok((descriptions, indices))
}

fn catalog_col_to_desc(cc: &CatalogColumn) -> ColumnDescription {
    ColumnDescription {
        name: cc.name.clone(),
        table_oid: 0,
        column_index: 0,
        type_oid: cc.type_oid,
        type_size: cc.type_size,
        type_modifier: -1,
        format_code: 0,
    }
}

/// Test whether a row passes all WHERE conditions.
fn matches_where(row: &[Option<String>], catalog_cols: &[CatalogColumn], conditions: &[WhereCondition]) -> bool {
    conditions.iter().all(|cond| {
        let idx = match find_column_index(catalog_cols, &cond.column) {
            Some(i) => i,
            None => return false,
        };
        let cell = &row[idx];
        compare_cell(cell, &cond.op, &cond.value)
    })
}

/// Compare a cell value against a literal using the given operator.
fn compare_cell(cell: &Option<String>, op: &ComparisonOp, literal: &LiteralValue) -> bool {
    // NULL handling: IS NULL (Eq/Null), and NULL compared to anything else → false.
    if *literal == LiteralValue::Null {
        return match op {
            ComparisonOp::Eq => cell.is_none(),
            ComparisonOp::NotEq => cell.is_some(),
            _ => false,
        };
    }

    let cell_str = match cell {
        Some(s) => s,
        None => return false, // NULL doesn't match any non-NULL literal
    };

    match literal {
        LiteralValue::String(s) => {
            let ord = cell_str.cmp(s);
            apply_ordering(op, ord)
        }
        LiteralValue::Integer(n) => {
            if let Ok(cell_i) = cell_str.parse::<i64>() {
                apply_ordering(op, cell_i.cmp(n))
            } else if let Ok(cell_f) = cell_str.parse::<f64>() {
                apply_float_ordering(op, cell_f, *n as f64)
            } else {
                false
            }
        }
        LiteralValue::Float(n) => {
            if let Ok(cell_f) = cell_str.parse::<f64>() {
                apply_float_ordering(op, cell_f, *n)
            } else {
                false
            }
        }
        LiteralValue::Boolean(b) => {
            let cell_b = matches!(cell_str.as_str(), "t" | "true" | "1" | "TRUE" | "True");
            match op {
                ComparisonOp::Eq => cell_b == *b,
                ComparisonOp::NotEq => cell_b != *b,
                _ => false,
            }
        }
        LiteralValue::Null => unreachable!("handled above"),
    }
}

fn apply_ordering(op: &ComparisonOp, ord: std::cmp::Ordering) -> bool {
    match op {
        ComparisonOp::Eq => ord == std::cmp::Ordering::Equal,
        ComparisonOp::NotEq => ord != std::cmp::Ordering::Equal,
        ComparisonOp::Lt => ord == std::cmp::Ordering::Less,
        ComparisonOp::LtEq => ord != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ord == std::cmp::Ordering::Greater,
        ComparisonOp::GtEq => ord != std::cmp::Ordering::Less,
    }
}

fn apply_float_ordering(op: &ComparisonOp, a: f64, b: f64) -> bool {
    match a.partial_cmp(&b) {
        Some(ord) => apply_ordering(op, ord),
        None => false, // NaN
    }
}

/// Compare two rows for ORDER BY sorting.
fn compare_rows(
    a: &[Option<String>],
    b: &[Option<String>],
    ob_indices: &[(usize, SortDirection)],
) -> std::cmp::Ordering {
    for &(idx, direction) in ob_indices {
        let va = &a[idx];
        let vb = &b[idx];

        let ord = compare_optional_strings(va, vb);
        let ord = match direction {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Compare two optional strings, with NULLs sorting last (Postgres default for ASC).
fn compare_optional_strings(a: &Option<String>, b: &Option<String>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater, // NULL last
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(sa), Some(sb)) => {
            // Try numeric comparison first.
            if let (Ok(fa), Ok(fb)) = (sa.parse::<f64>(), sb.parse::<f64>()) {
                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                sa.cmp(sb)
            }
        }
    }
}

/// Apply WHERE filtering to virtual (information_schema) rows.
fn apply_where_to_virtual_rows(
    columns: &[ColumnDescription],
    rows: &[Vec<Option<String>>],
    where_clause: &[WhereCondition],
) -> Vec<Vec<Option<String>>> {
    if where_clause.is_empty() {
        return rows.to_vec();
    }

    rows.iter()
        .filter(|row| {
            where_clause.iter().all(|cond| {
                let idx = match columns.iter().position(|c| c.name.eq_ignore_ascii_case(&cond.column)) {
                    Some(i) => i,
                    None => return false,
                };
                compare_cell(&row[idx], &cond.op, &cond.value)
            })
        })
        .cloned()
        .collect()
}

/// Apply OFFSET and LIMIT to a row set.
fn apply_offset_limit(
    rows: Vec<Vec<Option<String>>>,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Vec<Vec<Option<String>>> {
    let start = offset.unwrap_or(0) as usize;
    let rows = if start >= rows.len() { vec![] } else { rows[start..].to_vec() };
    match limit {
        Some(n) => rows.into_iter().take(n as usize).collect(),
        None => rows,
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test catalog stub ──────────────────────────────────────────

    struct StubCatalog {
        projections: Vec<ProjectionInfo>,
    }

    impl StubCatalog {
        fn empty() -> Self {
            Self { projections: vec![] }
        }

        fn with_people() -> Self {
            Self {
                projections: vec![ProjectionInfo {
                    name: "people".to_string(),
                    columns: vec![
                        CatalogColumn {
                            name: "name".to_string(),
                            type_oid: crate::type_mapping::PG_OID_TEXT,
                            type_size: -1,
                        },
                        CatalogColumn {
                            name: "age".to_string(),
                            type_oid: crate::type_mapping::PG_OID_INT8,
                            type_size: 8,
                        },
                        CatalogColumn {
                            name: "city".to_string(),
                            type_oid: crate::type_mapping::PG_OID_TEXT,
                            type_size: -1,
                        },
                    ],
                    rows: vec![
                        vec![Some("Alice".into()), Some("30".into()), Some("London".into())],
                        vec![Some("Bob".into()), Some("25".into()), Some("Paris".into())],
                        vec![Some("Charlie".into()), Some("35".into()), Some("London".into())],
                        vec![Some("Diana".into()), Some("28".into()), Some("Berlin".into())],
                        vec![Some("Eve".into()), Some("30".into()), None],
                    ],
                }],
            }
        }
    }

    impl ProjectionCatalog for StubCatalog {
        fn list_projections(&self) -> Vec<String> {
            self.projections.iter().map(|p| p.name.clone()).collect()
        }

        fn get_projection(&self, name: &str) -> Option<ProjectionInfo> {
            self.projections.iter().find(|p| p.name.eq_ignore_ascii_case(name)).cloned()
        }
    }

    // Helper: extract result or panic.
    fn unwrap_result(outcome: QueryOutcome) -> QueryResult {
        match outcome {
            QueryOutcome::Result(r) => r,
            QueryOutcome::Error { message, .. } => panic!("expected Result, got Error: {message}"),
        }
    }

    // Helper: extract error or panic.
    fn unwrap_error(outcome: QueryOutcome) -> (String, String, String) {
        match outcome {
            QueryOutcome::Error { severity, code, message } => (severity, code, message),
            QueryOutcome::Result(_) => panic!("expected Error, got Result"),
        }
    }

    // ── SHOW TABLES ────────────────────────────────────────────────

    #[test]
    fn test_show_tables_empty_catalog() {
        let catalog = StubCatalog::empty();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::ShowTables));
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "table_name");
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_show_tables_with_projections() {
        let catalog = StubCatalog::with_people();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::ShowTables));
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "table_name");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0], vec![Some("people".to_string())]);
    }

    // ── SELECT * ───────────────────────────────────────────────────

    #[test]
    fn test_select_star_from_projection() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[1].name, "age");
        assert_eq!(result.columns[2].name, "city");
        assert_eq!(result.rows.len(), 5);
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
    }

    #[test]
    fn test_select_star_column_oids() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns[0].type_oid, crate::type_mapping::PG_OID_TEXT);
        assert_eq!(result.columns[1].type_oid, crate::type_mapping::PG_OID_INT8);
        assert_eq!(result.columns[2].type_oid, crate::type_mapping::PG_OID_TEXT);
    }

    // ── Named columns ──────────────────────────────────────────────

    #[test]
    fn test_select_named_columns() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![
                SelectColumn::Named { name: "name".into(), alias: None },
                SelectColumn::Named { name: "city".into(), alias: None },
            ],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[1].name, "city");
        // Rows should only have 2 values each.
        assert_eq!(result.rows[0].len(), 2);
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
        assert_eq!(result.rows[0][1], Some("London".to_string()));
    }

    #[test]
    fn test_select_column_with_alias() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Named { name: "name".into(), alias: Some("full_name".into()) }],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // The column name in RowDescription should be the alias.
        assert_eq!(result.columns[0].name, "full_name");
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
    }

    #[test]
    fn test_select_unknown_column_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Named { name: "nonexistent".into(), alias: None }],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let (severity, code, message) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42703"); // undefined_column
        assert!(message.contains("nonexistent"), "message should mention column: {message}");
    }

    // ── Table not found ────────────────────────────────────────────

    #[test]
    fn test_select_unknown_table_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "nonexistent".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let (severity, code, message) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42P01"); // undefined_table
        assert!(message.contains("nonexistent"), "message should mention table: {message}");
    }

    // ── WHERE filtering ────────────────────────────────────────────

    #[test]
    fn test_where_eq_string() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "city".into(),
                op: ComparisonOp::Eq,
                value: LiteralValue::String("London".into()),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
        assert_eq!(result.rows[1][0], Some("Charlie".to_string()));
    }

    #[test]
    fn test_where_eq_integer() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "age".into(),
                op: ComparisonOp::Eq,
                value: LiteralValue::Integer(30),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 2); // Alice (30) and Eve (30)
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
        assert_eq!(result.rows[1][0], Some("Eve".to_string()));
    }

    #[test]
    fn test_where_gt_integer() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "age".into(),
                op: ComparisonOp::Gt,
                value: LiteralValue::Integer(29),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 3); // Alice (30), Charlie (35), Eve (30)
    }

    #[test]
    fn test_where_lt_integer() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "age".into(),
                op: ComparisonOp::Lt,
                value: LiteralValue::Integer(28),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 1); // Bob (25)
        assert_eq!(result.rows[0][0], Some("Bob".to_string()));
    }

    #[test]
    fn test_where_not_eq() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "city".into(),
                op: ComparisonOp::NotEq,
                value: LiteralValue::String("London".into()),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // Paris (Bob), Berlin (Diana). Eve has NULL city → excluded by NotEq.
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_where_null_handling() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "city".into(),
                op: ComparisonOp::Eq,
                value: LiteralValue::Null,
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 1); // Eve has NULL city
        assert_eq!(result.rows[0][0], Some("Eve".to_string()));
    }

    #[test]
    fn test_where_multiple_conditions_and() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![
                WhereCondition {
                    column: "city".into(),
                    op: ComparisonOp::Eq,
                    value: LiteralValue::String("London".into()),
                },
                WhereCondition { column: "age".into(), op: ComparisonOp::Gt, value: LiteralValue::Integer(31) },
            ],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 1); // Charlie (35, London)
        assert_eq!(result.rows[0][0], Some("Charlie".to_string()));
    }

    #[test]
    fn test_where_unknown_column_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "nonexistent".into(),
                op: ComparisonOp::Eq,
                value: LiteralValue::Integer(1),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let (severity, code, _) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42703"); // undefined_column
    }

    // ── ORDER BY ───────────────────────────────────────────────────

    #[test]
    fn test_order_by_string_asc() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "name".into(), direction: SortDirection::Asc }],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        let names: Vec<_> = result.rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
        assert_eq!(names, vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
    }

    #[test]
    fn test_order_by_string_desc() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "name".into(), direction: SortDirection::Desc }],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        let names: Vec<_> = result.rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
        assert_eq!(names, vec!["Eve", "Diana", "Charlie", "Bob", "Alice"]);
    }

    #[test]
    fn test_order_by_integer_asc() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "age".into(), direction: SortDirection::Asc }],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        let ages: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert_eq!(ages, vec!["25", "28", "30", "30", "35"]);
    }

    #[test]
    fn test_order_by_nulls_last() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "city".into(), direction: SortDirection::Asc }],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // NULLs sort last in Postgres ASC.
        let cities: Vec<_> = result.rows.iter().map(|r| r[2].as_deref()).collect();
        assert_eq!(cities, vec![Some("Berlin"), Some("London"), Some("London"), Some("Paris"), None]);
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![
                OrderByExpr { column: "age".into(), direction: SortDirection::Asc },
                OrderByExpr { column: "name".into(), direction: SortDirection::Desc },
            ],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        let name_age: Vec<_> =
            result.rows.iter().map(|r| (r[0].as_deref().unwrap(), r[1].as_deref().unwrap())).collect();
        // age ASC: 25, 28, 30, 30, 35; within age=30: name DESC → Eve, Alice.
        assert_eq!(name_age, vec![("Bob", "25"), ("Diana", "28"), ("Eve", "30"), ("Alice", "30"), ("Charlie", "35")]);
    }

    #[test]
    fn test_order_by_unknown_column_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "nonexistent".into(), direction: SortDirection::Asc }],
            limit: None,
            offset: None,
        };
        let (severity, code, _) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42703");
    }

    // ── LIMIT / OFFSET ─────────────────────────────────────────────

    #[test]
    fn test_limit() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: Some(2),
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_offset() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: Some(3),
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 2); // 5 rows, skip 3 → 2 left
    }

    #[test]
    fn test_limit_and_offset() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![OrderByExpr { column: "name".into(), direction: SortDirection::Asc }],
            limit: Some(2),
            offset: Some(1),
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 2);
        // Sorted: Alice, Bob, Charlie, Diana, Eve → skip 1 → Bob, Charlie
        assert_eq!(result.rows[0][0], Some("Bob".to_string()));
        assert_eq!(result.rows[1][0], Some("Charlie".to_string()));
    }

    #[test]
    fn test_limit_exceeds_rows() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: Some(100),
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 5);
    }

    #[test]
    fn test_offset_exceeds_rows() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: Some(100),
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 0);
    }

    // ── information_schema.tables ──────────────────────────────────

    #[test]
    fn test_information_schema_tables() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "tables".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // Must include standard columns: table_catalog, table_schema, table_name, table_type
        let col_names: Vec<_> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"table_catalog"), "missing table_catalog: {col_names:?}");
        assert!(col_names.contains(&"table_schema"), "missing table_schema: {col_names:?}");
        assert!(col_names.contains(&"table_name"), "missing table_name: {col_names:?}");
        assert!(col_names.contains(&"table_type"), "missing table_type: {col_names:?}");
        // Should have one row for the "people" projection.
        assert_eq!(result.rows.len(), 1);
        // table_name should be "people".
        let name_idx = col_names.iter().position(|&n| n == "table_name").unwrap();
        assert_eq!(result.rows[0][name_idx], Some("people".to_string()));
    }

    // ── information_schema.columns ─────────────────────────────────

    #[test]
    fn test_information_schema_columns() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "columns".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        let col_names: Vec<_> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"table_name"), "missing table_name: {col_names:?}");
        assert!(col_names.contains(&"column_name"), "missing column_name: {col_names:?}");
        assert!(col_names.contains(&"data_type"), "missing data_type: {col_names:?}");
        assert!(col_names.contains(&"ordinal_position"), "missing ordinal_position: {col_names:?}");
        // 3 columns for "people" projection → 3 rows.
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_information_schema_columns_with_where() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "columns".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "table_name".into(),
                op: ComparisonOp::Eq,
                value: LiteralValue::String("people".into()),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 3);
    }

    // ── Combined: WHERE + ORDER BY + LIMIT + named columns ────────

    #[test]
    fn test_full_query_pipeline() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![
                SelectColumn::Named { name: "name".into(), alias: None },
                SelectColumn::Named { name: "age".into(), alias: None },
            ],
            where_clause: vec![WhereCondition {
                column: "age".into(),
                op: ComparisonOp::GtEq,
                value: LiteralValue::Integer(28),
            }],
            order_by: vec![OrderByExpr { column: "age".into(), direction: SortDirection::Desc }],
            limit: Some(2),
            offset: Some(1),
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // Matching: Alice(30), Charlie(35), Diana(28), Eve(30)
        // Sorted DESC: Charlie(35), Alice(30), Eve(30), Diana(28)
        // Offset 1: Alice(30), Eve(30), Diana(28)
        // Limit 2: Alice(30), Eve(30)
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows[0], vec![Some("Alice".to_string()), Some("30".to_string())]);
        assert_eq!(result.rows[1], vec![Some("Eve".to_string()), Some("30".to_string())]);
    }

    // ── Schema-qualified unknown table ─────────────────────────────

    #[test]
    fn test_unknown_schema_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "stuff".into(),
            schema: Some("bogus_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let (severity, code, _) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42P01"); // undefined_table
    }

    // ── Case insensitivity ─────────────────────────────────────────

    #[test]
    fn test_table_name_case_insensitive() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "PEOPLE".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 5);
    }

    // ── WHERE with float comparison ────────────────────────────────

    #[test]
    fn test_where_lteq_with_float() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "people".into(),
            schema: None,
            columns: vec![SelectColumn::Star],
            where_clause: vec![WhereCondition {
                column: "age".into(),
                op: ComparisonOp::LtEq,
                value: LiteralValue::Float(28.5),
            }],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // Bob (25) and Diana (28) have age <= 28.5
        assert_eq!(result.rows.len(), 2);
    }
}
