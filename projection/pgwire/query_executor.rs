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
                if s.eq_ignore_ascii_case("pg_catalog") {
                    return execute_pg_catalog(catalog, table, columns, where_clause, order_by, *limit, *offset);
                }
                // Unknown schema → treat as regular table lookup (will fail if not found).
            }
            execute_select(catalog, table, columns, where_clause, order_by, *limit, *offset)
        }
        // No-op statements that BI tools send during connection setup.
        ParsedQuery::Set { .. } => command_complete("SET"),
        ParsedQuery::Begin => command_complete("BEGIN"),
        ParsedQuery::Commit => command_complete("COMMIT"),
        ParsedQuery::Rollback => command_complete("ROLLBACK"),
        ParsedQuery::Deallocate { .. } => command_complete("DEALLOCATE"),
        ParsedQuery::DiscardAll => command_complete("DISCARD ALL"),
        ParsedQuery::SelectExpression { expressions, aliases } => execute_select_expression(expressions, aliases),
    }
}

/// Build a CommandComplete-style outcome with no rows.
fn command_complete(tag: &str) -> QueryOutcome {
    QueryOutcome::Result(QueryResult { columns: vec![], rows: vec![], command_tag: Some(tag.to_string()) })
}

// ── SELECT expression (no FROM) ───────────────────────────────────

/// Handle `SELECT <expr>, ...` with no FROM clause.
///
/// Evaluates well-known functions (current_database, current_schema, version,
/// current_user, pg_backend_pid, etc.) and literal values.
fn execute_select_expression(expressions: &[String], aliases: &[Option<String>]) -> QueryOutcome {
    let mut columns = Vec::new();
    let mut values = Vec::new();

    for (i, expr) in expressions.iter().enumerate() {
        let lower = expr.to_ascii_lowercase();
        let (col_name, value) = evaluate_expression(&lower, expr);

        let name = aliases.get(i).and_then(|a| a.clone()).unwrap_or_else(|| col_name.unwrap_or_else(|| expr.clone()));

        columns.push(ColumnDescription {
            name,
            table_oid: 0,
            column_index: 0,
            type_oid: crate::type_mapping::PG_OID_TEXT,
            type_size: -1,
            type_modifier: -1,
            format_code: 0,
        });
        values.push(value);
    }

    QueryOutcome::Result(QueryResult { columns, rows: vec![values], command_tag: None })
}

/// Evaluate a single expression. Returns (optional default column name, value).
fn evaluate_expression(lower: &str, _original: &str) -> (Option<String>, Option<String>) {
    // Well-known functions.
    match lower {
        "current_database()" => (Some("current_database".into()), Some("typedb".into())),
        "current_schema()" | "current_schema" => (Some("current_schema".into()), Some("public".into())),
        "current_user" | "session_user" | "user" => (Some("current_user".into()), Some("typedb".into())),
        "version()" => {
            (Some("version".into()), Some("TypeDB (PostgreSQL-compatible) via pgwire projection facade".into()))
        }
        "pg_backend_pid()" => (Some("pg_backend_pid".into()), Some("1".into())),
        "inet_server_addr()" => (Some("inet_server_addr".into()), None),
        "inet_server_port()" => (Some("inet_server_port".into()), Some("5432".into())),
        "pg_is_in_recovery()" => (Some("pg_is_in_recovery".into()), Some("f".into())),
        "txid_current()" => (Some("txid_current".into()), Some("1".into())),
        _ => {
            // Try to evaluate as a simple integer/string literal.
            if let Ok(n) = lower.parse::<i64>() {
                (None, Some(n.to_string()))
            } else if lower.starts_with('\'') && lower.ends_with('\'') && lower.len() >= 2 {
                let inner = &lower[1..lower.len() - 1];
                (None, Some(inner.to_string()))
            } else {
                // Unknown expression — return the expression text as-is.
                (None, Some(lower.to_string()))
            }
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
    QueryOutcome::Result(QueryResult { columns, rows, command_tag: None })
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

    QueryOutcome::Result(QueryResult { columns: out_columns, rows: projected_rows, command_tag: None })
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
    } else if table.eq_ignore_ascii_case("schemata") {
        execute_info_schema_schemata(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("key_column_usage") || table.eq_ignore_ascii_case("table_constraints") {
        // ORMs query these for FK/PK discovery — we have none, return empty.
        execute_info_schema_empty_virtual(table, limit, offset)
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

    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
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

    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

fn execute_info_schema_schemata(
    where_clause: &[WhereCondition],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    let columns = vec![
        info_schema_text_col("catalog_name"),
        info_schema_text_col("schema_name"),
        info_schema_text_col("schema_owner"),
    ];

    let all_rows: Vec<Vec<Option<String>>> = vec![
        vec![Some("typedb".into()), Some("public".into()), Some("typedb".into())],
        vec![Some("typedb".into()), Some("information_schema".into()), Some("typedb".into())],
        vec![Some("typedb".into()), Some("pg_catalog".into()), Some("typedb".into())],
    ];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);

    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

// ── pg_catalog ─────────────────────────────────────────────────────

fn execute_pg_catalog(
    catalog: &dyn ProjectionCatalog,
    table: &str,
    _columns: &[SelectColumn],
    where_clause: &[WhereCondition],
    _order_by: &[OrderByExpr],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    if table.eq_ignore_ascii_case("pg_namespace") {
        execute_pg_namespace(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_class") {
        execute_pg_class(catalog, where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_attribute") {
        execute_pg_attribute(catalog, where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_type") {
        execute_pg_type(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_database") {
        execute_pg_database(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_constraint") {
        execute_pg_empty_virtual(
            "pg_constraint",
            &["oid", "conname", "connamespace", "contype", "conrelid"],
            limit,
            offset,
        )
    } else if table.eq_ignore_ascii_case("pg_index") {
        execute_pg_empty_virtual(
            "pg_index",
            &["indexrelid", "indrelid", "indisunique", "indisprimary", "indisvalid"],
            limit,
            offset,
        )
    } else if table.eq_ignore_ascii_case("pg_description") {
        execute_pg_empty_virtual("pg_description", &["objoid", "classoid", "objsubid", "description"], limit, offset)
    } else if table.eq_ignore_ascii_case("pg_am") {
        execute_pg_am(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_settings") {
        execute_pg_settings(where_clause, limit, offset)
    } else if table.eq_ignore_ascii_case("pg_stat_activity") {
        execute_pg_empty_virtual(
            "pg_stat_activity",
            &["datid", "datname", "pid", "usename", "application_name", "state"],
            limit,
            offset,
        )
    } else {
        QueryOutcome::Error {
            severity: "ERROR".into(),
            code: "42P01".into(),
            message: format!("relation \"pg_catalog.{table}\" does not exist"),
        }
    }
}

fn pg_catalog_int4_col(name: &str) -> ColumnDescription {
    ColumnDescription {
        name: name.to_string(),
        table_oid: 0,
        column_index: 0,
        type_oid: crate::type_mapping::PG_OID_INT4,
        type_size: 4,
        type_modifier: -1,
        format_code: 0,
    }
}

fn pg_catalog_text_col(name: &str) -> ColumnDescription {
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

fn pg_catalog_bool_col(name: &str) -> ColumnDescription {
    ColumnDescription {
        name: name.to_string(),
        table_oid: 0,
        column_index: 0,
        type_oid: crate::type_mapping::PG_OID_BOOL,
        type_size: 1,
        type_modifier: -1,
        format_code: 0,
    }
}

fn pg_catalog_oid_col(name: &str) -> ColumnDescription {
    ColumnDescription {
        name: name.to_string(),
        table_oid: 0,
        column_index: 0,
        type_oid: crate::type_mapping::PG_OID_OID,
        type_size: 4,
        type_modifier: -1,
        format_code: 0,
    }
}

/// pg_namespace: oid, nspname, nspowner
fn execute_pg_namespace(where_clause: &[WhereCondition], limit: Option<u64>, offset: Option<u64>) -> QueryOutcome {
    let columns = vec![pg_catalog_oid_col("oid"), pg_catalog_text_col("nspname"), pg_catalog_oid_col("nspowner")];

    let all_rows: Vec<Vec<Option<String>>> = vec![
        vec![Some("11".into()), Some("pg_catalog".into()), Some("10".into())],
        vec![Some("2200".into()), Some("public".into()), Some("10".into())],
        vec![Some("13000".into()), Some("information_schema".into()), Some("10".into())],
    ];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// pg_class: oid, relname, relnamespace, relkind, relowner, reltuples, relhasindex, relhasrules, relhastriggers
fn execute_pg_class(
    catalog: &dyn ProjectionCatalog,
    where_clause: &[WhereCondition],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    let columns = vec![
        pg_catalog_oid_col("oid"),
        pg_catalog_text_col("relname"),
        pg_catalog_oid_col("relnamespace"),
        pg_catalog_text_col("relkind"),
        pg_catalog_oid_col("relowner"),
        pg_catalog_text_col("reltuples"),
        pg_catalog_bool_col("relhasindex"),
        pg_catalog_bool_col("relhasrules"),
        pg_catalog_bool_col("relhastriggers"),
    ];

    let names = catalog.list_projections();
    let all_rows: Vec<Vec<Option<String>>> = names
        .into_iter()
        .enumerate()
        .map(|(i, n)| {
            vec![
                Some(format!("{}", 16384 + i)), // synthetic oid
                Some(n),                        // relname
                Some("2200".into()),            // relnamespace = public
                Some("r".into()),               // relkind = ordinary table
                Some("10".into()),              // relowner
                Some("-1".into()),              // reltuples (unknown)
                Some("f".into()),               // relhasindex
                Some("f".into()),               // relhasrules
                Some("f".into()),               // relhastriggers
            ]
        })
        .collect();

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// pg_attribute: attrelid, attname, atttypid, attnum, attnotnull, attlen, atttypmod
fn execute_pg_attribute(
    catalog: &dyn ProjectionCatalog,
    where_clause: &[WhereCondition],
    limit: Option<u64>,
    offset: Option<u64>,
) -> QueryOutcome {
    let columns = vec![
        pg_catalog_oid_col("attrelid"),
        pg_catalog_text_col("attname"),
        pg_catalog_oid_col("atttypid"),
        pg_catalog_int4_col("attnum"),
        pg_catalog_bool_col("attnotnull"),
        pg_catalog_int4_col("attlen"),
        pg_catalog_int4_col("atttypmod"),
    ];

    let projections = catalog.list_projections();
    let mut all_rows: Vec<Vec<Option<String>>> = Vec::new();
    for (pi, proj_name) in projections.iter().enumerate() {
        if let Some(proj) = catalog.get_projection(proj_name) {
            let rel_oid = 16384 + pi;
            for (ci, col) in proj.columns.iter().enumerate() {
                all_rows.push(vec![
                    Some(format!("{rel_oid}")),
                    Some(col.name.clone()),
                    Some(format!("{}", col.type_oid)),
                    Some(format!("{}", ci + 1)),
                    Some("f".into()),
                    Some(format!("{}", col.type_size)),
                    Some("-1".into()),
                ]);
            }
        }
    }

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// pg_type: oid, typname, typnamespace, typlen, typtype, typbasetype
fn execute_pg_type(where_clause: &[WhereCondition], limit: Option<u64>, offset: Option<u64>) -> QueryOutcome {
    let columns = vec![
        pg_catalog_oid_col("oid"),
        pg_catalog_text_col("typname"),
        pg_catalog_oid_col("typnamespace"),
        pg_catalog_int4_col("typlen"),
        pg_catalog_text_col("typtype"),
        pg_catalog_oid_col("typbasetype"),
    ];

    // Minimal set of types for BI tools.
    let all_rows: Vec<Vec<Option<String>>> = vec![
        vec![s("16"), s("bool"), s("11"), s("1"), s("b"), s("0")],
        vec![s("20"), s("int8"), s("11"), s("8"), s("b"), s("0")],
        vec![s("21"), s("int2"), s("11"), s("2"), s("b"), s("0")],
        vec![s("23"), s("int4"), s("11"), s("4"), s("b"), s("0")],
        vec![s("25"), s("text"), s("11"), s("-1"), s("b"), s("0")],
        vec![s("700"), s("float4"), s("11"), s("4"), s("b"), s("0")],
        vec![s("701"), s("float8"), s("11"), s("8"), s("b"), s("0")],
        vec![s("1043"), s("varchar"), s("11"), s("-1"), s("b"), s("0")],
        vec![s("1114"), s("timestamp"), s("11"), s("8"), s("b"), s("0")],
        vec![s("1184"), s("timestamptz"), s("11"), s("8"), s("b"), s("0")],
        vec![s("1700"), s("numeric"), s("11"), s("-1"), s("b"), s("0")],
        vec![s("2950"), s("uuid"), s("11"), s("16"), s("b"), s("0")],
    ];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

/// pg_database: oid, datname, datdba, encoding, datcollate, datctype
fn execute_pg_database(where_clause: &[WhereCondition], limit: Option<u64>, offset: Option<u64>) -> QueryOutcome {
    let columns = vec![
        pg_catalog_oid_col("oid"),
        pg_catalog_text_col("datname"),
        pg_catalog_oid_col("datdba"),
        pg_catalog_int4_col("encoding"),
        pg_catalog_text_col("datcollate"),
        pg_catalog_text_col("datctype"),
    ];

    let all_rows: Vec<Vec<Option<String>>> = vec![vec![
        s("16384"),
        s("typedb"),
        s("10"),
        s("6"), // UTF-8
        s("en_US.UTF-8"),
        s("en_US.UTF-8"),
    ]];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// Generic empty virtual table — returns the named columns with zero rows.
/// Used for pg_constraint, pg_index, pg_description, pg_stat_activity where we have no data
/// but ORMs need the table to exist.
fn execute_pg_empty_virtual(
    _table: &str,
    col_names: &[&str],
    _limit: Option<u64>,
    _offset: Option<u64>,
) -> QueryOutcome {
    let columns: Vec<ColumnDescription> = col_names.iter().map(|n| pg_catalog_text_col(n)).collect();
    QueryOutcome::Result(QueryResult { columns, rows: vec![], command_tag: None })
}

/// pg_am: oid, amname, amhandler, amtype
fn execute_pg_am(where_clause: &[WhereCondition], limit: Option<u64>, offset: Option<u64>) -> QueryOutcome {
    let columns = vec![
        pg_catalog_oid_col("oid"),
        pg_catalog_text_col("amname"),
        pg_catalog_text_col("amhandler"),
        pg_catalog_text_col("amtype"),
    ];

    let all_rows: Vec<Vec<Option<String>>> = vec![
        vec![s("2"), s("heap"), s("heap_tableam_handler"), s("t")],
        vec![s("403"), s("btree"), s("bthandler"), s("i")],
        vec![s("405"), s("hash"), s("hashhandler"), s("i")],
    ];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// pg_settings: server variables exposed as a virtual table (for `SHOW` and BI tool introspection).
fn execute_pg_settings(where_clause: &[WhereCondition], limit: Option<u64>, offset: Option<u64>) -> QueryOutcome {
    let columns = vec![
        pg_catalog_text_col("name"),
        pg_catalog_text_col("setting"),
        pg_catalog_text_col("unit"),
        pg_catalog_text_col("category"),
        pg_catalog_text_col("short_desc"),
    ];

    let all_rows: Vec<Vec<Option<String>>> = vec![
        vec![s("server_version"), s("16.0"), None, s("Preset Options"), s("Shows the server version.")],
        vec![s("server_encoding"), s("UTF8"), None, s("Client Connection Defaults"), s("Shows the server encoding.")],
        vec![s("client_encoding"), s("UTF8"), None, s("Client Connection Defaults"), s("Sets the client encoding.")],
        vec![
            s("standard_conforming_strings"),
            s("on"),
            None,
            s("Version and Platform Compatibility"),
            s("Causes ... strings to treat backslashes literally."),
        ],
        vec![s("DateStyle"), s("ISO, MDY"), None, s("Client Connection Defaults"), s("Sets the display format.")],
        vec![s("TimeZone"), s("UTC"), None, s("Client Connection Defaults"), s("Sets the time zone.")],
        vec![
            s("max_connections"),
            s("100"),
            None,
            s("Connections and Authentication"),
            s("Sets the maximum number of concurrent connections."),
        ],
        vec![
            s("search_path"),
            s("\"$user\", public"),
            None,
            s("Client Connection Defaults"),
            s("Sets the schema search path."),
        ],
    ];

    let filtered = apply_where_to_virtual_rows(&columns, &all_rows, where_clause);
    let paged = apply_offset_limit(filtered, offset, limit);
    QueryOutcome::Result(QueryResult { columns, rows: paged, command_tag: None })
}

/// Empty information_schema virtual tables (key_column_usage, table_constraints).
fn execute_info_schema_empty_virtual(table: &str, _limit: Option<u64>, _offset: Option<u64>) -> QueryOutcome {
    let col_names = match table.to_ascii_lowercase().as_str() {
        "key_column_usage" => vec![
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
        ],
        "table_constraints" => vec![
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_type",
            "is_deferrable",
            "initially_deferred",
        ],
        _ => vec!["placeholder"],
    };
    let columns: Vec<ColumnDescription> = col_names.iter().map(|n| info_schema_text_col(n)).collect();
    QueryOutcome::Result(QueryResult { columns, rows: vec![], command_tag: None })
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

    // ── No-op commands (SET, BEGIN, COMMIT, ROLLBACK, DEALLOCATE, DISCARD) ──

    #[test]
    fn test_set_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Set { name: "client_encoding".into(), value: "UTF8".into() };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert_eq!(result.command_tag, Some("SET".to_string()));
    }

    #[test]
    fn test_begin_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::Begin));
        assert!(result.columns.is_empty());
        assert_eq!(result.command_tag, Some("BEGIN".to_string()));
    }

    #[test]
    fn test_commit_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::Commit));
        assert_eq!(result.command_tag, Some("COMMIT".to_string()));
    }

    #[test]
    fn test_rollback_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::Rollback));
        assert_eq!(result.command_tag, Some("ROLLBACK".to_string()));
    }

    #[test]
    fn test_deallocate_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Deallocate { name: Some("stmt_1".into()) };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.command_tag, Some("DEALLOCATE".to_string()));
    }

    #[test]
    fn test_discard_all_returns_command_complete() {
        let catalog = StubCatalog::with_people();
        let result = unwrap_result(execute_query(&catalog, &ParsedQuery::DiscardAll));
        assert_eq!(result.command_tag, Some("DISCARD ALL".to_string()));
    }

    // ── information_schema.schemata ────────────────────────────────

    #[test]
    fn test_info_schema_schemata_returns_three_schemas() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "schemata".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.rows.len(), 3);
        let schema_names: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert!(schema_names.contains(&"public"));
        assert!(schema_names.contains(&"information_schema"));
        assert!(schema_names.contains(&"pg_catalog"));
    }

    // ── pg_catalog.pg_namespace ────────────────────────────────────

    #[test]
    fn test_pg_namespace_returns_three_namespaces() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_namespace".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.rows.len(), 3);
        let names: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert!(names.contains(&"pg_catalog"));
        assert!(names.contains(&"public"));
        assert!(names.contains(&"information_schema"));
    }

    // ── pg_catalog.pg_class ────────────────────────────────────────

    #[test]
    fn test_pg_class_lists_projections() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_class".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 1); // only "people" projection
        assert_eq!(result.rows[0][1], Some("people".to_string())); // relname
        assert_eq!(result.rows[0][3], Some("r".to_string())); // relkind
    }

    // ── pg_catalog.pg_attribute ────────────────────────────────────

    #[test]
    fn test_pg_attribute_lists_columns() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_attribute".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        // people has 3 columns: name, age, city
        assert_eq!(result.rows.len(), 3);
        let col_names: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"age"));
        assert!(col_names.contains(&"city"));
    }

    // ── pg_catalog.pg_type ─────────────────────────────────────────

    #[test]
    fn test_pg_type_contains_common_types() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_type".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.len() >= 10, "Should have at least 10 common types");
        let type_names: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert!(type_names.contains(&"int4"));
        assert!(type_names.contains(&"text"));
        assert!(type_names.contains(&"bool"));
        assert!(type_names.contains(&"float8"));
    }

    // ── pg_catalog.pg_database ─────────────────────────────────────

    #[test]
    fn test_pg_database_returns_typedb() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_database".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("typedb".to_string())); // datname
    }

    // ── pg_catalog unknown table ───────────────────────────────────

    #[test]
    fn test_pg_catalog_unknown_table_error() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_nonexistent".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let (severity, code, msg) = unwrap_error(execute_query(&catalog, &query));
        assert_eq!(severity, "ERROR");
        assert_eq!(code, "42P01");
        assert!(msg.contains("pg_nonexistent"));
    }

    // ── command_tag on regular SELECT is None ──────────────────────

    #[test]
    fn test_regular_select_has_no_command_tag() {
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
        assert_eq!(result.command_tag, None);
    }

    // ── SELECT expression (no FROM) ───────────────────────────────

    #[test]
    fn test_select_current_database() {
        let catalog = StubCatalog::with_people();
        let query =
            ParsedQuery::SelectExpression { expressions: vec!["current_database()".into()], aliases: vec![None] };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "current_database");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("typedb".to_string()));
    }

    #[test]
    fn test_select_version() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::SelectExpression { expressions: vec!["version()".into()], aliases: vec![None] };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns[0].name, "version");
        assert!(result.rows[0][0].as_ref().unwrap().contains("TypeDB"));
    }

    #[test]
    fn test_select_current_schema() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::SelectExpression { expressions: vec!["current_schema()".into()], aliases: vec![None] };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows[0][0], Some("public".to_string()));
    }

    #[test]
    fn test_select_literal_integer() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::SelectExpression { expressions: vec!["1".into()], aliases: vec![Some("one".into())] };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns[0].name, "one");
        assert_eq!(result.rows[0][0], Some("1".to_string()));
    }

    #[test]
    fn test_select_multiple_expressions() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::SelectExpression {
            expressions: vec!["current_database()".into(), "current_user".into()],
            aliases: vec![None, Some("me".into())],
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[1].name, "me");
        assert_eq!(result.rows[0][0], Some("typedb".to_string()));
        assert_eq!(result.rows[0][1], Some("typedb".to_string()));
    }

    // ── pg_catalog ORM compat (empty virtual tables) ───────────────

    #[test]
    fn test_pg_constraint_returns_empty() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_constraint".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.is_empty());
        assert!(!result.columns.is_empty());
    }

    #[test]
    fn test_pg_index_returns_empty() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_index".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_pg_am_returns_access_methods() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_am".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert_eq!(result.rows.len(), 3);
        let names: Vec<_> = result.rows.iter().map(|r| r[1].as_deref().unwrap()).collect();
        assert!(names.contains(&"heap"));
        assert!(names.contains(&"btree"));
    }

    #[test]
    fn test_pg_settings_has_common_variables() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "pg_settings".into(),
            schema: Some("pg_catalog".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.len() >= 5);
        let names: Vec<_> = result.rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
        assert!(names.contains(&"server_version"));
        assert!(names.contains(&"search_path"));
    }

    // ── information_schema ORM compat (empty virtual tables) ───────

    #[test]
    fn test_info_schema_key_column_usage_returns_empty() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "key_column_usage".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.is_empty());
        assert!(result.columns.len() >= 6);
    }

    #[test]
    fn test_info_schema_table_constraints_returns_empty() {
        let catalog = StubCatalog::with_people();
        let query = ParsedQuery::Select {
            table: "table_constraints".into(),
            schema: Some("information_schema".into()),
            columns: vec![SelectColumn::Star],
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let result = unwrap_result(execute_query(&catalog, &query));
        assert!(result.rows.is_empty());
        assert!(result.columns.len() >= 6);
    }
}
