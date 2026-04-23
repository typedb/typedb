/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use sqlparser::{
    ast::{
        self, BinaryOperator, Expr, Ident, LimitClause, ObjectNamePart, OrderByKind, SelectItem, SetExpr, Statement,
        TableFactor, Value,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

/// A column reference in a SELECT list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    /// `*` — all columns
    Star,
    /// A named column, optionally with an alias.
    Named { name: String, alias: Option<String> },
    /// A `NULL` projection, optionally with an alias.
    Null { alias: Option<String> },
}

/// Comparison operators supported in WHERE clauses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A literal value in a WHERE clause.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

/// A single WHERE condition (only simple `col op literal` for MVP).
#[derive(Debug, Clone, PartialEq)]
pub struct WhereCondition {
    pub column: String,
    pub op: ComparisonOp,
    pub value: LiteralValue,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// A single ORDER BY clause entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByExpr {
    pub column: String,
    pub direction: SortDirection,
}

/// The result of parsing a SQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedQuery {
    /// `SELECT ... FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n]`
    Select {
        table: String,
        schema: Option<String>,
        columns: Vec<SelectColumn>,
        where_clause: Vec<WhereCondition>,
        order_by: Vec<OrderByExpr>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    /// `SHOW TABLES`
    ShowTables,
    /// `SET <name> = <value>` or `SET <name> TO <value>` — acknowledged but ignored.
    Set { name: String, value: String },
    /// `BEGIN` / `START TRANSACTION` — no-op (we have no transactions).
    Begin,
    /// `COMMIT` — no-op.
    Commit,
    /// `ROLLBACK` — no-op.
    Rollback,
    /// `DEALLOCATE <name>` or `DEALLOCATE ALL` — no-op.
    Deallocate { name: Option<String> },
    /// `DISCARD ALL` — no-op (BI tools send this on connection reset).
    DiscardAll,
    /// `SELECT <expr>` without FROM — function calls or literals used by ORMs during setup.
    /// e.g. `SELECT current_database()`, `SELECT version()`, `SELECT 1`.
    SelectExpression {
        /// The raw expressions as strings, e.g. `["current_database()"]`.
        expressions: Vec<String>,
        /// Column aliases if any, e.g. `["current_database"]`.
        aliases: Vec<Option<String>>,
    },
}

/// Errors that can occur while parsing SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParseError {
    /// The SQL string could not be parsed at all.
    SyntaxError(String),
    /// The SQL was valid but uses features we don't support.
    Unsupported(String),
    /// Empty input.
    EmptyQuery,
}

impl fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlParseError::SyntaxError(msg) => write!(f, "SQL syntax error: {msg}"),
            SqlParseError::Unsupported(msg) => write!(f, "Unsupported SQL: {msg}"),
            SqlParseError::EmptyQuery => write!(f, "Empty query"),
        }
    }
}

impl std::error::Error for SqlParseError {}

/// Parse a SQL string into a [`ParsedQuery`].
///
/// Only a minimal subset of SQL is supported — enough for BI tool
/// compatibility (SELECT, SHOW TABLES, information_schema queries).
pub fn parse_sql(sql: &str) -> Result<ParsedQuery, SqlParseError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(SqlParseError::EmptyQuery);
    }

    // Fast-path: SHOW TABLES (not standard PG SQL, handle before parser).
    if trimmed.eq_ignore_ascii_case("SHOW TABLES") || trimmed.eq_ignore_ascii_case("SHOW TABLES;") {
        return Ok(ParsedQuery::ShowTables);
    }

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).map_err(|e| SqlParseError::SyntaxError(e.to_string()))?;

    if statements.is_empty() {
        return Err(SqlParseError::EmptyQuery);
    }
    if statements.len() > 1 {
        return Err(SqlParseError::Unsupported("multiple statements".to_string()));
    }

    convert_statement(statements.into_iter().next().unwrap())
}

/// Parse a SQL string into a batch of [`ParsedQuery`] values.
pub fn parse_sql_batch(sql: &str) -> Result<Vec<ParsedQuery>, SqlParseError> {
    split_sql_statements(sql)?.into_iter().map(|statement| parse_sql(&statement)).collect()
}

pub fn split_sql_statements(sql: &str) -> Result<Vec<String>, SqlParseError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(SqlParseError::EmptyQuery);
    }

    // Fast-path: SHOW TABLES (not standard PG SQL, handle before parser).
    if trimmed.eq_ignore_ascii_case("SHOW TABLES") || trimmed.eq_ignore_ascii_case("SHOW TABLES;") {
        return Ok(vec!["SHOW TABLES".to_string()]);
    }

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed).map_err(|e| SqlParseError::SyntaxError(e.to_string()))?;

    if statements.is_empty() {
        return Err(SqlParseError::EmptyQuery);
    }

    Ok(statements.into_iter().map(|statement| statement.to_string()).collect())
}

fn convert_statement(statement: Statement) -> Result<ParsedQuery, SqlParseError> {
    match statement {
        Statement::Query(query) => convert_query(*query),
        Statement::ShowTables { .. } => Ok(ParsedQuery::ShowTables),
        Statement::Set(set_stmt) => {
            let (name, val) = match set_stmt {
                ast::Set::SingleAssignment { variable, values, .. } => {
                    (variable.to_string(), values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
                }
                other => (format!("{other}"), String::new()),
            };
            Ok(ParsedQuery::Set { name, value: val })
        }
        Statement::StartTransaction { .. } => Ok(ParsedQuery::Begin),
        Statement::Commit { .. } => Ok(ParsedQuery::Commit),
        Statement::Rollback { .. } => Ok(ParsedQuery::Rollback),
        Statement::Deallocate { name, .. } => {
            let n = name.to_string();
            if n.eq_ignore_ascii_case("ALL") {
                Ok(ParsedQuery::Deallocate { name: None })
            } else {
                Ok(ParsedQuery::Deallocate { name: Some(n) })
            }
        }
        Statement::Discard { .. } => Ok(ParsedQuery::DiscardAll),
        other => Err(SqlParseError::Unsupported(statement_kind(&other).to_string())),
    }
}

/// Return a human-readable label for an unsupported statement kind.
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete { .. } => "DELETE",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        _ => "unsupported statement",
    }
}

/// Convert a parsed `Query` AST into our `ParsedQuery::Select`.
fn convert_query(query: ast::Query) -> Result<ParsedQuery, SqlParseError> {
    // Extract LIMIT and OFFSET from the combined limit_clause.
    let (limit, offset) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            let l = match limit {
                Some(expr) => Some(expr_to_u64(expr)?),
                None => None,
            };
            let o = match offset {
                Some(ast::Offset { value, .. }) => Some(expr_to_u64(value)?),
                None => None,
            };
            (l, o)
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            (Some(expr_to_u64(limit)?), Some(expr_to_u64(offset)?))
        }
        None => (None, None),
    };

    // Extract ORDER BY.
    let order_by = match &query.order_by {
        Some(ob) => convert_order_by(ob)?,
        None => vec![],
    };

    // Unwrap the body — must be a simple SELECT.
    let select = match *query.body {
        SetExpr::Select(select) => *select,
        _ => return Err(SqlParseError::Unsupported("non-SELECT query body".to_string())),
    };

    // Handle SELECT without FROM — function calls / literals (e.g. SELECT version()).
    if select.from.is_empty() {
        return convert_select_expression(&select);
    }

    // FROM clause — must be exactly one simple table (no joins, no subqueries).
    let (schema, table) = extract_table_from_select(&select)?;

    // Columns.
    let columns = convert_projection(&select.projection)?;

    // WHERE clause.
    let where_clause = match select.selection {
        Some(expr) => convert_where(expr)?,
        None => vec![],
    };

    Ok(ParsedQuery::Select { table, schema, columns, where_clause, order_by, limit, offset })
}

/// Convert a SELECT without FROM into a `SelectExpression`.
///
/// Handles queries like `SELECT current_database()`, `SELECT version()`, `SELECT 1 AS x`.
fn convert_select_expression(select: &ast::Select) -> Result<ParsedQuery, SqlParseError> {
    let mut expressions = Vec::new();
    let mut aliases = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                expressions.push(expr.to_string());
                aliases.push(None);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                expressions.push(expr.to_string());
                aliases.push(Some(alias.value.clone()));
            }
            other => {
                return Err(SqlParseError::Unsupported(format!("SELECT expression: {other}")));
            }
        }
    }

    Ok(ParsedQuery::SelectExpression { expressions, aliases })
}

/// Extract the single table name (and optional schema) from a SELECT's FROM.
fn extract_table_from_select(select: &ast::Select) -> Result<(Option<String>, String), SqlParseError> {
    let from = &select.from;
    if from.len() != 1 {
        return Err(SqlParseError::Unsupported(if from.is_empty() {
            "SELECT without FROM".to_string()
        } else {
            "multiple FROM tables (implicit join)".to_string()
        }));
    }

    let table_with_joins = &from[0];
    if !table_with_joins.joins.is_empty() {
        return Err(SqlParseError::Unsupported("JOIN".to_string()));
    }

    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => split_object_name(name),
        TableFactor::Derived { .. } => Err(SqlParseError::Unsupported("subquery in FROM".to_string())),
        other => Err(SqlParseError::Unsupported(format!("table factor: {other}"))),
    }
}

/// Split a possibly schema-qualified `ObjectName` into (schema, table).
fn split_object_name(name: &ast::ObjectName) -> Result<(Option<String>, String), SqlParseError> {
    let idents: Vec<&Ident> = name
        .0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident),
            _ => None,
        })
        .collect();
    match idents.len() {
        1 => Ok((None, ident_to_lower(idents[0]))),
        2 => Ok((Some(ident_to_lower(idents[0])), ident_to_lower(idents[1]))),
        _ => Err(SqlParseError::Unsupported(format!("qualified name with {} parts", idents.len()))),
    }
}

fn ident_to_lower(ident: &Ident) -> String {
    // If the identifier was quoted, preserve case; otherwise lowercase.
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn is_null_value_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(val_with_span) if matches!(&val_with_span.value, Value::Null))
}

/// Convert the SELECT projection items.
fn convert_projection(items: &[SelectItem]) -> Result<Vec<SelectColumn>, SqlParseError> {
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard(_) => result.push(SelectColumn::Star),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                result.push(SelectColumn::Named { name: ident_to_lower(ident), alias: None });
            }
            SelectItem::UnnamedExpr(expr) if is_null_value_expr(expr) => {
                result.push(SelectColumn::Null { alias: None });
            }
            SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), alias } => {
                result.push(SelectColumn::Named { name: ident_to_lower(ident), alias: Some(ident_to_lower(alias)) });
            }
            SelectItem::ExprWithAlias { expr, alias } if is_null_value_expr(expr) => {
                result.push(SelectColumn::Null { alias: Some(ident_to_lower(alias)) });
            }
            other => {
                return Err(SqlParseError::Unsupported(format!("projection item: {other}")));
            }
        }
    }
    Ok(result)
}

/// Convert an ORDER BY clause.
fn convert_order_by(order_by: &ast::OrderBy) -> Result<Vec<OrderByExpr>, SqlParseError> {
    let exprs = match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs,
        OrderByKind::All(_) => {
            return Err(SqlParseError::Unsupported("ORDER BY ALL".to_string()));
        }
    };
    let mut result = Vec::new();
    for expr in exprs {
        let column = match &expr.expr {
            Expr::Identifier(ident) => ident_to_lower(ident),
            other => {
                return Err(SqlParseError::Unsupported(format!("ORDER BY expression: {other}")));
            }
        };
        let direction = if expr.options.asc == Some(false) { SortDirection::Desc } else { SortDirection::Asc };
        result.push(OrderByExpr { column, direction });
    }
    Ok(result)
}

/// Convert a WHERE expression into a flat list of AND-ed conditions.
fn convert_where(expr: Expr) -> Result<Vec<WhereCondition>, SqlParseError> {
    let mut conditions = Vec::new();
    flatten_and(expr, &mut conditions)?;
    Ok(conditions)
}

/// Recursively flatten AND expressions into a list of conditions.
fn flatten_and(expr: Expr, out: &mut Vec<WhereCondition>) -> Result<(), SqlParseError> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            flatten_and(*left, out)?;
            flatten_and(*right, out)?;
        }
        Expr::BinaryOp { left: _, op: BinaryOperator::Or, .. } => {
            return Err(SqlParseError::Unsupported("OR in WHERE clause".to_string()));
        }
        Expr::IsNull(inner) => {
            let column = expr_to_column_name(*inner)?;
            out.push(WhereCondition { column, op: ComparisonOp::Eq, value: LiteralValue::Null });
        }
        Expr::BinaryOp { left, op, right } => {
            let column = expr_to_column_name(*left)?;
            let comp_op = convert_binary_op(op)?;
            let value = expr_to_literal(*right)?;
            out.push(WhereCondition { column, op: comp_op, value });
        }
        other => {
            return Err(SqlParseError::Unsupported(format!("WHERE expression: {other}")));
        }
    }
    Ok(())
}

fn expr_to_column_name(expr: Expr) -> Result<String, SqlParseError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident_to_lower(&ident)),
        other => Err(SqlParseError::Unsupported(format!("expected column name, got: {other}"))),
    }
}

fn convert_binary_op(op: BinaryOperator) -> Result<ComparisonOp, SqlParseError> {
    match op {
        BinaryOperator::Eq => Ok(ComparisonOp::Eq),
        BinaryOperator::NotEq => Ok(ComparisonOp::NotEq),
        BinaryOperator::Lt => Ok(ComparisonOp::Lt),
        BinaryOperator::LtEq => Ok(ComparisonOp::LtEq),
        BinaryOperator::Gt => Ok(ComparisonOp::Gt),
        BinaryOperator::GtEq => Ok(ComparisonOp::GtEq),
        other => Err(SqlParseError::Unsupported(format!("operator: {other}"))),
    }
}

fn expr_to_literal(expr: Expr) -> Result<LiteralValue, SqlParseError> {
    match expr {
        Expr::Value(val_with_span) => match val_with_span.value {
            Value::SingleQuotedString(s) => Ok(LiteralValue::String(s)),
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(LiteralValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(LiteralValue::Float(f))
                } else {
                    Err(SqlParseError::Unsupported(format!("numeric literal: {n}")))
                }
            }
            Value::Boolean(b) => Ok(LiteralValue::Boolean(b)),
            Value::Null => Ok(LiteralValue::Null),
            other => Err(SqlParseError::Unsupported(format!("literal: {other}"))),
        },
        other => Err(SqlParseError::Unsupported(format!("expected literal, got: {other}"))),
    }
}

fn expr_to_u64(expr: &Expr) -> Result<u64, SqlParseError> {
    match expr {
        Expr::Value(val_with_span) => match &val_with_span.value {
            Value::Number(n, _) => {
                n.parse::<u64>().map_err(|_| SqlParseError::Unsupported(format!("non-integer limit/offset: {n}")))
            }
            other => Err(SqlParseError::Unsupported(format!("expected integer, got: {other}"))),
        },
        other => Err(SqlParseError::Unsupported(format!("expected integer, got: {other}"))),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── SELECT * ─────────────────────────────────────────────────────

    #[test]
    fn test_select_star_from_table() {
        let result = parse_sql("SELECT * FROM my_projection").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "my_projection".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_star_case_insensitive() {
        let result = parse_sql("select * from My_Projection").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "my_projection".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    // ── Named columns ─────────────────────────────────────────────────

    #[test]
    fn test_select_named_columns() {
        let result = parse_sql("SELECT name, age FROM people").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![
                    SelectColumn::Named { name: "name".to_string(), alias: None },
                    SelectColumn::Named { name: "age".to_string(), alias: None },
                ],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_column_with_alias() {
        let result = parse_sql("SELECT name AS full_name FROM people").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![SelectColumn::Named { name: "name".to_string(), alias: Some("full_name".to_string()) }],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    // ── WHERE clause ────────────────────────────────────────────────

    #[test]
    fn test_select_with_where_eq_string() {
        let result = parse_sql("SELECT * FROM users WHERE city = 'London'").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "city".to_string(),
                    op: ComparisonOp::Eq,
                    value: LiteralValue::String("London".to_string()),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_eq_integer() {
        let result = parse_sql("SELECT * FROM users WHERE age = 30").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "age".to_string(),
                    op: ComparisonOp::Eq,
                    value: LiteralValue::Integer(30),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_gt() {
        let result = parse_sql("SELECT * FROM orders WHERE total > 100").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "orders".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "total".to_string(),
                    op: ComparisonOp::Gt,
                    value: LiteralValue::Integer(100),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_lt_eq() {
        let result = parse_sql("SELECT * FROM items WHERE price <= 9.99").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "items".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "price".to_string(),
                    op: ComparisonOp::LtEq,
                    value: LiteralValue::Float(9.99),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_not_eq() {
        let result = parse_sql("SELECT * FROM users WHERE status != 'inactive'").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "status".to_string(),
                    op: ComparisonOp::NotEq,
                    value: LiteralValue::String("inactive".to_string()),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_boolean() {
        let result = parse_sql("SELECT * FROM users WHERE active = true").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "active".to_string(),
                    op: ComparisonOp::Eq,
                    value: LiteralValue::Boolean(true),
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_where_is_null() {
        let result = parse_sql("SELECT * FROM users WHERE email IS NULL").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![WhereCondition {
                    column: "email".to_string(),
                    op: ComparisonOp::Eq,
                    value: LiteralValue::Null,
                }],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_multiple_where_and() {
        let result = parse_sql("SELECT * FROM users WHERE city = 'London' AND age > 25").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![
                    WhereCondition {
                        column: "city".to_string(),
                        op: ComparisonOp::Eq,
                        value: LiteralValue::String("London".to_string()),
                    },
                    WhereCondition {
                        column: "age".to_string(),
                        op: ComparisonOp::Gt,
                        value: LiteralValue::Integer(25),
                    },
                ],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    // ── ORDER BY ────────────────────────────────────────────────────

    #[test]
    fn test_select_with_order_by_asc() {
        let result = parse_sql("SELECT * FROM people ORDER BY name ASC").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![OrderByExpr { column: "name".to_string(), direction: SortDirection::Asc }],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_order_by_desc() {
        let result = parse_sql("SELECT * FROM people ORDER BY age DESC").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![OrderByExpr { column: "age".to_string(), direction: SortDirection::Desc }],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_order_by_default_asc() {
        let result = parse_sql("SELECT * FROM people ORDER BY name").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![OrderByExpr { column: "name".to_string(), direction: SortDirection::Asc }],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_multiple_order_by() {
        let result = parse_sql("SELECT * FROM people ORDER BY last_name ASC, first_name DESC").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "people".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![
                    OrderByExpr { column: "last_name".to_string(), direction: SortDirection::Asc },
                    OrderByExpr { column: "first_name".to_string(), direction: SortDirection::Desc },
                ],
                limit: None,
                offset: None,
            }
        );
    }

    // ── LIMIT / OFFSET ────────────────────────────────────────────

    #[test]
    fn test_select_with_limit() {
        let result = parse_sql("SELECT * FROM items LIMIT 10").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "items".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: Some(10),
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_with_limit_and_offset() {
        let result = parse_sql("SELECT * FROM items LIMIT 10 OFFSET 20").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "items".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: Some(10),
                offset: Some(20),
            }
        );
    }

    // ── Full kitchen-sink query ─────────────────────────────────────

    #[test]
    fn test_full_query() {
        let sql = "SELECT name, age FROM users WHERE city = 'London' AND age > 25 ORDER BY age DESC LIMIT 50 OFFSET 10";
        let result = parse_sql(sql).unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![
                    SelectColumn::Named { name: "name".to_string(), alias: None },
                    SelectColumn::Named { name: "age".to_string(), alias: None },
                ],
                where_clause: vec![
                    WhereCondition {
                        column: "city".to_string(),
                        op: ComparisonOp::Eq,
                        value: LiteralValue::String("London".to_string()),
                    },
                    WhereCondition {
                        column: "age".to_string(),
                        op: ComparisonOp::Gt,
                        value: LiteralValue::Integer(25),
                    },
                ],
                order_by: vec![OrderByExpr { column: "age".to_string(), direction: SortDirection::Desc }],
                limit: Some(50),
                offset: Some(10),
            }
        );
    }

    // ── Schema-qualified table ──────────────────────────────────────

    #[test]
    fn test_select_schema_qualified_table() {
        let result = parse_sql("SELECT * FROM public.my_projection").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "my_projection".to_string(),
                schema: Some("public".to_string()),
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_information_schema_tables() {
        let result = parse_sql("SELECT * FROM information_schema.tables").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "tables".to_string(),
                schema: Some("information_schema".to_string()),
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_select_information_schema_columns() {
        let result = parse_sql("SELECT * FROM information_schema.columns").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "columns".to_string(),
                schema: Some("information_schema".to_string()),
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    // ── SHOW TABLES ─────────────────────────────────────────────────

    #[test]
    fn test_show_tables() {
        let result = parse_sql("SHOW TABLES").unwrap();
        assert_eq!(result, ParsedQuery::ShowTables);
    }

    #[test]
    fn test_show_tables_case_insensitive() {
        let result = parse_sql("show tables").unwrap();
        assert_eq!(result, ParsedQuery::ShowTables);
    }

    // ── Error cases ─────────────────────────────────────────────────

    #[test]
    fn test_empty_query() {
        let result = parse_sql("");
        assert_eq!(result, Err(SqlParseError::EmptyQuery));
    }

    #[test]
    fn test_whitespace_only_query() {
        let result = parse_sql("   ");
        assert_eq!(result, Err(SqlParseError::EmptyQuery));
    }

    #[test]
    fn test_syntax_error() {
        let result = parse_sql("SELEC * FORM users");
        assert!(matches!(result, Err(SqlParseError::SyntaxError(_))));
    }

    #[test]
    fn test_unsupported_insert() {
        let result = parse_sql("INSERT INTO users (name) VALUES ('Alice')");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_update() {
        let result = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_delete() {
        let result = parse_sql("DELETE FROM users WHERE id = 1");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_create_table() {
        let result = parse_sql("CREATE TABLE foo (id INT)");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_drop_table() {
        let result = parse_sql("DROP TABLE foo");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_where_or() {
        // OR in WHERE is not supported for MVP — only AND conjunctions.
        let result = parse_sql("SELECT * FROM users WHERE age = 1 OR age = 2");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_subquery() {
        let result = parse_sql("SELECT * FROM (SELECT * FROM users) AS sub");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    #[test]
    fn test_unsupported_join() {
        let result = parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        assert!(matches!(result, Err(SqlParseError::Unsupported(_))));
    }

    // ── Semicolons and trailing whitespace ──────────────────────────

    #[test]
    fn test_trailing_semicolon() {
        let result = parse_sql("SELECT * FROM users;").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    #[test]
    fn test_leading_trailing_whitespace() {
        let result = parse_sql("  SELECT * FROM users  ").unwrap();
        assert_eq!(
            result,
            ParsedQuery::Select {
                table: "users".to_string(),
                schema: None,
                columns: vec![SelectColumn::Star],
                where_clause: vec![],
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );
    }

    // ── SET ────────────────────────────────────────────────────────

    #[test]
    fn test_set_variable_eq() {
        let result = parse_sql("SET client_encoding = 'UTF8'").unwrap();
        match result {
            ParsedQuery::Set { name, value } => {
                assert!(name.contains("client_encoding"));
                assert!(value.contains("UTF8") || value.contains("'UTF8'"));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_set_variable_to() {
        let result = parse_sql("SET search_path TO public").unwrap();
        match result {
            ParsedQuery::Set { name, value } => {
                assert!(name.contains("search_path"));
                assert!(value.contains("public"));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    // ── BEGIN / COMMIT / ROLLBACK ──────────────────────────────────

    #[test]
    fn test_begin() {
        assert_eq!(parse_sql("BEGIN").unwrap(), ParsedQuery::Begin);
    }

    #[test]
    fn test_start_transaction() {
        assert_eq!(parse_sql("START TRANSACTION").unwrap(), ParsedQuery::Begin);
    }

    #[test]
    fn test_commit() {
        assert_eq!(parse_sql("COMMIT").unwrap(), ParsedQuery::Commit);
    }

    #[test]
    fn test_rollback() {
        assert_eq!(parse_sql("ROLLBACK").unwrap(), ParsedQuery::Rollback);
    }

    // ── DEALLOCATE ─────────────────────────────────────────────────

    #[test]
    fn test_deallocate_named() {
        let result = parse_sql("DEALLOCATE stmt_1").unwrap();
        assert_eq!(result, ParsedQuery::Deallocate { name: Some("stmt_1".to_string()) });
    }

    #[test]
    fn test_deallocate_all() {
        let result = parse_sql("DEALLOCATE ALL").unwrap();
        assert_eq!(result, ParsedQuery::Deallocate { name: None });
    }

    // ── DISCARD ALL ────────────────────────────────────────────────

    #[test]
    fn test_discard_all() {
        assert_eq!(parse_sql("DISCARD ALL").unwrap(), ParsedQuery::DiscardAll);
    }

    // ── SELECT expression (no FROM) ───────────────────────────────

    #[test]
    fn test_select_current_database_fn() {
        let result = parse_sql("SELECT current_database()").unwrap();
        match result {
            ParsedQuery::SelectExpression { expressions, aliases } => {
                assert_eq!(expressions.len(), 1);
                assert!(expressions[0].contains("current_database"));
                assert_eq!(aliases[0], None);
            }
            other => panic!("expected SelectExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_select_version_fn() {
        let result = parse_sql("SELECT version()").unwrap();
        match result {
            ParsedQuery::SelectExpression { expressions, .. } => {
                assert!(expressions[0].to_lowercase().contains("version"));
            }
            other => panic!("expected SelectExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_select_literal_with_alias() {
        let result = parse_sql("SELECT 1 AS one").unwrap();
        match result {
            ParsedQuery::SelectExpression { expressions, aliases } => {
                assert_eq!(expressions[0], "1");
                assert_eq!(aliases[0], Some("one".into()));
            }
            other => panic!("expected SelectExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_select_multiple_exprs_no_from() {
        let result = parse_sql("SELECT current_database(), current_user").unwrap();
        match result {
            ParsedQuery::SelectExpression { expressions, .. } => {
                assert_eq!(expressions.len(), 2);
            }
            other => panic!("expected SelectExpression, got {:?}", other),
        }
    }
}
