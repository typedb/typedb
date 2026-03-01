/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Parser for projection DDL statements.
//!
//! Since the `typeql` crate is an external dependency (pinned to tag 3.8.0)
//! and its `Definable` enum cannot be extended without forking, this module
//! provides a standalone parser for projection-specific schema statements.
//!
//! Supported syntax:
//!
//! ```text
//! define projection <name>(<col>: <type>, ...) as "<source_query>";
//! undefine projection <name>;
//! ```
//!
//! The parser produces a [`ProjectionStatement`] which can be fed into
//! [`ProjectionStore::define`](crate::projection_store::ProjectionStore::define)
//! or [`ProjectionStore::undefine`](crate::projection_store::ProjectionStore::undefine).

use encoding::value::value_type::ValueTypeCategory;

use crate::definition::{ColumnDefinition, ProjectionDefinition, ProjectionDefinitionError};

// ── Public types ───────────────────────────────────────────────────

/// A parsed projection DDL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionStatement {
    /// `define projection <name>(<columns>) as "<query>";`
    Define(ProjectionDefinition),
    /// `undefine projection <name>;`
    Undefine { name: String },
}

/// Errors from parsing projection DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Input is empty or all whitespace.
    EmptyInput,
    /// Expected a specific keyword but found something else.
    ExpectedKeyword { expected: &'static str, found: String },
    /// Expected a specific character at a position.
    ExpectedChar { expected: char, position: usize },
    /// Expected an identifier at a position.
    InvalidIdentifier { position: usize },
    /// Unknown column type name.
    InvalidType(String),
    /// A quoted string was not terminated.
    UnterminatedString { position: usize },
    /// Unexpected input after the statement.
    TrailingInput { position: usize },
    /// Column list between parentheses was empty.
    EmptyColumnList,
    /// The constructed definition failed validation.
    DefinitionError(ProjectionDefinitionError),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "empty input"),
            Self::ExpectedKeyword { expected, found } => {
                write!(f, "expected keyword '{expected}', found '{found}'")
            }
            Self::ExpectedChar { expected, position } => {
                write!(f, "expected '{expected}' at position {position}")
            }
            Self::InvalidIdentifier { position } => {
                write!(f, "expected identifier at position {position}")
            }
            Self::InvalidType(t) => write!(f, "unknown column type: '{t}'"),
            Self::UnterminatedString { position } => {
                write!(f, "unterminated string starting at position {position}")
            }
            Self::TrailingInput { position } => {
                write!(f, "unexpected trailing input at position {position}")
            }
            Self::EmptyColumnList => write!(f, "column list must not be empty"),
            Self::DefinitionError(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<ProjectionDefinitionError> for ParseError {
    fn from(err: ProjectionDefinitionError) -> Self {
        Self::DefinitionError(err)
    }
}

// ── Public API ─────────────────────────────────────────────────────

/// Parse a projection DDL statement.
///
/// Accepts `define projection …` and `undefine projection …` syntax.
/// Returns a [`ProjectionStatement`] on success or a [`ParseError`]
/// describing what went wrong.
pub fn parse_projection_ddl(input: &str) -> Result<ProjectionStatement, ParseError> {
    let mut parser = Parser::new(input);
    parser.parse()
}

// ── Type name mapping ──────────────────────────────────────────────

/// Map a type name string to a [`ValueTypeCategory`].
///
/// Accepts canonical TypeDB type names plus common aliases:
/// - `boolean` / `bool`
/// - `integer` / `int` / `long`
/// - `double` / `float`
/// - `decimal`
/// - `date`
/// - `datetime`
/// - `datetime-tz` / `datetimetz`
/// - `duration`
/// - `string` / `text`
/// - `struct`
pub fn parse_value_type(name: &str) -> Result<ValueTypeCategory, ParseError> {
    match name.to_lowercase().as_str() {
        "boolean" | "bool" => Ok(ValueTypeCategory::Boolean),
        "integer" | "int" | "long" => Ok(ValueTypeCategory::Integer),
        "double" | "float" => Ok(ValueTypeCategory::Double),
        "decimal" => Ok(ValueTypeCategory::Decimal),
        "date" => Ok(ValueTypeCategory::Date),
        "datetime" => Ok(ValueTypeCategory::DateTime),
        "datetime-tz" | "datetimetz" => Ok(ValueTypeCategory::DateTimeTZ),
        "duration" => Ok(ValueTypeCategory::Duration),
        "string" | "text" => Ok(ValueTypeCategory::String),
        "struct" => Ok(ValueTypeCategory::Struct),
        _ => Err(ParseError::InvalidType(name.to_string())),
    }
}

// ── Internal parser ────────────────────────────────────────────────

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<ProjectionStatement, ParseError> {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return Err(ParseError::EmptyInput);
        }

        let keyword = self.read_identifier()?;
        match keyword.to_lowercase().as_str() {
            "define" => self.parse_define(),
            "undefine" => self.parse_undefine(),
            _ => Err(ParseError::ExpectedKeyword { expected: "define' or 'undefine", found: keyword }),
        }
    }

    fn parse_define(&mut self) -> Result<ProjectionStatement, ParseError> {
        self.skip_whitespace();
        self.expect_keyword("projection")?;
        self.skip_whitespace();
        let name = self.read_identifier()?;
        self.skip_whitespace();
        self.expect_char('(')?;
        let columns = self.parse_column_list()?;
        self.skip_whitespace();
        self.expect_keyword("as")?;
        self.skip_whitespace();
        let source_query = self.read_quoted_string()?;
        self.skip_whitespace();
        self.expect_char(';')?;
        self.skip_whitespace();

        if self.pos < self.input.len() {
            return Err(ParseError::TrailingInput { position: self.pos });
        }

        let def = ProjectionDefinition::new(name, columns, source_query)?;
        Ok(ProjectionStatement::Define(def))
    }

    fn parse_undefine(&mut self) -> Result<ProjectionStatement, ParseError> {
        self.skip_whitespace();
        self.expect_keyword("projection")?;
        self.skip_whitespace();
        let name = self.read_identifier()?;
        self.skip_whitespace();
        self.expect_char(';')?;
        self.skip_whitespace();

        if self.pos < self.input.len() {
            return Err(ParseError::TrailingInput { position: self.pos });
        }

        Ok(ProjectionStatement::Undefine { name })
    }

    fn parse_column_list(&mut self) -> Result<Vec<ColumnDefinition>, ParseError> {
        let mut columns = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(')') {
                self.pos += 1;
                break;
            }
            if !columns.is_empty() {
                self.expect_char(',')?;
                self.skip_whitespace();
            }
            let col_name = self.read_identifier()?;
            self.skip_whitespace();
            self.expect_char(':')?;
            self.skip_whitespace();
            let type_name = self.read_type_identifier()?;
            let value_type = parse_value_type(&type_name)?;
            columns.push(ColumnDefinition::new(col_name, value_type));
        }
        if columns.is_empty() {
            return Err(ParseError::EmptyColumnList);
        }
        Ok(columns)
    }

    // ── Primitives ─────────────────────────────────────────────

    fn skip_whitespace(&mut self) {
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() && bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn read_identifier(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() && (bytes[self.pos].is_ascii_alphanumeric() || bytes[self.pos] == b'_') {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(ParseError::InvalidIdentifier { position: start });
        }
        Ok(self.input[start..self.pos].to_string())
    }

    /// Like `read_identifier` but also allows hyphens (for `datetime-tz`).
    fn read_type_identifier(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len()
            && (bytes[self.pos].is_ascii_alphanumeric() || bytes[self.pos] == b'_' || bytes[self.pos] == b'-')
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(ParseError::InvalidIdentifier { position: start });
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn read_quoted_string(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let quote = match self.peek() {
            Some('"') => b'"',
            Some('\'') => b'\'',
            _ => return Err(ParseError::ExpectedChar { expected: '"', position: self.pos }),
        };
        self.pos += 1; // skip opening quote
        let content_start = self.pos;
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() {
            if bytes[self.pos] == quote {
                let content = self.input[content_start..self.pos].to_string();
                self.pos += 1; // skip closing quote
                return Ok(content);
            }
            if bytes[self.pos] == b'\\' && self.pos + 1 < bytes.len() {
                self.pos += 2; // skip escape sequence
            } else {
                self.pos += 1;
            }
        }
        Err(ParseError::UnterminatedString { position: start })
    }

    fn expect_keyword(&mut self, keyword: &'static str) -> Result<(), ParseError> {
        let ident = self.read_identifier()?;
        if ident.to_lowercase() != keyword {
            Err(ParseError::ExpectedKeyword { expected: keyword, found: ident })
        } else {
            Ok(())
        }
    }

    fn expect_char(&mut self, ch: char) -> Result<(), ParseError> {
        match self.peek() {
            Some(c) if c == ch => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(ParseError::ExpectedChar { expected: ch, position: self.pos }),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_QUERY: &str = "match $e isa employee; fetch { name: $e.name; };";

    // ── Define: happy paths ──────────────────────────────────────

    #[test]
    fn parse_define_single_column() {
        let input = r#"define projection Employees(name: string) as "match $e isa employee;";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.name(), "Employees");
                assert_eq!(def.column_count(), 1);
                assert_eq!(def.columns()[0].name, "name");
                assert_eq!(def.columns()[0].value_type, ValueTypeCategory::String);
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_multiple_columns() {
        let input = r#"define projection Staff(name: string, age: integer, active: boolean) as "match $s isa staff;";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.name(), "Staff");
                assert_eq!(def.column_count(), 3);
                assert_eq!(def.column_names(), vec!["name", "age", "active"]);
                assert_eq!(
                    def.column_types(),
                    vec![ValueTypeCategory::String, ValueTypeCategory::Integer, ValueTypeCategory::Boolean,]
                );
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_all_scalar_types() {
        let input = r#"define projection AllTypes(
            a: boolean,
            b: integer,
            c: double,
            d: decimal,
            e: date,
            f: datetime,
            g: datetime-tz,
            h: duration,
            i: string
        ) as "match $x isa thing;";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.column_count(), 9);
                assert_eq!(
                    def.column_types(),
                    vec![
                        ValueTypeCategory::Boolean,
                        ValueTypeCategory::Integer,
                        ValueTypeCategory::Double,
                        ValueTypeCategory::Decimal,
                        ValueTypeCategory::Date,
                        ValueTypeCategory::DateTime,
                        ValueTypeCategory::DateTimeTZ,
                        ValueTypeCategory::Duration,
                        ValueTypeCategory::String,
                    ]
                );
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_type_aliases() {
        // bool, int, long, float, text, datetimetz
        let input = r#"define projection A(a: bool, b: int, c: long, d: float, e: text, f: datetimetz) as "q";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(
                    def.column_types(),
                    vec![
                        ValueTypeCategory::Boolean,
                        ValueTypeCategory::Integer,
                        ValueTypeCategory::Integer,
                        ValueTypeCategory::Double,
                        ValueTypeCategory::String,
                        ValueTypeCategory::DateTimeTZ,
                    ]
                );
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_preserves_source_query() {
        let query = "match $p isa person, has name $n; fetch { name: $n; };";
        let input = format!(r#"define projection P(name: string) as "{query}";"#);
        let result = parse_projection_ddl(&input).unwrap();
        match result {
            ProjectionStatement::Define(def) => assert_eq!(def.source_query(), query),
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_with_single_quotes() {
        let input = "define projection P(name: string) as 'match $x isa thing;';";
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.source_query(), "match $x isa thing;");
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_with_extra_whitespace() {
        let input = "  define   projection   Ws  ( name : string , age : integer )  as  \"query\"  ;  ";
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.name(), "Ws");
                assert_eq!(def.column_count(), 2);
            }
            _ => panic!("expected Define"),
        }
    }

    #[test]
    fn parse_define_case_insensitive_keywords() {
        let input = r#"DEFINE PROJECTION Upper(id: INTEGER) AS "q";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.name(), "Upper");
                assert_eq!(def.columns()[0].value_type, ValueTypeCategory::Integer);
            }
            _ => panic!("expected Define"),
        }
    }

    // ── Undefine: happy paths ────────────────────────────────────

    #[test]
    fn parse_undefine_statement() {
        let input = "undefine projection Employees;";
        let result = parse_projection_ddl(input).unwrap();
        assert_eq!(result, ProjectionStatement::Undefine { name: "Employees".into() });
    }

    #[test]
    fn parse_undefine_with_whitespace() {
        let input = "  undefine   projection   Old  ;  ";
        let result = parse_projection_ddl(input).unwrap();
        assert_eq!(result, ProjectionStatement::Undefine { name: "Old".into() });
    }

    #[test]
    fn parse_undefine_case_insensitive() {
        let input = "UNDEFINE PROJECTION Test;";
        let result = parse_projection_ddl(input).unwrap();
        assert_eq!(result, ProjectionStatement::Undefine { name: "Test".into() });
    }

    // ── Error paths ──────────────────────────────────────────────

    #[test]
    fn parse_error_empty_input() {
        assert_eq!(parse_projection_ddl(""), Err(ParseError::EmptyInput));
        assert_eq!(parse_projection_ddl("   "), Err(ParseError::EmptyInput));
    }

    #[test]
    fn parse_error_unknown_keyword() {
        let err = parse_projection_ddl("create projection X(a: int) as \"q\";").unwrap_err();
        match err {
            ParseError::ExpectedKeyword { found, .. } => assert_eq!(found, "create"),
            _ => panic!("expected ExpectedKeyword error, got {err:?}"),
        }
    }

    #[test]
    fn parse_error_missing_projection_keyword() {
        let err = parse_projection_ddl("define table X(a: int) as \"q\";").unwrap_err();
        match err {
            ParseError::ExpectedKeyword { expected, found } => {
                assert_eq!(expected, "projection");
                assert_eq!(found, "table");
            }
            _ => panic!("expected ExpectedKeyword error, got {err:?}"),
        }
    }

    #[test]
    fn parse_error_missing_name() {
        let err = parse_projection_ddl("define projection (a: int) as \"q\";").unwrap_err();
        // Parser tries to read an identifier for the name but finds '('
        assert!(matches!(err, ParseError::InvalidIdentifier { .. }));
    }

    #[test]
    fn parse_error_empty_column_list() {
        let err = parse_projection_ddl(r#"define projection X() as "q";"#).unwrap_err();
        assert_eq!(err, ParseError::EmptyColumnList);
    }

    #[test]
    fn parse_error_invalid_type() {
        let err = parse_projection_ddl(r#"define projection X(a: varchar) as "q";"#).unwrap_err();
        assert_eq!(err, ParseError::InvalidType("varchar".into()));
    }

    #[test]
    fn parse_error_missing_source_query() {
        let err = parse_projection_ddl("define projection X(a: int);").unwrap_err();
        // Parser tries to read "as" keyword but finds ';' — returns InvalidIdentifier
        assert!(matches!(err, ParseError::InvalidIdentifier { .. }));
    }

    #[test]
    fn parse_error_unterminated_string() {
        let err = parse_projection_ddl(r#"define projection X(a: int) as "unterminated;"#).unwrap_err();
        assert!(matches!(err, ParseError::UnterminatedString { .. }));
    }

    #[test]
    fn parse_error_missing_semicolon() {
        let err = parse_projection_ddl(r#"define projection X(a: int) as "q""#).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedChar { expected: ';', .. }));
    }

    #[test]
    fn parse_error_trailing_input() {
        let err = parse_projection_ddl(r#"define projection X(a: int) as "q"; extra"#).unwrap_err();
        assert!(matches!(err, ParseError::TrailingInput { .. }));
    }

    #[test]
    fn parse_error_missing_colon_in_column() {
        let err = parse_projection_ddl(r#"define projection X(name string) as "q";"#).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedChar { expected: ':', .. }));
    }

    #[test]
    fn parse_error_missing_comma_between_columns() {
        let err = parse_projection_ddl(r#"define projection X(a: int b: string) as "q";"#).unwrap_err();
        assert!(matches!(err, ParseError::ExpectedChar { expected: ',', .. }));
    }

    // ── parse_value_type unit tests ──────────────────────────────

    #[test]
    fn value_type_canonical_names() {
        assert_eq!(parse_value_type("boolean").unwrap(), ValueTypeCategory::Boolean);
        assert_eq!(parse_value_type("integer").unwrap(), ValueTypeCategory::Integer);
        assert_eq!(parse_value_type("double").unwrap(), ValueTypeCategory::Double);
        assert_eq!(parse_value_type("decimal").unwrap(), ValueTypeCategory::Decimal);
        assert_eq!(parse_value_type("date").unwrap(), ValueTypeCategory::Date);
        assert_eq!(parse_value_type("datetime").unwrap(), ValueTypeCategory::DateTime);
        assert_eq!(parse_value_type("datetime-tz").unwrap(), ValueTypeCategory::DateTimeTZ);
        assert_eq!(parse_value_type("duration").unwrap(), ValueTypeCategory::Duration);
        assert_eq!(parse_value_type("string").unwrap(), ValueTypeCategory::String);
        assert_eq!(parse_value_type("struct").unwrap(), ValueTypeCategory::Struct);
    }

    #[test]
    fn value_type_case_insensitive() {
        assert_eq!(parse_value_type("BOOLEAN").unwrap(), ValueTypeCategory::Boolean);
        assert_eq!(parse_value_type("Integer").unwrap(), ValueTypeCategory::Integer);
        assert_eq!(parse_value_type("STRING").unwrap(), ValueTypeCategory::String);
    }

    #[test]
    fn value_type_aliases() {
        assert_eq!(parse_value_type("bool").unwrap(), ValueTypeCategory::Boolean);
        assert_eq!(parse_value_type("int").unwrap(), ValueTypeCategory::Integer);
        assert_eq!(parse_value_type("long").unwrap(), ValueTypeCategory::Integer);
        assert_eq!(parse_value_type("float").unwrap(), ValueTypeCategory::Double);
        assert_eq!(parse_value_type("text").unwrap(), ValueTypeCategory::String);
        assert_eq!(parse_value_type("datetimetz").unwrap(), ValueTypeCategory::DateTimeTZ);
    }

    #[test]
    fn value_type_unknown_is_error() {
        assert_eq!(parse_value_type("varchar"), Err(ParseError::InvalidType("varchar".into())));
        assert_eq!(parse_value_type("bigint"), Err(ParseError::InvalidType("bigint".into())));
    }

    // ── Error Display ────────────────────────────────────────────

    #[test]
    fn error_display_messages() {
        assert_eq!(ParseError::EmptyInput.to_string(), "empty input");
        assert_eq!(ParseError::EmptyColumnList.to_string(), "column list must not be empty");
        assert_eq!(ParseError::InvalidType("varchar".into()).to_string(), "unknown column type: 'varchar'");
        assert!(ParseError::UnterminatedString { position: 5 }.to_string().contains("5"));
        assert!(ParseError::TrailingInput { position: 10 }.to_string().contains("10"));
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(ParseError::EmptyInput);
        assert_eq!(err.to_string(), "empty input");
    }

    // ── Struct type parses (even if store rejects it) ────────────

    #[test]
    fn parse_define_with_struct_column() {
        let input = r#"define projection S(data: struct) as "match $x isa thing;";"#;
        let result = parse_projection_ddl(input).unwrap();
        match result {
            ProjectionStatement::Define(def) => {
                assert_eq!(def.columns()[0].value_type, ValueTypeCategory::Struct);
            }
            _ => panic!("expected Define"),
        }
    }
}
