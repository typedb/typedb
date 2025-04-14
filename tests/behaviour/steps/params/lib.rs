/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{borrow::Cow, convert::Infallible, fmt, str::FromStr, sync::Arc};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use concept::{
    error::ConceptWriteError,
    type_::{
        annotation::{
            Annotation as TypeDBAnnotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade,
            AnnotationCategory as TypeDBAnnotationCategory, AnnotationDistinct, AnnotationIndependent, AnnotationKey,
            AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        constraint::{ConstraintCategory as TypeDBConstraintCategory, ConstraintDescription as TypeDBConstraint},
        object_type::ObjectType,
        type_manager::{validation::SchemaValidationError, TypeManager},
    },
};
use cucumber::Parameter;
use encoding::{
    graph::type_::Kind as TypeDBTypeKind,
    value::{
        decimal_value::Decimal, label::Label as TypeDBLabel, timezone::TimeZone, value::Value as TypeDBValue,
        value_type::ValueType as TypeDBValueType,
    },
};
use itertools::{Either, Itertools};
use regex::Regex;
use storage::snapshot::ReadableSnapshot;
use test_utils::assert_matches;

#[derive(Debug, Clone, Parameter)]
#[param(name = "may_error", regex = "(|; fails|; fails with a message containing: \".*\")")]
pub enum MayError {
    False,
    True(Option<String>),
}

impl MayError {
    pub fn check<T: fmt::Debug, E: fmt::Debug>(&self, res: Result<T, E>) -> Either<T, E> {
        match self {
            MayError::False => Either::Left(res.unwrap()),
            MayError::True(None) => Either::Right(res.unwrap_err()),
            MayError::True(Some(expected_message)) => {
                let actual_error = res.unwrap_err();
                let actual_message = format!("{actual_error:?}");

                if actual_message.contains(expected_message) {
                    Either::Right(actual_error)
                } else {
                    panic!(
                        "Expected error message containing: '{}', but got error message: '{}'",
                        expected_message, actual_message
                    );
                }
            }
        }
    }

    pub fn check_concept_write_without_read_errors<T: fmt::Debug>(
        &self,
        res: &Result<T, Box<ConceptWriteError>>,
    ) -> Option<Box<ConceptWriteError>> {
        match self.check(res.as_ref().map_err(|e| e.as_ref())) {
            Either::Left(_) => None,
            Either::Right(error) => match error {
                ConceptWriteError::ConceptRead { typedb_source: source } => {
                    panic!("Expected logic error, got ConceptRead {:?}", source)
                }
                ConceptWriteError::SchemaValidation { typedb_source: err } => match **err {
                    SchemaValidationError::ConceptRead { .. } => {
                        panic!("Expected logic error, got SchemaValidation::ConceptRead {:?}", err)
                    }
                    _ => Some(Box::new(error.clone())),
                },
                _ => Some(Box::new(error.clone())),
            },
        }
    }

    pub fn expects_error(&self) -> bool {
        match self {
            MayError::True(_) => true,
            MayError::False => false,
        }
    }

    pub fn do_not_expect_error_message(self) -> Self {
        match self {
            MayError::False => MayError::False,
            MayError::True(_) => MayError::True(None),
        }
    }
}

impl FromStr for MayError {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(MayError::False)
        } else if s == "; fails" {
            Ok(MayError::True(None))
        } else if let Some(message) =
            s.strip_prefix("; fails with a message containing: \"").and_then(|suffix| suffix.strip_suffix("\""))
        {
            Ok(MayError::True(Some(message.to_string())))
        } else {
            Err(format!("Invalid `MayError`: {}", s))
        }
    }
}

#[derive(Debug, Clone, Parameter)]
#[param(name = "typeql_may_error", regex = "(|; fails|; parsing fails|; fails with a message containing: \".*\")")]
pub enum TypeQLMayError {
    False,
    Parsing,
    Logic(Option<String>),
}

impl TypeQLMayError {
    pub fn check<T: fmt::Debug, E: fmt::Debug>(&self, res: Result<T, E>) -> Either<T, E> {
        self.as_may_error().check(res)
    }

    pub fn check_parsing<T: fmt::Debug, E: fmt::Debug>(&self, res: Result<T, E>) -> Either<T, E> {
        self.as_may_error_parsing().check(res)
    }

    pub fn check_logic<T: fmt::Debug, E: fmt::Debug>(&self, res: Result<T, E>) -> Either<T, E> {
        self.as_may_error_logic().check(res)
    }

    pub fn expects_parsing_error(&self) -> bool {
        self.as_may_error_parsing().expects_error()
    }

    pub fn expects_logic_error(&self) -> bool {
        self.as_may_error_logic().expects_error()
    }

    pub fn as_may_error_parsing(&self) -> MayError {
        match self {
            TypeQLMayError::Parsing => MayError::True(None),
            | TypeQLMayError::False | TypeQLMayError::Logic(_) => MayError::False,
        }
    }

    pub fn as_may_error_logic(&self) -> MayError {
        match self {
            TypeQLMayError::Logic(message) => MayError::True(message.clone()),
            | TypeQLMayError::False | TypeQLMayError::Parsing => MayError::False,
        }
    }

    pub fn as_may_error(&self) -> MayError {
        match self {
            TypeQLMayError::False => MayError::False,
            TypeQLMayError::Parsing => MayError::True(None),
            TypeQLMayError::Logic(message) => MayError::True(message.clone()),
        }
    }
}

impl FromStr for TypeQLMayError {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(TypeQLMayError::False)
        } else if s == "; parsing fails" {
            Ok(TypeQLMayError::Parsing)
        } else if s == "; fails" {
            Ok(TypeQLMayError::Logic(None))
        } else if let Some(message) =
            s.strip_prefix("; fails with a message containing: \"").and_then(|suffix| suffix.strip_suffix("\""))
        {
            Ok(TypeQLMayError::Logic(Some(message.to_string())))
        } else {
            Err(format!("Invalid `TypeQLMayError`: {}", s))
        }
    }
}

#[derive(Debug, Parameter)]
#[param(name = "boolean", regex = "(true|false)")]
pub enum Boolean {
    False,
    True,
}

#[macro_export]
macro_rules! check_boolean {
    ($boolean:ident, $expr:expr) => {
        match $boolean {
            $crate::Boolean::True => assert!($expr),
            $crate::Boolean::False => assert!(!$expr),
        }
    };
}

impl FromStr for Boolean {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "true" => Self::True,
            "false" => Self::False,
            invalid => return Err(format!("Invalid `Boolean`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "exists_or_doesnt", regex = "(exists|does not exist)")]
pub enum ExistsOrDoesnt {
    Exists,
    DoesNotExist,
}

impl ExistsOrDoesnt {
    pub fn check<T: fmt::Debug>(&self, scrutinee: &Option<T>, message: &str) {
        match (self, scrutinee) {
            (Self::Exists, Some(_)) | (Self::DoesNotExist, None) => (),
            (Self::Exists, None) => panic!("{message} does not exist"),
            (Self::DoesNotExist, Some(value)) => panic!("{message} exists: {value:?}"),
        }
    }

    pub fn check_result<T: fmt::Debug, E>(&self, scrutinee: &Result<T, E>, message: &str) {
        let option = match scrutinee {
            Ok(result) => Some(result),
            Err(_) => None,
        };
        self.check(&option, message)
    }
}

impl FromStr for ExistsOrDoesnt {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "exists" => Self::Exists,
            "does not exist" => Self::DoesNotExist,
            invalid => return Err(format!("Invalid `ExistsOrDoesnt`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "is_empty_or_not", regex = "(is empty|is not empty)")]
pub enum IsEmptyOrNot {
    IsEmpty,
    IsNotEmpty,
}

impl IsEmptyOrNot {
    pub fn check(&self, real_is_empty: bool) {
        match self {
            Self::IsEmpty => {
                debug_assert!(real_is_empty)
            }
            Self::IsNotEmpty => {
                debug_assert!(!real_is_empty)
            }
        };
    }
}

impl FromStr for IsEmptyOrNot {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "is empty" => Self::IsEmpty,
            "is not empty" => Self::IsNotEmpty,
            invalid => return Err(format!("Invalid `IsEmptyOrNot`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "contains_or_doesnt", regex = "(contain|contains|do not contain|does not contain)")]
pub enum ContainsOrDoesnt {
    Contains,
    DoesNotContain,
}

impl ContainsOrDoesnt {
    pub fn check<T: PartialEq + fmt::Debug>(&self, expected: &[T], actual: &[T]) {
        let expected_contains = self.expected_contains();
        for expected_item in expected {
            assert_eq!(
                expected_contains,
                actual.contains(expected_item),
                "{:?} {} {:?} ",
                actual,
                if expected_contains { "should contain" } else { "should not contain" },
                expected
            );
        }
    }

    pub fn check_bool(&self, contains: bool, message: &str) {
        match self {
            ContainsOrDoesnt::Contains => assert!(contains, "Expected to contain, not found: {message}"),
            ContainsOrDoesnt::DoesNotContain => assert!(!contains, "Expected not to contain, but found: {message}"),
        }
    }

    pub fn expected_contains(&self) -> bool {
        match self {
            ContainsOrDoesnt::Contains => true,
            ContainsOrDoesnt::DoesNotContain => false,
        }
    }
}

impl FromStr for ContainsOrDoesnt {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "contain" | "contains" => Self::Contains,
            "do not contain" | "does not contain" => Self::DoesNotContain,
            invalid => return Err(format!("Invalid `ContainsOrDoesnt`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "type_label", regex = r"[A-Za-z0-9_:-]+")]
pub struct Label {
    pub label_string: String,
}

impl Label {
    pub fn into_typedb(self) -> TypeDBLabel {
        match &self.label_string.split_once(":") {
            None => TypeDBLabel::build(&self.label_string, None),
            Some((name, scope)) => TypeDBLabel::build_scoped(scope, name, None),
        }
    }
}

impl FromStr for Label {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { label_string: s.to_string() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "kind", regex = r"(attribute|entity|relation)")]
pub struct Kind {
    kind: TypeDBTypeKind,
}

impl Kind {
    pub fn into_typedb(self) -> TypeDBTypeKind {
        self.kind
    }
}

impl FromStr for Kind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "attribute" => TypeDBTypeKind::Attribute,
            "entity" => TypeDBTypeKind::Entity,
            "relation" => TypeDBTypeKind::Relation,
            _ => unreachable!(),
        };
        Ok(Kind { kind })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "object_kind", regex = r"(entity|relation|entities|relations)")]
pub struct ObjectKind {
    kind: TypeDBTypeKind,
}

impl ObjectKind {
    pub fn into_typedb(self) -> TypeDBTypeKind {
        self.kind
    }

    pub fn assert(&self, object: &ObjectType) {
        match self.kind {
            TypeDBTypeKind::Entity => assert_matches!(object, ObjectType::Entity(_)),
            TypeDBTypeKind::Relation => assert_matches!(object, ObjectType::Relation(_)),
            _ => unreachable!("an ObjectKind contains a non-object kind: {:?}", self.kind),
        }
    }
}

impl FromStr for ObjectKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "entity" | "entities" => TypeDBTypeKind::Entity,
            "relation" | "relations" => TypeDBTypeKind::Relation,
            _ => unreachable!(),
        };
        Ok(Self { kind })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "kind_extended", regex = r"(attribute|entity|relation|role|object)")]
pub enum KindExtended {
    Attribute,
    Entity,
    Relation,
    Role,
    Object,
}

impl FromStr for KindExtended {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "attribute" => Self::Attribute,
            "entity" => Self::Entity,
            "relation" => Self::Relation,
            "role" => Self::Role,
            "object" => Self::Object,
            invalid => return Err(format!("Invalid `KindExtended`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(
    name = "value_type",
    regex = r"(boolean|integer|double|decimal|date|datetime(?:-tz)?|duration|string|struct|struct\([A-Za-z0-9_:-]+\))"
)]
pub enum ValueType {
    Boolean,
    Integer,
    Double,
    Decimal,
    Date,
    Datetime,
    DatetimeTZ,
    Duration,
    String,
    Struct(Label),
}

impl ValueType {
    pub fn into_typedb(self, type_manager: &Arc<TypeManager>, snapshot: &impl ReadableSnapshot) -> TypeDBValueType {
        match self {
            ValueType::Boolean => TypeDBValueType::Boolean,
            ValueType::Integer => TypeDBValueType::Integer,
            ValueType::Double => TypeDBValueType::Double,
            ValueType::Decimal => TypeDBValueType::Decimal,
            ValueType::Date => TypeDBValueType::Date,
            ValueType::Datetime => TypeDBValueType::DateTime,
            ValueType::DatetimeTZ => TypeDBValueType::DateTimeTZ,
            ValueType::Duration => TypeDBValueType::Duration,
            ValueType::String => TypeDBValueType::String,
            ValueType::Struct(label) => TypeDBValueType::Struct(
                type_manager
                    .get_struct_definition_key(snapshot, label.into_typedb().scoped_name().as_str())
                    .unwrap()
                    .unwrap(),
            ),
        }
    }

    pub fn into_typedb_static(self) -> TypeDBValueType {
        match self {
            ValueType::Boolean => TypeDBValueType::Boolean,
            ValueType::Integer => TypeDBValueType::Integer,
            ValueType::Double => TypeDBValueType::Double,
            ValueType::Decimal => TypeDBValueType::Decimal,
            ValueType::Date => TypeDBValueType::Date,
            ValueType::Datetime => TypeDBValueType::DateTime,
            ValueType::DatetimeTZ => TypeDBValueType::DateTimeTZ,
            ValueType::Duration => TypeDBValueType::Duration,
            ValueType::String => TypeDBValueType::String,
            ValueType::Struct(_) => panic!("Structs are not static value types. Use `into_typedb` instead"),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            ValueType::Boolean => "boolean",
            ValueType::Integer => "integer",
            ValueType::Double => "double",
            ValueType::Decimal => "decimal",
            ValueType::Date => "date",
            ValueType::Datetime => "datetime",
            ValueType::DatetimeTZ => "datetime-tz",
            ValueType::Duration => "duration",
            ValueType::String => "string",
            ValueType::Struct(value) => &value.label_string,
        }
    }
}

impl FromStr for ValueType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "boolean" => Self::Boolean,
            "integer" => Self::Integer,
            "double" => Self::Double,
            "decimal" => Self::Decimal,
            "date" => Self::Date,
            "datetime" => Self::Datetime,
            "datetime-tz" => Self::DatetimeTZ,
            "duration" => Self::Duration,
            "string" => Self::String,
            "struct" => Self::Struct(Label { label_string: "struct".to_string() }),
            other => {
                let re = Regex::new(r"^struct\(([A-Za-z0-9_:-]+)\)$").unwrap();
                if let Some(caps) = re.captures(other) {
                    Self::Struct(Label { label_string: caps[1].to_string() })
                } else {
                    panic!("Cannot parse value type '{other}'")
                }
            }
        })
    }
}

#[derive(Debug, Default, Parameter, Clone)]
#[param(name = "value", regex = ".*?")]
pub struct Value {
    pub raw_value: String,
}

impl Value {
    const DATETIME_FORMATS: [&'static str; 8] = [
        "%Y-%m-%dT%H:%M:%S%.9f",
        "%Y-%m-%d %H:%M:%S%.9f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H",
        "%Y-%m-%d %H",
    ];
    const DATE_FORMAT: &'static str = "%Y-%m-%d";

    const FRACTIONAL_ZEROES: usize = 19;

    pub fn as_str(&self) -> &str {
        &self.raw_value
    }

    pub fn into_typedb(self, value_type: TypeDBValueType) -> TypeDBValue<'static> {
        match value_type {
            TypeDBValueType::Boolean => TypeDBValue::Boolean(self.raw_value.parse().unwrap()),
            TypeDBValueType::Integer => TypeDBValue::Integer(self.raw_value.parse().unwrap()),
            TypeDBValueType::Double => TypeDBValue::Double(self.raw_value.parse().unwrap()),
            TypeDBValueType::Decimal => {
                let trimmed_value = if self.raw_value.ends_with("dec") {
                    self.raw_value.trim_end_matches("dec")
                } else {
                    self.raw_value.as_str()
                };
                let (integer, fractional) = trimmed_value.split_once(".").unwrap_or((trimmed_value, "0"));

                let integer_parsed: i64 = integer.trim().parse().unwrap();
                let integer_parsed_abs = integer_parsed.abs();
                let fractional_parsed = Self::parse_decimal_fraction_part(fractional);

                TypeDBValue::Decimal(match integer.starts_with('-') {
                    false => Decimal::new(integer_parsed_abs, fractional_parsed),
                    true => -Decimal::new(integer_parsed_abs, fractional_parsed),
                })
            }
            TypeDBValueType::Date => {
                TypeDBValue::Date(NaiveDate::parse_from_str(&self.raw_value, Self::DATE_FORMAT).unwrap())
            }
            TypeDBValueType::DateTime => {
                let (datetime, remainder) = Self::parse_date_time_and_remainder(self.raw_value.as_str());
                assert!(
                    remainder.is_empty(),
                    "Unexpected remainder when parsing {:?} with result of {:?}",
                    self.raw_value,
                    datetime
                );
                TypeDBValue::DateTime(datetime)
            }
            TypeDBValueType::DateTimeTZ => {
                let (datetime, timezone) = Self::parse_date_time_and_remainder(self.raw_value.as_str());

                assert!(!timezone.is_empty(), "No timezone when parsing {:?}", self.raw_value);

                if timezone.starts_with(['+', '-']) {
                    TypeDBValue::DateTimeTZ(
                        datetime.and_local_timezone(TimeZone::Fixed(timezone.parse().unwrap())).unwrap(),
                    )
                } else {
                    TypeDBValue::DateTimeTZ(
                        datetime.and_local_timezone(TimeZone::IANA(timezone.parse().unwrap())).unwrap(),
                    )
                }
            }
            TypeDBValueType::Duration => TypeDBValue::Duration(self.raw_value.parse().unwrap()),
            TypeDBValueType::String => {
                let value = if self.raw_value.starts_with('"') && self.raw_value.ends_with('"') {
                    &self.raw_value[1..&self.raw_value.len() - 1]
                } else {
                    self.raw_value.as_str()
                };
                TypeDBValue::String(Cow::Owned(value.to_string()))
            }
            // TODO: Could compare string representations like in driver
            TypeDBValueType::Struct(_) => todo!(),
        }
    }

    fn parse_decimal_fraction_part(value: &str) -> u64 {
        assert!(Self::FRACTIONAL_ZEROES >= value.len());
        10_u64.pow((Self::FRACTIONAL_ZEROES - value.len()) as u32) * value.trim().parse::<u64>().unwrap()
    }

    fn parse_date_time_and_remainder(value: &str) -> (NaiveDateTime, &str) {
        for format in Self::DATETIME_FORMATS {
            if let Ok((datetime, remainder)) = NaiveDateTime::parse_and_remainder(value, format) {
                return (datetime, remainder.trim());
            }
        }
        if let Ok((date, remainder)) = NaiveDate::parse_and_remainder(value, Self::DATE_FORMAT) {
            return (date.and_time(NaiveTime::default()), remainder.trim());
        }
        panic!(
            "Cannot parse DateTime: none of the formats {:?} or {:?} fits for {:?}",
            Self::DATETIME_FORMATS,
            Self::DATE_FORMAT,
            value
        )
    }
}

impl FromStr for Value {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { raw_value: s.to_owned() })
    }
}

fn parse_regex_annotation(regex: &str) -> TypeDBAnnotation {
    assert!(
        regex.starts_with(r#"@regex(""#) && regex.ends_with(r#"")"#),
        r#"Invalid @regex format: {regex:?}. Expected "@regex("regex-here")""#
    );
    let regex = &regex[r#"@regex(""#.len()..regex.len() - r#"")"#.len()];
    TypeDBAnnotation::Regex(AnnotationRegex::new(regex.to_owned()))
}

fn parse_card_annotation(card: &str) -> TypeDBAnnotation {
    assert!(
        card.starts_with("@card(") && card.ends_with(')'),
        r#"Invalid @card format: {card:?}. Expected "@card(min..max)""#
    );
    let card = card["@card(".len()..card.len() - ")".len()].trim();
    let (min, max) = card.split_once("..").map(|(min, max)| (min.trim(), max.trim())).unwrap();

    TypeDBAnnotation::Cardinality(AnnotationCardinality::new(
        min.parse().unwrap(),
        if max.is_empty() { None } else { Some(max.parse().unwrap()) },
    ))
}

fn parse_values_annotation(values: &str, value_type: Option<TypeDBValueType>) -> TypeDBAnnotation {
    assert!(
        values.starts_with("@values(") && values.ends_with(')'),
        r#"Invalid @values format: {values:?}. Expected "@values(val1, val2, ..., valN)""#
    );
    assert!(value_type.is_some(), "ValueType is expected to parse annotation @values");
    let value_type = value_type.unwrap();
    let values = values["@values(".len()..values.len() - ")".len()].trim();
    let values = values.split(',');
    TypeDBAnnotation::Values(AnnotationValues::new(
        values.map(|value| Value::from_str(value.trim()).unwrap().into_typedb(value_type.clone())).collect_vec(),
    ))
}

fn parse_range_annotation(range: &str, value_type: Option<TypeDBValueType>) -> TypeDBAnnotation {
    assert!(
        range.starts_with("@range(") && range.ends_with(')') && range.contains(".."),
        r#"Invalid @range format: {range:?}. Expected "@range(min..max)""#
    );
    assert!(value_type.is_some(), "ValueType is expected to parse annotation @range");
    let value_type = value_type.unwrap();
    let range = range["@range(".len()..range.len() - ")".len()].trim();
    let (min, max) = range.split_once("..").map(|(min, max)| (min.trim(), max.trim())).unwrap();
    TypeDBAnnotation::Range(AnnotationRange::new(
        if min.is_empty() { None } else { Some(Value::from_str(min).unwrap().into_typedb(value_type.clone())) },
        if max.is_empty() { None } else { Some(Value::from_str(max).unwrap().into_typedb(value_type)) },
    ))
}

fn parse_subkey_annotation(subkey: &str) -> TypeDBAnnotation {
    unreachable!("Subkey is not implemented for tests!");
    // assert!(
    //     subkey.starts_with(r#"@subkey("#) && subkey.ends_with(r#")"#),
    //     r#"Invalid @subkey format: {subkey:?}. Expected "@subkey(LABEL)""#
    // );
    // let label = &subkey[r#"@subkey("#.len()..subkey.len() - r#")"#.len()];
    // TypeDBAnnotation::Subkey(AnnotationSubkey::new(label.to_owned()))
}

#[derive(Debug, Parameter)]
#[param(name = "annotation", regex = r"@[a-z]+(?:\(.+\))?")]
pub struct Annotation {
    raw_annotation: String,
}

impl Annotation {
    pub fn into_typedb(self, value_type: Option<TypeDBValueType>) -> TypeDBAnnotation {
        match self.raw_annotation.as_str() {
            "@abstract" => TypeDBAnnotation::Abstract(AnnotationAbstract),
            "@independent" => TypeDBAnnotation::Independent(AnnotationIndependent),
            "@key" => TypeDBAnnotation::Key(AnnotationKey),
            "@unique" => TypeDBAnnotation::Unique(AnnotationUnique),
            "@distinct" => TypeDBAnnotation::Distinct(AnnotationDistinct),
            "@cascade" => TypeDBAnnotation::Cascade(AnnotationCascade),
            regex if regex.starts_with("@regex") => parse_regex_annotation(regex),
            card if card.starts_with("@card") => parse_card_annotation(card),
            values if values.starts_with("@values") => parse_values_annotation(values, value_type),
            range if range.starts_with("@range") => parse_range_annotation(range, value_type),
            subkey if subkey.starts_with("@subkey") => parse_subkey_annotation(subkey),
            _ => unreachable!("Cannot parse annotation {:?}", self.raw_annotation),
        }
    }
}

impl FromStr for Annotation {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { raw_annotation: s.to_owned() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "annotation_category", regex = r"@[a-z]+")]
pub struct AnnotationCategory {
    typedb_annotation_category: TypeDBAnnotationCategory,
}

impl AnnotationCategory {
    pub fn into_typedb(self) -> TypeDBAnnotationCategory {
        self.typedb_annotation_category
    }
}

impl FromStr for AnnotationCategory {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // This will have to be smarter to parse annotations out.
        let typedb_annotation_category = match s {
            "@abstract" => TypeDBAnnotationCategory::Abstract,
            "@independent" => TypeDBAnnotationCategory::Independent,
            "@key" => TypeDBAnnotationCategory::Key,
            "@unique" => TypeDBAnnotationCategory::Unique,
            "@distinct" => TypeDBAnnotationCategory::Distinct,
            "@cascade" => TypeDBAnnotationCategory::Cascade,
            "@regex" => TypeDBAnnotationCategory::Regex,
            "@card" => TypeDBAnnotationCategory::Cardinality,
            "@range" => TypeDBAnnotationCategory::Range,
            "@values" => TypeDBAnnotationCategory::Values,
            "@subkey" => return Err("Not implemented!".to_owned()), //TypeDBAnnotationCategory::Subkey,
            _ => panic!("Unrecognised (or unimplemented) annotation: {s}"),
        };
        Ok(Self { typedb_annotation_category })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "annotations", regex = r"@[a-z]+(?:\([^)]+\))?(?: +@[a-z]+(?:\([^)]+\))?)?")]
pub struct Annotations {
    typedb_annotations: Vec<TypeDBAnnotation>,
}

impl Annotations {
    pub fn into_typedb(self) -> Vec<TypeDBAnnotation> {
        self.typedb_annotations
    }
}

impl FromStr for Annotations {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut cursor = 0;

        let typedb_annotations = std::iter::from_fn(|| {
            if s.len() >= cursor {
                None
            } else {
                let next_at = if let Some(index) = s[cursor..].find('@') { cursor + index } else { s.len() };
                let anno = s[cursor..next_at].trim();
                cursor = next_at;
                Some(anno.parse::<Annotation>().map(|anno| anno.into_typedb(None)))
                // TODO: Refactor parsing to support passing ValueTypes into anno.into_typedb
            }
        })
        .try_collect()?;

        Ok(Self { typedb_annotations })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "constraint", regex = r"@[a-z]+(?:\(.+\))?")]
pub struct Constraint {
    raw_constraint: String,
}

impl Constraint {
    pub fn into_typedb(self, value_type: Option<TypeDBValueType>) -> TypeDBConstraint {
        let constraints = TypeDBConstraint::from_annotation(match self.raw_constraint.as_str() {
            "@abstract" => TypeDBAnnotation::Abstract(AnnotationAbstract),
            "@independent" => TypeDBAnnotation::Independent(AnnotationIndependent),
            "@key" => TypeDBAnnotation::Key(AnnotationKey),
            "@unique" => TypeDBAnnotation::Unique(AnnotationUnique),
            "@distinct" => TypeDBAnnotation::Distinct(AnnotationDistinct),
            "@cascade" => TypeDBAnnotation::Cascade(AnnotationCascade),
            regex if regex.starts_with("@regex") => parse_regex_annotation(regex),
            regex if regex.starts_with("@regex") => parse_regex_annotation(regex),
            card if card.starts_with("@card") => parse_card_annotation(card),
            values if values.starts_with("@values") => parse_values_annotation(values, value_type),
            range if range.starts_with("@range") => parse_range_annotation(range, value_type),
            subkey if subkey.starts_with("@subkey") => parse_subkey_annotation(subkey),
            _ => unreachable!("Cannot parse constraint {:?}", self.raw_constraint),
        });
        if constraints.len() != 1 {
            panic!(
                "Cannot parse constraint {}. Make sure you expect a constraint, not an annotation.",
                self.raw_constraint
            );
        }
        constraints.iter().next().unwrap().clone()
    }
}

impl FromStr for Constraint {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { raw_constraint: s.to_owned() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "constraint_category", regex = r"@[a-z]+")]
pub struct ConstraintCategory {
    typedb_constraint_category: TypeDBConstraintCategory,
}

impl ConstraintCategory {
    pub fn into_typedb(self) -> TypeDBConstraintCategory {
        self.typedb_constraint_category
    }
}

impl FromStr for ConstraintCategory {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let typedb_constraint_category = match s {
            "@abstract" => TypeDBConstraintCategory::Abstract,
            "@independent" => TypeDBConstraintCategory::Independent,
            "@unique" => TypeDBConstraintCategory::Unique,
            "@distinct" => TypeDBConstraintCategory::Distinct,
            "@regex" => TypeDBConstraintCategory::Regex,
            "@card" => TypeDBConstraintCategory::Cardinality,
            "@range" => TypeDBConstraintCategory::Range,
            "@values" => TypeDBConstraintCategory::Values,
            "@subkey" => return Err("Not implemented!".to_owned()), //TypeDBConstraintCategory::Subkey,
            _ => panic!("Unrecognised (or unimplemented) annotation: {s}"),
        };
        Ok(Self { typedb_constraint_category })
    }
}

#[derive(Clone, Debug, Default, Parameter)]
#[param(name = "var", regex = r"\$[\w_-]+")]
pub struct Var {
    pub name: String,
}

impl FromStr for Var {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(Self { name: name.to_owned() })
    }
}

#[derive(Clone, Debug, Default, Parameter)]
#[param(name = "vars", regex = r"\[(\$[\w_-]+(?:,\s*\$[\w_-]+)*)\]")]
pub struct Vars {
    pub names: Vec<String>,
}

impl FromStr for Vars {
    type Err = Infallible;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        Ok(Self { names: str.split(',').map(|name| name.trim().to_owned()).collect() })
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "ordering", regex = "(unordered|ordered)")]
pub enum Ordering {
    Unordered,
    Ordered,
}

impl Ordering {
    pub fn into_typedb(self) -> concept::type_::Ordering {
        match self {
            Ordering::Unordered => concept::type_::Ordering::Unordered,
            Ordering::Ordered => concept::type_::Ordering::Ordered,
        }
    }
}

impl FromStr for Ordering {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "unordered" => Self::Unordered,
            "ordered" => Self::Ordered,
            _ => panic!("Unrecognised ordering"),
        })
    }
}

#[derive(Clone, Copy, Debug, Parameter)]
#[param(name = "optional", regex = "(|\\?)")]
pub enum Optional {
    False,
    True,
}

impl Optional {
    pub fn into_typedb(self) -> bool {
        match &self {
            Optional::False => false,
            Optional::True => true,
        }
    }
}

impl FromStr for Optional {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "?" => Self::True,
            "" => Self::False,
            invalid => return Err(format!("Invalid `Optional`: {invalid}")),
        })
    }
}
