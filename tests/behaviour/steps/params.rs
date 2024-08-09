/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, convert::Infallible, fmt, str::FromStr, sync::Arc};

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Tz;
use concept::{
    error::ConceptWriteError,
    type_::{
        annotation::{
            Annotation as TypeDBAnnotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade,
            AnnotationCategory as TypeDBAnnotationCategory, AnnotationDistinct, AnnotationIndependent, AnnotationKey,
            AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues,
        },
        object_type::ObjectType,
        type_manager::{validation::SchemaValidationError, TypeManager},
    },
};
use cucumber::Parameter;
use encoding::{
    graph::type_::Kind as TypeDBTypeKind,
    value::{
        decimal_value::Decimal, label::Label as TypeDBLabel, value::Value as TypeDBValue,
        value_type::ValueType as TypeDBValueType,
    },
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::assert::assert_matches;

#[derive(Debug, Parameter)]
#[param(name = "may_error", regex = "(; fails|)")]
pub(crate) enum MayError {
    False,
    True,
}

impl MayError {
    pub fn check<'a, T: fmt::Debug, E: fmt::Debug>(&self, res: &'a Result<T, E>) -> Option<&'a E> {
        match self {
            MayError::False => {
                res.as_ref().unwrap();
                None
            }
            MayError::True => Some(res.as_ref().unwrap_err()),
        }
    }

    pub fn check_concept_write_without_read_errors<T: fmt::Debug>(&self, res: &Result<T, ConceptWriteError>) {
        match self {
            MayError::False => {
                res.as_ref().unwrap();
            }
            MayError::True => match res.as_ref().unwrap_err() {
                ConceptWriteError::ConceptRead { source } => panic!("Expected error is ConceptRead {:?}", source),
                ConceptWriteError::SchemaValidation { source } => match source {
                    SchemaValidationError::ConceptRead(source) => {
                        panic!("Expected error is SchemaValidation::ConceptRead {:?}", source)
                    }
                    _ => {}
                },
                _ => {}
            },
        };
    }

    pub fn expects_error(&self) -> bool {
        match self {
            MayError::True => true,
            MayError::False => false,
        }
    }
}

impl FromStr for MayError {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "; fails" => Self::True,
            "" => Self::False,
            invalid => return Err(format!("Invalid `MayError`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "boolean", regex = "(true|false)")]
pub(crate) enum Boolean {
    False,
    True,
}

macro_rules! check_boolean {
    ($boolean:ident, $expr:expr) => {
        match $boolean {
            $crate::params::Boolean::True => assert!($expr),
            $crate::params::Boolean::False => assert!(!$expr),
        }
    };
}
pub(crate) use check_boolean;

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
pub(crate) enum ExistsOrDoesnt {
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
pub(crate) enum IsEmptyOrNot {
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
#[param(name = "contains_or_doesnt", regex = "(contain|do not contain)")]
pub(crate) enum ContainsOrDoesnt {
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
                if expected_contains { "contains" } else { "does not contain" },
                expected
            );
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
            "contain" => Self::Contains,
            "do not contain" => Self::DoesNotContain,
            invalid => return Err(format!("Invalid `ContainsOrDoesnt`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "type_label", regex = r"[A-Za-z0-9_:-]+")]
pub(crate) struct Label {
    label_string: String,
}

impl Default for Label {
    fn default() -> Self {
        unreachable!("Why is default called?");
    }
}

impl Label {
    pub fn into_typedb(&self) -> TypeDBLabel<'static> {
        match &self.label_string.split_once(":") {
            None => TypeDBLabel::build(&self.label_string),
            Some((name, scope)) => TypeDBLabel::build_scoped(scope, name),
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
#[param(name = "root_label", regex = r"(attribute|entity|relation)")]
pub(crate) struct RootLabel {
    kind: TypeDBTypeKind,
}

impl RootLabel {
    pub fn into_typedb(&self) -> TypeDBTypeKind {
        self.kind
    }
}

impl FromStr for RootLabel {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "attribute" => TypeDBTypeKind::Attribute,
            "entity" => TypeDBTypeKind::Entity,
            "relation" => TypeDBTypeKind::Relation,
            _ => unreachable!(),
        };
        Ok(RootLabel { kind })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "object_root_label", regex = r"(entity|relation|entities|relations)")]
pub(crate) struct ObjectRootLabel {
    kind: TypeDBTypeKind,
}

impl ObjectRootLabel {
    pub fn into_typedb(&self) -> TypeDBTypeKind {
        self.kind
    }

    pub fn assert(&self, object: &ObjectType<'_>) {
        match self.kind {
            TypeDBTypeKind::Entity => assert_matches!(object, ObjectType::Entity(_)),
            TypeDBTypeKind::Relation => assert_matches!(object, ObjectType::Relation(_)),
            _ => unreachable!("an ObjectRootLabel contains a non-object kind: {:?}", self.kind),
        }
    }
}

impl FromStr for ObjectRootLabel {
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
#[param(name = "root_label_extended", regex = r"(attribute|entity|relation|role|object)")]
pub(crate) enum RootLabelExtended {
    Attribute,
    Entity,
    Relation,
    Role,
    Object,
}

impl FromStr for RootLabelExtended {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "attribute" => Self::Attribute,
            "entity" => Self::Entity,
            "relation" => Self::Relation,
            "role" => Self::Role,
            "object" => Self::Object,
            invalid => return Err(format!("Invalid `RootLabelExtended`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "value_type", regex = "(boolean|long|double|decimal|datetime(?:-tz)?|duration|string|[A-Za-z0-9_:-]+)")]
pub(crate) enum ValueType {
    Boolean,
    Long,
    Double,
    Decimal,
    Date,
    DateTime,
    DateTimeTZ,
    Duration,
    String,
    Struct(Label),
}

impl ValueType {
    pub fn into_typedb(&self, type_manager: &Arc<TypeManager>, snapshot: &impl ReadableSnapshot) -> TypeDBValueType {
        match self {
            ValueType::Boolean => TypeDBValueType::Boolean,
            ValueType::Long => TypeDBValueType::Long,
            ValueType::Double => TypeDBValueType::Double,
            ValueType::Decimal => TypeDBValueType::Decimal,
            ValueType::Date => TypeDBValueType::Date,
            ValueType::DateTime => TypeDBValueType::DateTime,
            ValueType::DateTimeTZ => TypeDBValueType::DateTimeTZ,
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
}

impl FromStr for ValueType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "boolean" => Self::Boolean,
            "long" => Self::Long,
            "double" => Self::Double,
            "decimal" => Self::Decimal,
            "date" => Self::Date,
            "datetime" => Self::DateTime,
            "datetime-tz" => Self::DateTimeTZ,
            "duration" => Self::Duration,
            "string" => Self::String,
            _ => Self::Struct(Label { label_string: s.to_string() }),
        })
    }
}

#[derive(Debug, Default, Parameter)]
#[param(name = "value", regex = ".*?")]
pub(crate) struct Value {
    raw_value: String,
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

    const FRACTIONAL_ZEROES: usize = 18;

    pub fn into_typedb(self, value_type: TypeDBValueType) -> TypeDBValue<'static> {
        match value_type {
            TypeDBValueType::Boolean => TypeDBValue::Boolean(self.raw_value.parse().unwrap()),
            TypeDBValueType::Long => TypeDBValue::Long(self.raw_value.parse().unwrap()),
            TypeDBValueType::Double => TypeDBValue::Double(self.raw_value.parse().unwrap()),
            TypeDBValueType::Decimal => {
                let (integer, fractional) = if let Some(split) = self.raw_value.split_once(".") {
                    split
                } else {
                    (self.raw_value.as_str(), "0")
                };

                let integer_parsed = integer.trim().parse().unwrap();
                let fractional_parsed = Self::parse_decimal_fraction_part(fractional);
                if integer.starts_with('-') && integer_parsed == 0 {
                    TypeDBValue::Decimal(Decimal::new(-1, 0) + Decimal::new(0, fractional_parsed))
                } else {
                    TypeDBValue::Decimal(Decimal::new(integer_parsed, fractional_parsed))
                }
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

                if timezone.is_empty() {
                    TypeDBValue::DateTimeTZ(datetime.and_local_timezone(Tz::default()).unwrap())
                } else if timezone.starts_with('+') || timezone.starts_with('-') {
                    // TODO: Temporarily create a TZ for this format as well. It should be a separate DateTimeTZ format later!
                    let hours: i32 = timezone[1..3].parse().unwrap();
                    let minutes: i32 = timezone[3..].parse().unwrap();
                    let total_minutes = hours * 60 + minutes;
                    let fixed_offset = if &timezone[0..1] == "+" {
                        FixedOffset::east_opt(total_minutes * 60)
                    } else {
                        FixedOffset::west_opt(total_minutes * 60)
                    };
                    TypeDBValue::DateTimeTZ(
                        datetime.and_local_timezone(Self::fixed_offset_to_tz(fixed_offset.unwrap()).unwrap()).unwrap(),
                    )
                } else {
                    TypeDBValue::DateTimeTZ(datetime.and_local_timezone(timezone.parse().unwrap()).unwrap())
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
            TypeDBValueType::Struct(_) => todo!(),
        }
    }

    fn parse_decimal_fraction_part(value: &str) -> u64 {
        assert!(Self::FRACTIONAL_ZEROES >= value.len());
        10_u64.pow((Self::FRACTIONAL_ZEROES - value.len() + 1) as u32) * value.trim().parse::<u64>().unwrap()
    }

    fn parse_date_time_and_remainder(value: &str) -> (NaiveDateTime, &str) {
        for format in Self::DATETIME_FORMATS {
            if let Ok((datetime, remainder)) = NaiveDateTime::parse_and_remainder(&value, format) {
                return (datetime, remainder.trim());
            }
        }
        if let Ok((date, remainder)) = NaiveDate::parse_and_remainder(&value, Self::DATE_FORMAT) {
            return (date.and_time(NaiveTime::default()), remainder.trim());
        }
        panic!(
            "Cannot parse DateTime: none of the formats {:?} or {:?} fits for {:?}",
            Self::DATETIME_FORMATS,
            Self::DATE_FORMAT,
            value
        )
    }

    // TODO: A temporary hack
    fn fixed_offset_to_tz(offset: FixedOffset) -> Option<Tz> {
        // A predefined mapping of FixedOffset to Tz
        // This is a simplified example and may not cover all cases
        let offset_seconds = offset.local_minus_utc();
        match offset_seconds {
            0 => Some(chrono_tz::UTC),                      // UTC
            3600 => Some(chrono_tz::Europe::London),        // GMT+1
            7200 => Some(chrono_tz::Europe::Berlin),        // GMT+2
            36000 => Some(chrono_tz::Australia::Brisbane),  // GMT+10
            -36000 => Some(chrono_tz::Pacific::Honolulu),   // GMT-10
            600 => Some(chrono_tz::Australia::Adelaide),    // GMT+10:00 (Common in Oceania)
            -600 => Some(chrono_tz::Pacific::Pago_Pago),    // GMT-10:00 (Common in Pacific)
            -3600 => Some(chrono_tz::Atlantic::Cape_Verde), // GMT-1:00 (Common in Atlantic)
            180 => Some(chrono_tz::Etc::GMTPlus3),          // GMT+00:03
            -180 => Some(chrono_tz::Etc::GMTMinus3),        // GMT-00:03
            120 => Some(chrono_tz::Etc::GMTPlus2),          // GMT+00:02
            -120 => Some(chrono_tz::Etc::GMTMinus2),        // GMT-00:02
            60 => Some(chrono_tz::Etc::GMTPlus0),           // GMT+00:01 (Example for custom mapping)
            -60 => Some(chrono_tz::Etc::GMTMinus0),         // GMT-00:01 (Example for custom mapping)
            // Add more mappings as needed
            _ => None,
        }
    }
}

impl FromStr for Value {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { raw_value: s.to_owned() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "annotation", regex = r"@[a-z]+(?:\(.+\))?")]
pub(crate) struct Annotation {
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
            regex if regex.starts_with("@regex") => {
                assert!(
                    regex.starts_with(r#"@regex(""#) && regex.ends_with(r#"")"#),
                    r#"Invalid @regex format: {regex:?}. Expected "@regex("regex-here")""#
                );
                let regex = &regex[r#"@regex(""#.len()..regex.len() - r#"")"#.len()];
                TypeDBAnnotation::Regex(AnnotationRegex::new(regex.to_owned()))
            }
            card if card.starts_with("@card") => {
                assert!(
                    card.starts_with("@card(") && card.ends_with(')'),
                    r#"Invalid @card format: {card:?}. Expected "@card(min, max)""#
                );
                let card = card["@card(".len()..card.len() - ")".len()].trim();
                let (min, max) = card.split_once("..").map(|(min, max)| (min.trim(), max.trim())).unwrap();

                TypeDBAnnotation::Cardinality(AnnotationCardinality::new(
                    min.parse().unwrap(),
                    if max.is_empty() { None } else { Some(max.parse().unwrap()) },
                ))
            }
            values if values.starts_with("@values") => {
                assert!(
                    values.starts_with("@values(") && values.ends_with(')'),
                    r#"Invalid @values format: {values:?}. Expected "@values(val1, val2, ..., valN)""#
                );
                assert!(value_type.is_some(), "ValueType is expected to parse annotation @values");
                let value_type = value_type.unwrap();
                let values = values["@values(".len()..values.len() - ")".len()].trim();
                let values = values.split(',');
                TypeDBAnnotation::Values(AnnotationValues::new(
                    values
                        .map(|value| Value::from_str(value.trim()).unwrap().into_typedb(value_type.clone()))
                        .collect_vec(),
                ))
            }
            range if range.starts_with("@range") => {
                assert!(
                    range.starts_with("@range(") && range.ends_with(')') && range.contains(".."),
                    r#"Invalid @range format: {range:?}. Expected "@range(min..max)""#
                );
                assert!(value_type.is_some(), "ValueType is expected to parse annotation @range");
                let value_type = value_type.unwrap();
                let range = range["@range(".len()..range.len() - ")".len()].trim();
                let (min, max) = range.split_once("..").map(|(min, max)| (min.trim(), max.trim())).unwrap();
                TypeDBAnnotation::Range(AnnotationRange::new(
                    if min.is_empty() {
                        None
                    } else {
                        Some(Value::from_str(min).unwrap().into_typedb(value_type.clone()))
                    },
                    if max.is_empty() { None } else { Some(Value::from_str(max).unwrap().into_typedb(value_type)) },
                ))
            }
            subkey if subkey.starts_with("@subkey") => {
                unreachable!("Subkey is not implemented for tests!");
                // assert!(
                //     subkey.starts_with(r#"@subkey("#) && subkey.ends_with(r#")"#),
                //     r#"Invalid @subkey format: {subkey:?}. Expected "@subkey(LABEL)""#
                // );
                // let label = &subkey[r#"@subkey("#.len()..subkey.len() - r#")"#.len()];
                // TypeDBAnnotation::Subkey(AnnotationSubkey::new(label.to_owned()))
            }
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
pub(crate) struct AnnotationCategory {
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
pub(crate) struct Annotations {
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

#[derive(Debug, Parameter)]
#[param(name = "ordering", regex = "(unordered|ordered)")]
pub(crate) enum Ordering {
    Unordered,
    Ordered,
}

impl Ordering {
    pub fn into_typedb(&self) -> concept::type_::Ordering {
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

#[derive(Debug, Parameter)]
#[param(name = "optional", regex = "(|\\?)")]
pub(crate) enum Optional {
    False,
    True,
}

impl Optional {
    pub fn into_typedb(&self) -> bool {
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
