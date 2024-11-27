/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::HashSet,
    error::Error,
    fmt,
    fmt::Formatter,
    hash::Hash,
    iter::Sum,
    ops::Add,
};

use bytes::{byte_array::ByteArray, Bytes};
use encoding::{
    graph::type_::property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
    layout::infix::Infix,
    value::{value::Value, value_type::ValueType, ValueEncodable},
};
use regex::Regex;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{Deserialize, Serialize};

use crate::type_::{
    constraint::{CapabilityConstraint, ConstraintDescription, TypeConstraint},
    Capability, KindAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Annotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Independent(AnnotationIndependent),
    Unique(AnnotationUnique),
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
    Regex(AnnotationRegex),
    Cascade(AnnotationCascade),
    Range(AnnotationRange),
    Values(AnnotationValues),
    // TODO: Subkey
    // TODO: Replace
}

impl fmt::Display for Annotation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Annotation::Abstract(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Distinct(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Independent(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Unique(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Key(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Cardinality(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Regex(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Cascade(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Range(annotation) => fmt::Display::fmt(annotation, f),
            Annotation::Values(annotation) => fmt::Display::fmt(annotation, f),
        }
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationAbstract;

impl fmt::Display for AnnotationAbstract {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@abstract")
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationDistinct;

impl fmt::Display for AnnotationDistinct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@distinct")
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationUnique;

impl AnnotationUnique {
    pub fn value_type_valid(value_type: Option<ValueType>) -> bool {
        match value_type {
            Some(value_type) => value_type.keyable(),
            None => false,
        }
    }
}

impl fmt::Display for AnnotationUnique {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@unique")
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationKey;

impl AnnotationKey {
    pub fn value_type_valid(value_type: Option<ValueType>) -> bool {
        AnnotationUnique::value_type_valid(value_type)
    }
}

impl AnnotationKey {
    pub const UNIQUE: AnnotationUnique = AnnotationUnique;
    pub const CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(1, Some(1));
}

impl fmt::Display for AnnotationKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@key")
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationIndependent;

impl fmt::Display for AnnotationIndependent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@independent")
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCardinality {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    start_inclusive: u64,
    end_inclusive: Option<u64>,
}

impl AnnotationCardinality {
    pub const fn new(start_inclusive: u64, end_inclusive: Option<u64>) -> Self {
        Self { start_inclusive, end_inclusive }
    }

    // TODO: Can move to impl Default for AnnotationCardinality and other Annotations
    pub const fn default() -> Self {
        Self::unchecked()
    }

    pub const fn unchecked() -> Self {
        Self::new(0, None)
    }

    pub fn is_unchecked(&self) -> bool {
        self == &Self::unchecked()
    }

    pub fn is_bounded_to_one(&self) -> bool {
        self.end() == Some(1)
    }

    pub fn valid(&self) -> bool {
        match self.end_inclusive {
            Some(end_inclusive) if self.start_inclusive > end_inclusive => false,
            Some(end_inclusive) if self.start_inclusive == end_inclusive && end_inclusive == 0 => false,
            _ => true,
        }
    }

    pub fn value_valid(&self, value: u64) -> bool {
        self.value_satisfies_start(value) && self.value_satisfies_end(Some(value))
    }

    pub fn start(&self) -> u64 {
        self.start_inclusive
    }

    pub fn end(&self) -> Option<u64> {
        self.end_inclusive
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        self.value_satisfies_start(other.start()) && self.value_satisfies_end(other.end())
    }

    pub fn value_satisfies_start(&self, value: u64) -> bool {
        self.start_inclusive <= value
    }

    pub fn value_satisfies_end(&self, value: Option<u64>) -> bool {
        self.end_inclusive.unwrap_or(u64::MAX) >= value.unwrap_or(u64::MAX)
    }
}

impl Add for AnnotationCardinality {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let (lhs_start, lhs_end) = (self.start_inclusive, self.end_inclusive);
        let (rhs_start, rhs_end) = (rhs.start_inclusive, rhs.end_inclusive);

        let new_start = lhs_start + rhs_start;
        let new_end = match (lhs_end, rhs_end) {
            (None, None) => None,
            (Some(end), None) | (None, Some(end)) => Some(max(new_start, end)),
            (Some(lhs_end), Some(rhs_end)) => Some(max(new_start, min(lhs_end, rhs_end))),
        };

        Self::new(new_start, new_end)
    }
}

impl Sum for AnnotationCardinality {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |lhs, rhs| lhs + rhs)
    }
}

impl fmt::Display for AnnotationCardinality {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.end() {
            None => write!(f, "@card({}..)", self.start_inclusive),
            Some(end) => write!(f, "@card({}..{})", self.start_inclusive, end),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationRegex {
    regex: Cow<'static, str>,
}

impl AnnotationRegex {
    pub const fn new(regex: String) -> Self {
        Self { regex: Cow::Owned(regex) }
    }

    pub const fn default() -> Self {
        Self { regex: Cow::Borrowed(".*") }
    }

    pub fn regex(&self) -> &str {
        &self.regex
    }

    pub fn valid(&self) -> bool {
        !self.regex.is_empty() && Regex::new(&self.regex).is_ok()
    }

    pub fn value_valid(&self, value: &str) -> bool {
        Regex::new(&self.regex).is_ok_and(|regex| regex.is_match(value))
    }

    pub fn value_type_valid(value_type: Option<ValueType>) -> bool {
        matches!(value_type, Some(ValueType::String))
    }

    // TODO: Can try to implement the check, but allow everything now!
    pub fn narrowed_correctly_by(&self, _other: &Self) -> bool {
        true
    }
}

impl fmt::Display for AnnotationRegex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.regex)
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCascade;

impl fmt::Display for AnnotationCascade {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@cascade")
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationRange {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    start_inclusive: Option<Value<'static>>,
    end_inclusive: Option<Value<'static>>,
}

impl AnnotationRange {
    pub const fn new(start_inclusive: Option<Value<'static>>, end_inclusive: Option<Value<'static>>) -> Self {
        Self { start_inclusive, end_inclusive }
    }

    pub const fn default() -> Self {
        Self { start_inclusive: Some(Value::Boolean(false)), end_inclusive: Some(Value::Boolean(true)) }
    }

    pub fn start(&self) -> Option<Value<'static>> {
        self.start_inclusive.clone()
    }

    pub fn end(&self) -> Option<Value<'static>> {
        self.end_inclusive.clone()
    }

    // TODO: We might want to return different errors for incorrect order / unmatched value types
    pub fn valid(&self, value_type: Option<ValueType>) -> bool {
        match &self.start_inclusive {
            None => match &self.end_inclusive {
                None => false,
                Some(end_inclusive) => {
                    let end_value_type = end_inclusive.value_type();
                    value_type.unwrap_or(end_value_type.clone()) == end_value_type
                }
            },
            Some(start_inclusive) => match &self.end_inclusive {
                None => {
                    let start_value_type = start_inclusive.value_type();
                    value_type.unwrap_or(start_value_type.clone()) == start_value_type
                }
                Some(end_inclusive) => {
                    if start_inclusive.value_type() != end_inclusive.value_type() {
                        return false;
                    }
                    if value_type.unwrap_or(start_inclusive.value_type()) != start_inclusive.value_type() {
                        return false;
                    }

                    match start_inclusive {
                        Value::Boolean(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_boolean(),
                        Value::Long(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_long(),
                        Value::Double(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_double(),
                        Value::Decimal(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_decimal(),
                        Value::Date(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_date(),
                        Value::DateTime(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_date_time(),
                        Value::DateTimeTZ(start_inclusive) => {
                            start_inclusive < &end_inclusive.clone().unwrap_date_time_tz()
                        }
                        Value::String(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_string(),
                        Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
                        Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
                    }
                }
            },
        }
    }

    pub fn value_valid(&self, value: Value<'_>) -> bool {
        self.value_satisfies_start(Some(value.clone())) && self.value_satisfies_end(Some(value))
    }

    pub fn value_type_valid(value_type: Option<ValueType>) -> bool {
        match value_type {
            Some(value_type) => match &value_type {
                | ValueType::Boolean
                | ValueType::Long
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::String => true,

                | ValueType::Duration | ValueType::Struct(_) => false,
            },
            None => false,
        }
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        self.value_satisfies_start(other.start()) && self.value_satisfies_end(other.end())
    }

    fn value_satisfies_start(&self, value: Option<Value<'_>>) -> bool {
        match self.start() {
            None => true,
            Some(start) => match &value {
                None => false,
                Some(value) => match value {
                    Value::Boolean(value) => &start.unwrap_boolean() <= value,
                    Value::Long(value) => &start.unwrap_long() <= value,
                    Value::Double(value) => &start.unwrap_double() <= value,
                    Value::Decimal(value) => &start.unwrap_decimal() <= value,
                    Value::Date(value) => &start.unwrap_date() <= value,
                    Value::DateTime(value) => &start.unwrap_date_time() <= value,
                    Value::DateTimeTZ(value) => &start.unwrap_date_time_tz() <= value,
                    Value::String(value) => &start.unwrap_string() <= value,
                    Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
                    Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
                },
            },
        }
    }

    fn value_satisfies_end(&self, value: Option<Value<'_>>) -> bool {
        match self.end() {
            None => true,
            Some(end) => match &value {
                None => false,
                Some(value) => match value {
                    Value::Boolean(value) => &end.unwrap_boolean() >= value,
                    Value::Long(value) => &end.unwrap_long() >= value,
                    Value::Double(value) => &end.unwrap_double() >= value,
                    Value::Decimal(value) => &end.unwrap_decimal() >= value,
                    Value::Date(value) => &end.unwrap_date() >= value,
                    Value::DateTime(value) => &end.unwrap_date_time() >= value,
                    Value::DateTimeTZ(value) => &end.unwrap_date_time_tz() >= value,
                    Value::String(value) => &end.unwrap_string() >= value,
                    Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
                    Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
                },
            },
        }
    }
}

impl fmt::Display for AnnotationRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match (self.start(), self.end()) {
            (Some(start), Some(end)) => write!(f, "@range({start}..{end})"),
            (Some(start), None) => write!(f, "@range({start}..)"),
            (None, Some(end)) => write!(f, "@range(..{end})"),
            (None, None) => unreachable!("Empty range @range(..) should never be written to the schema - start or end should always be specified."),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationValues {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    values: Vec<Value<'static>>,
}

impl AnnotationValues {
    pub const fn new(values: Vec<Value<'static>>) -> Self {
        Self { values }
    }

    pub const fn default() -> Self {
        Self { values: vec![] }
    }

    pub fn values(&self) -> &[Value<'static>] {
        &self.values
    }

    // TODO: We might want to return different errors for empty / unmatched value types
    pub fn valid(&self, expected_value_type: Option<ValueType>) -> bool {
        if self.values.is_empty() {
            return false;
        }

        let unique_value_types: HashSet<ValueType> = self.values.iter().map(|value| value.value_type()).collect();
        assert!(!unique_value_types.is_empty());
        if unique_value_types.len() > 1 {
            return false;
        }
        if expected_value_type.is_some()
            && unique_value_types.iter().any(|value_type| value_type != &expected_value_type.clone().unwrap())
        {
            return false;
        }

        // Value does not implement Hash, so we run a N^2 loop here expecting a limited number of values
        let values = &self.values;
        for i in 0..values.len() {
            for j in i + 1..values.len() {
                if values[i] == values[j] {
                    return false;
                }
            }
        }

        true
    }

    pub fn value_valid(&self, value: Value<'_>) -> bool {
        self.contains(&value)
    }

    pub fn value_type_valid(value_type: Option<ValueType>) -> bool {
        match value_type {
            Some(value_type) => match &value_type {
                | ValueType::Boolean
                | ValueType::Long
                | ValueType::Double
                | ValueType::Decimal
                | ValueType::Date
                | ValueType::DateTime
                | ValueType::DateTimeTZ
                | ValueType::Duration
                | ValueType::String => true,

                | ValueType::Struct(_) => false,
            },
            None => false,
        }
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        for other_value in &other.values {
            if !self.contains(other_value) {
                return false;
            }
        }
        true
    }

    fn contains(&self, value: &Value<'_>) -> bool {
        self.values.contains(value)
    }
}

impl fmt::Display for AnnotationValues {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        assert!(!self.values.is_empty());
        write!(f, "@values({}", self.values[0])?;
        for value in &self.values[1..] {
            write!(f, ", {value}")?;
        }
        write!(f, ")")
    }
}

impl Annotation {
    pub fn category(&self) -> AnnotationCategory {
        match self {
            Self::Abstract(_) => AnnotationCategory::Abstract,
            Self::Distinct(_) => AnnotationCategory::Distinct,
            Self::Independent(_) => AnnotationCategory::Independent,
            Self::Unique(_) => AnnotationCategory::Unique,
            Self::Key(_) => AnnotationCategory::Key,
            Self::Cardinality(_) => AnnotationCategory::Cardinality,
            Self::Regex(_) => AnnotationCategory::Regex,
            Self::Cascade(_) => AnnotationCategory::Cascade,
            Self::Range(_) => AnnotationCategory::Range,
            Self::Values(_) => AnnotationCategory::Values,
        }
    }

    pub fn to_type_constraints<T: KindAPI<'static>>(&self, source: T) -> HashSet<TypeConstraint<T>> {
        self.clone().into_type_constraints(source)
    }

    pub fn to_capability_constraints<CAP: Capability<'static>>(
        &self,
        source: CAP,
    ) -> HashSet<CapabilityConstraint<CAP>> {
        self.clone().into_capability_constraints(source)
    }

    pub fn into_type_constraints<T: KindAPI<'static>>(self, source: T) -> HashSet<TypeConstraint<T>> {
        ConstraintDescription::from_annotation(self)
            .into_iter()
            .map(|description| TypeConstraint::new(description.clone(), source.clone()))
            .collect()
    }

    pub fn into_capability_constraints<CAP: Capability<'static>>(
        self,
        source: CAP,
    ) -> HashSet<CapabilityConstraint<CAP>> {
        ConstraintDescription::from_annotation(self)
            .into_iter()
            .map(|description| CapabilityConstraint::new(description.clone(), source.clone()))
            .collect()
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum AnnotationCategory {
    Abstract,
    Distinct,
    Independent,
    Unique,
    Key,
    Cardinality,
    Regex,
    Cascade,
    Range,
    Values,
    // TODO: Subkey
    // TODO: Replace
}

impl AnnotationCategory {
    const fn to_default(self) -> Annotation {
        match self {
            AnnotationCategory::Abstract => Annotation::Abstract(AnnotationAbstract),
            AnnotationCategory::Distinct => Annotation::Distinct(AnnotationDistinct),
            AnnotationCategory::Independent => Annotation::Independent(AnnotationIndependent),
            AnnotationCategory::Unique => Annotation::Unique(AnnotationUnique),
            AnnotationCategory::Key => Annotation::Key(AnnotationKey),
            AnnotationCategory::Cardinality => Annotation::Cardinality(AnnotationCardinality::default()),
            AnnotationCategory::Regex => Annotation::Regex(AnnotationRegex::default()),
            AnnotationCategory::Cascade => Annotation::Cascade(AnnotationCascade),
            AnnotationCategory::Range => Annotation::Range(AnnotationRange::default()),
            AnnotationCategory::Values => Annotation::Values(AnnotationValues::default()),
        }
    }

    pub fn declarable_alongside(&self, other: AnnotationCategory) -> bool {
        match self {
            AnnotationCategory::Unique => !matches!(other, AnnotationCategory::Key),
            AnnotationCategory::Cardinality => !matches!(other, AnnotationCategory::Key),
            AnnotationCategory::Key => !matches!(other, AnnotationCategory::Unique | AnnotationCategory::Cardinality),
            | AnnotationCategory::Abstract
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Regex
            | AnnotationCategory::Cascade
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }

    pub fn has_parameter(&self) -> bool {
        match self {
            | AnnotationCategory::Abstract
            | AnnotationCategory::Key
            | AnnotationCategory::Unique
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Cascade => false,

            | AnnotationCategory::Cardinality
            | AnnotationCategory::Regex
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }

    pub fn name(&self) -> &'static str {
        // TODO: use TypeQL structures
        match self {
            AnnotationCategory::Abstract => "@abstract",
            AnnotationCategory::Distinct => "@distinct",
            AnnotationCategory::Independent => "@independent",
            AnnotationCategory::Unique => "@unique",
            AnnotationCategory::Key => "@key",
            AnnotationCategory::Cardinality => "@card",
            AnnotationCategory::Regex => "@regex",
            AnnotationCategory::Cascade => "@cascade",
            AnnotationCategory::Range => "@range",
            AnnotationCategory::Values => "@values",
        }
    }
}

impl fmt::Display for AnnotationCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Debug for AnnotationCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

pub trait DefaultFrom<FromType, ErrorType> {
    fn try_getting_default(from: FromType) -> Result<Self, ErrorType>
    where
        Self: Sized;
}

impl<T> DefaultFrom<AnnotationCategory, AnnotationError> for T
where
    T: TryFrom<Annotation, Error = AnnotationError>,
{
    // Note: creating default annotation from category is a workaround for creating new category types per Attribute/Entity/Relation/etc
    fn try_getting_default(from: AnnotationCategory) -> Result<Self, AnnotationError> {
        from.to_default().try_into()
    }
}

macro_rules! empty_type_vertex_property_encoding {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeVertexPropertyEncoding<'a> for $property {
            const INFIX: Infix = Infix::$infix;

            fn from_value_bytes(value: &[u8]) -> $property {
                debug_assert!(value.is_empty());
                $property
            }

            fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

// It is a sign of poor architecture, but it lets us wrap other general places for annotations,
// otherwise we'd need to write "unreachable"s on every caller's side
macro_rules! unreachable_type_vertex_property_encoding {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeVertexPropertyEncoding<'a> for $property {
            const INFIX: Infix = Infix::$infix;

            fn from_value_bytes(_: &[u8]) -> $property {
                unreachable!("TypeVertexPropertyEncoding is not be implemented for {}", stringify!($property))
            }

            fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                unreachable!("TypeVertexPropertyEncoding is not be implemented for {}", stringify!($property))
            }
        }
    };
}

unreachable_type_vertex_property_encoding!(AnnotationDistinct, PropertyAnnotationDistinct);
unreachable_type_vertex_property_encoding!(AnnotationUnique, PropertyAnnotationUnique);
unreachable_type_vertex_property_encoding!(AnnotationKey, PropertyAnnotationKey);
unreachable_type_vertex_property_encoding!(AnnotationCardinality, PropertyAnnotationCardinality);

empty_type_vertex_property_encoding!(AnnotationAbstract, PropertyAnnotationAbstract);
empty_type_vertex_property_encoding!(AnnotationIndependent, PropertyAnnotationIndependent);
empty_type_vertex_property_encoding!(AnnotationCascade, PropertyAnnotationCascade);

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationRegex {
    const INFIX: Infix = Infix::PropertyAnnotationRegex;

    fn from_value_bytes(value: &[u8]) -> AnnotationRegex {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        AnnotationRegex::new(std::str::from_utf8(value).unwrap().to_owned())
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(self.regex().as_bytes())))
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationRange {
    const INFIX: Infix = Infix::PropertyAnnotationRange;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationValues {
    const INFIX: Infix = Infix::PropertyAnnotationValues;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

macro_rules! empty_type_edge_property_encoder {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeEdgePropertyEncoding<'a> for $property {
            const INFIX: Infix = Infix::$infix;

            fn from_value_bytes(value: &[u8]) -> $property {
                debug_assert!(value.is_empty());
                $property
            }

            fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

// It is a sign of poor architecture, but it lets us wrap other general places for annotations,
// otherwise we'd need to write "unreachable"s on every caller's side
macro_rules! unreachable_type_edge_property_encoder {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeEdgePropertyEncoding<'a> for $property {
            const INFIX: Infix = Infix::$infix;

            fn from_value_bytes(_value: &[u8]) -> $property {
                unreachable!("TypeEdgePropertyEncoding is not be implemented for {}", stringify!($property))
            }

            fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                unreachable!("TypeEdgePropertyEncoding is not be implemented for {}", stringify!($property))
            }
        }
    };
}

unreachable_type_edge_property_encoder!(AnnotationIndependent, PropertyAnnotationIndependent);
unreachable_type_edge_property_encoder!(AnnotationCascade, PropertyAnnotationCascade);

empty_type_edge_property_encoder!(AnnotationAbstract, PropertyAnnotationAbstract);
empty_type_edge_property_encoder!(AnnotationDistinct, PropertyAnnotationDistinct);
empty_type_edge_property_encoder!(AnnotationUnique, PropertyAnnotationUnique);
empty_type_edge_property_encoder!(AnnotationKey, PropertyAnnotationKey);

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationCardinality {
    const INFIX: Infix = Infix::PropertyAnnotationCardinality;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationRegex {
    const INFIX: Infix = Infix::PropertyAnnotationRegex;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        AnnotationRegex::new(std::str::from_utf8(value).unwrap().to_owned())
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(self.regex().as_bytes())))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationRange {
    const INFIX: Infix = Infix::PropertyAnnotationRange;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationValues {
    const INFIX: Infix = Infix::PropertyAnnotationValues;
    fn from_value_bytes(value: &[u8]) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

#[derive(Debug, Clone)]
pub enum AnnotationError {
    UnsupportedAnnotationForEntityType(AnnotationCategory),
    UnsupportedAnnotationForRelationType(AnnotationCategory),
    UnsupportedAnnotationForAttributeType(AnnotationCategory),
    UnsupportedAnnotationForRoleType(AnnotationCategory),
    UnsupportedAnnotationForRelates(AnnotationCategory),
    UnsupportedAnnotationForPlays(AnnotationCategory),
    UnsupportedAnnotationForOwns(AnnotationCategory),
    UnsupportedAnnotationForAlias(AnnotationCategory),
    UnsupportedAnnotationForSub(AnnotationCategory),
    UnsupportedAnnotationForValueType(AnnotationCategory),
}

impl fmt::Display for AnnotationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for AnnotationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedAnnotationForEntityType(_) => None,
            Self::UnsupportedAnnotationForRelationType(_) => None,
            Self::UnsupportedAnnotationForAttributeType(_) => None,
            Self::UnsupportedAnnotationForRoleType(_) => None,
            Self::UnsupportedAnnotationForRelates(_) => None,
            Self::UnsupportedAnnotationForPlays(_) => None,
            Self::UnsupportedAnnotationForOwns(_) => None,
            Self::UnsupportedAnnotationForAlias(_) => None,
            Self::UnsupportedAnnotationForSub(_) => None,
            Self::UnsupportedAnnotationForValueType(_) => None,
        }
    }
}

mod serialize_annotation {
    use std::{borrow::Cow, fmt};

    use bytes::Bytes;
    use encoding::value::{
        boolean_bytes::BooleanBytes, date_bytes::DateBytes, date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes, decimal_bytes::DecimalBytes, double_bytes::DoubleBytes,
        duration_bytes::DurationBytes, long_bytes::LongBytes, string_bytes::StringBytes, value::Value,
        value_type::ValueTypeCategory, ValueEncodable,
    };
    use resource::constants::encoding::AD_HOC_BYTES_INLINE;
    use serde::{
        de,
        de::{MapAccess, SeqAccess, Visitor},
        ser::SerializeStruct,
        Deserialize, Deserializer, Serialize, Serializer,
    };

    use crate::type_::annotation::{AnnotationRange, AnnotationValues};

    fn serialize_value(value: Value<'_>) -> Vec<u8> {
        match value.value_type().category() {
            | ValueTypeCategory::Boolean
            | ValueTypeCategory::Long
            | ValueTypeCategory::Double
            | ValueTypeCategory::Decimal
            | ValueTypeCategory::Date
            | ValueTypeCategory::DateTime
            | ValueTypeCategory::DateTimeTZ
            | ValueTypeCategory::Duration
            | ValueTypeCategory::String => value.encode_bytes::<AD_HOC_BYTES_INLINE>().to_vec(),
            ValueTypeCategory::Struct => unreachable!("Structs are not supported in annotation serialization"),
        }
    }

    fn deserialize_value(bytes: &[u8], value_type_category: ValueTypeCategory) -> Value<'static> {
        match value_type_category {
            ValueTypeCategory::Boolean => Value::Boolean(BooleanBytes::new(bytes.try_into().unwrap()).as_bool()),
            ValueTypeCategory::Long => Value::Long(LongBytes::new(bytes.try_into().unwrap()).as_i64()),
            ValueTypeCategory::Double => Value::Double(DoubleBytes::new(bytes.try_into().unwrap()).as_f64()),
            ValueTypeCategory::Decimal => Value::Decimal(DecimalBytes::new(bytes.try_into().unwrap()).as_decimal()),
            ValueTypeCategory::Date => Value::Date(DateBytes::new(bytes.try_into().unwrap()).as_naive_date()),
            ValueTypeCategory::DateTime => {
                Value::DateTime(DateTimeBytes::new(bytes.try_into().unwrap()).as_naive_date_time())
            }
            ValueTypeCategory::DateTimeTZ => {
                Value::DateTimeTZ(DateTimeTZBytes::new(bytes.try_into().unwrap()).as_date_time())
            }
            ValueTypeCategory::Duration => Value::Duration(DurationBytes::new(bytes.try_into().unwrap()).as_duration()),
            ValueTypeCategory::String => Value::String(Cow::Owned(
                StringBytes::new(Bytes::<AD_HOC_BYTES_INLINE>::copy(bytes)).as_str().to_owned(),
            )),
            ValueTypeCategory::Struct => unreachable!("Structs are not supported in annotation deserialization"),
        }
    }

    enum RangeField {
        ValueTypeCategory,
        StartInclusive,
        EndInclusive,
    }

    impl RangeField {
        const NAMES: [&'static str; 3] =
            [Self::ValueTypeCategory.name(), Self::StartInclusive.name(), Self::EndInclusive.name()];

        const fn name(&self) -> &str {
            match self {
                RangeField::ValueTypeCategory => "ValueTypeCategory",
                RangeField::StartInclusive => "StartInclusive",
                RangeField::EndInclusive => "EndInclusive",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "ValueTypeCategory" => Some(RangeField::ValueTypeCategory),
                "StartInclusive" => Some(RangeField::StartInclusive),
                "EndInclusive" => Some(RangeField::EndInclusive),
                _ => None,
            }
        }
    }

    fn serialize_annotation_range_value_field(value: Option<Value<'_>>) -> Option<Vec<u8>> {
        let value = value?;
        match value.value_type().category() {
            | ValueTypeCategory::Boolean
            | ValueTypeCategory::Long
            | ValueTypeCategory::Double
            | ValueTypeCategory::Decimal
            | ValueTypeCategory::Date
            | ValueTypeCategory::DateTime
            | ValueTypeCategory::DateTimeTZ
            | ValueTypeCategory::String => Some(serialize_value(value.clone())),
            ValueTypeCategory::Duration => unreachable!("Can't use duration for AnnotationRange"),
            ValueTypeCategory::Struct => unreachable!("Can't use struct for AnnotationRange"),
        }
    }

    fn deserialize_annotation_range_value_field(
        bytes_opt: Option<&[u8]>,
        value_type_category: ValueTypeCategory,
    ) -> Option<Value<'static>> {
        let bytes = bytes_opt?;
        match &value_type_category {
            | ValueTypeCategory::Boolean
            | ValueTypeCategory::Long
            | ValueTypeCategory::Double
            | ValueTypeCategory::Decimal
            | ValueTypeCategory::Date
            | ValueTypeCategory::DateTime
            | ValueTypeCategory::DateTimeTZ
            | ValueTypeCategory::String => Some(deserialize_value(bytes, value_type_category)),
            ValueTypeCategory::Duration => unreachable!("Can't use duration for AnnotationRange"),
            ValueTypeCategory::Struct => unreachable!("Can't use struct for AnnotationRange"),
        }
    }

    impl Serialize for AnnotationRange {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("AnnotationRange", RangeField::NAMES.len())?;

            assert!(self.start_inclusive.is_some() || self.end_inclusive.is_some());
            let value_type_category = self
                .start_inclusive
                .clone()
                .unwrap_or(self.end_inclusive.clone().unwrap_or(Value::Boolean(false)))
                .value_type()
                .category();
            state.serialize_field(RangeField::ValueTypeCategory.name(), &value_type_category.to_bytes())?;

            state.serialize_field(
                RangeField::StartInclusive.name(),
                &serialize_annotation_range_value_field(self.start_inclusive.clone()),
            )?;
            state.serialize_field(
                RangeField::EndInclusive.name(),
                &serialize_annotation_range_value_field(self.end_inclusive.clone()),
            )?;
            state.end()
        }
    }

    impl<'de> Deserialize<'de> for AnnotationRange {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for RangeField {
                fn deserialize<D>(deserializer: D) -> Result<RangeField, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = RangeField;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<RangeField, E>
                        where
                            E: de::Error,
                        {
                            RangeField::from(value).ok_or_else(|| de::Error::unknown_field(value, &RangeField::NAMES))
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct AnnotationRangeVisitor;

            impl<'de> Visitor<'de> for AnnotationRangeVisitor {
                type Value = AnnotationRange;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct AnnotationRangeVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<AnnotationRange, V::Error>
                where
                    V: SeqAccess<'de>,
                {
                    let value_type_category = ValueTypeCategory::from_bytes(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                    );

                    let start_inclusive = deserialize_annotation_range_value_field(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                        value_type_category,
                    );
                    let end_inclusive = deserialize_annotation_range_value_field(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        value_type_category,
                    );

                    Ok(AnnotationRange { start_inclusive, end_inclusive })
                }

                fn visit_map<V>(self, mut map: V) -> Result<AnnotationRange, V::Error>
                where
                    V: MapAccess<'de>,
                {
                    let mut value_type_category: Option<ValueTypeCategory> = None;
                    let mut start_inclusive: Option<Option<Value<'static>>> = None;
                    let mut end_inclusive: Option<Option<Value<'static>>> = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            RangeField::ValueTypeCategory => {
                                if value_type_category.is_some() {
                                    return Err(de::Error::duplicate_field(ValuesField::ValueTypeCategory.name()));
                                }
                                value_type_category = Some(ValueTypeCategory::from_bytes(map.next_value()?));
                            }
                            RangeField::StartInclusive => {
                                if value_type_category.is_none() {
                                    return Err(de::Error::missing_field(ValuesField::ValueTypeCategory.name()));
                                }
                                if start_inclusive.is_some() {
                                    return Err(de::Error::duplicate_field(RangeField::StartInclusive.name()));
                                }
                                start_inclusive = Some(deserialize_annotation_range_value_field(
                                    map.next_value()?,
                                    value_type_category.unwrap(),
                                ));
                            }
                            RangeField::EndInclusive => {
                                if value_type_category.is_none() {
                                    return Err(de::Error::missing_field(ValuesField::ValueTypeCategory.name()));
                                }
                                if end_inclusive.is_some() {
                                    return Err(de::Error::duplicate_field(RangeField::EndInclusive.name()));
                                }
                                end_inclusive = Some(deserialize_annotation_range_value_field(
                                    map.next_value()?,
                                    value_type_category.unwrap(),
                                ));
                            }
                        }
                    }

                    Ok(AnnotationRange {
                        start_inclusive: start_inclusive
                            .ok_or_else(|| de::Error::missing_field(RangeField::StartInclusive.name()))?,
                        end_inclusive: end_inclusive
                            .ok_or_else(|| de::Error::missing_field(RangeField::EndInclusive.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("AnnotationRange", &RangeField::NAMES, AnnotationRangeVisitor)
        }
    }

    enum ValuesField {
        ValueTypeCategory,
        Values,
    }

    impl ValuesField {
        const NAMES: [&'static str; 2] = [Self::ValueTypeCategory.name(), Self::Values.name()];

        const fn name(&self) -> &str {
            match self {
                ValuesField::ValueTypeCategory => "ValueTypeCategory",
                ValuesField::Values => "Values",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "ValueTypeCategory" => Some(ValuesField::ValueTypeCategory),
                "Values" => Some(ValuesField::Values),
                _ => None,
            }
        }
    }

    fn serialize_annotation_values_value_field(values: &[Value<'_>]) -> Vec<Vec<u8>> {
        values
            .iter()
            .map(|value| match value {
                | Value::Boolean(_)
                | Value::Long(_)
                | Value::Double(_)
                | Value::Decimal(_)
                | Value::Date(_)
                | Value::DateTime(_)
                | Value::DateTimeTZ(_)
                | Value::String(_)
                | Value::Duration(_) => value.encode_bytes::<AD_HOC_BYTES_INLINE>().to_vec(),
                Value::Struct(_) => unreachable!("Can't use struct for AnnotationValues"),
            })
            .collect()
    }

    fn deserialize_annotation_values_value_field(
        bytes_vec: Vec<&[u8]>,
        value_type_category: ValueTypeCategory,
    ) -> Vec<Value<'static>> {
        bytes_vec.iter().map(|bytes| deserialize_value(bytes, value_type_category)).collect()
    }

    impl Serialize for AnnotationValues {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("AnnotationValues", ValuesField::NAMES.len())?;

            assert!(!self.values.is_empty());
            let value_type_category = self.values.first().unwrap_or(&Value::Boolean(false)).value_type().category();
            state.serialize_field(ValuesField::ValueTypeCategory.name(), &value_type_category.to_bytes())?;

            state
                .serialize_field(ValuesField::Values.name(), &serialize_annotation_values_value_field(&self.values))?;
            state.end()
        }
    }

    impl<'de> Deserialize<'de> for AnnotationValues {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for ValuesField {
                fn deserialize<D>(deserializer: D) -> Result<ValuesField, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = ValuesField;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<ValuesField, E>
                        where
                            E: de::Error,
                        {
                            ValuesField::from(value).ok_or_else(|| de::Error::unknown_field(value, &ValuesField::NAMES))
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct AnnotationValuesVisitor;

            impl<'de> Visitor<'de> for AnnotationValuesVisitor {
                type Value = AnnotationValues;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct AnnotationValuesVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<AnnotationValues, V::Error>
                where
                    V: SeqAccess<'de>,
                {
                    let value_type_category = ValueTypeCategory::from_bytes(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                    );

                    let values = deserialize_annotation_values_value_field(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                        value_type_category,
                    );

                    Ok(AnnotationValues { values })
                }

                fn visit_map<V>(self, mut map: V) -> Result<AnnotationValues, V::Error>
                where
                    V: MapAccess<'de>,
                {
                    let mut value_type_category: Option<ValueTypeCategory> = None;
                    let mut values: Option<Vec<Value<'static>>> = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            ValuesField::ValueTypeCategory => {
                                if value_type_category.is_some() {
                                    return Err(de::Error::duplicate_field(ValuesField::ValueTypeCategory.name()));
                                }
                                value_type_category = Some(ValueTypeCategory::from_bytes(map.next_value()?));
                            }
                            ValuesField::Values => {
                                if value_type_category.is_none() {
                                    return Err(de::Error::missing_field(ValuesField::ValueTypeCategory.name()));
                                }
                                if values.is_some() {
                                    return Err(de::Error::duplicate_field(ValuesField::Values.name()));
                                }
                                values = Some(deserialize_annotation_values_value_field(
                                    map.next_value()?,
                                    value_type_category.unwrap(),
                                ));
                            }
                        }
                    }

                    Ok(AnnotationValues {
                        values: values.ok_or_else(|| de::Error::missing_field(ValuesField::Values.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("AnnotationValues", &ValuesField::NAMES, AnnotationValuesVisitor)
        }
    }
}
