/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, error::Error, fmt};
use std::hash::{Hash, Hasher};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use encoding::{
    graph::type_::property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
    layout::infix::{
        Infix,
        Infix::{
            PropertyAnnotationAbstract, PropertyAnnotationCascade, PropertyAnnotationDistinct,
            PropertyAnnotationIndependent, PropertyAnnotationKey, PropertyAnnotationUnique,
        },
    },
};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{Deserialize, Serialize};
use encoding::value::value::Value;
use encoding::value::ValueEncodable;

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

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationAbstract;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationDistinct;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationUnique;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationKey;

impl AnnotationKey {
    pub const CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(1, Some(1));
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationIndependent;

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

    pub const fn default() -> Self {
        Self::new(0, Some(1))
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

    fn value_satisfies_start(&self, value: u64) -> bool {
        self.start_inclusive <= value
    }

    fn value_satisfies_end(&self, value: Option<u64>) -> bool {
        self.end_inclusive.unwrap_or(u64::MAX) >= value.unwrap_or(u64::MAX)
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
        !self.regex.is_empty()
    }

    // TODO: value_valid
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCascade;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
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
    pub fn valid(&self) -> bool {
        match &self.start_inclusive {
            None => match &self.end_inclusive {
                None => false,
                Some(_) => true,
            }
            Some(start_inclusive) => match &self.end_inclusive {
                None => true,
                Some(end_inclusive) => {
                    if start_inclusive.value_type() != end_inclusive.value_type() {
                        return false;
                    }
                    match start_inclusive {
                        Value::Boolean(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_boolean(),
                        Value::Long(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_long(),
                        Value::Double(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_double(),
                        Value::Decimal(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_decimal(),
                        Value::Date(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_date(),
                        Value::DateTime(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_date_time(),
                        Value::DateTimeTZ(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_date_time_tz(),
                        Value::String(start_inclusive) => start_inclusive < &end_inclusive.clone().unwrap_string(),
                        Value::Duration(start_inclusive) => unreachable!("Cannot use duration for AnnotationRange"),
                        Value::Struct(start_inclusive) => unreachable!("Cannot use structs for AnnotationRange"),
                    }
                }
            }
        }
    }

    pub fn value_valid(&self, value: Value<'static>) -> bool {
        self.value_satisfies_start(Some(value.clone())) && self.value_satisfies_end(Some(value))
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        self.value_satisfies_start(other.start()) && self.value_satisfies_end(other.end())
    }

    fn value_satisfies_start(&self, value: Option<Value<'static>>) -> bool {
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
                }
            }
        }
    }

    fn value_satisfies_end(&self, value: Option<Value<'static>>) -> bool {
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
                }
            }
        }
    }
}

impl Hash for AnnotationRange {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value::hash_value_opt(&self.start_inclusive, state);
        hash_value::hash_value_opt(&self.end_inclusive, state);
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
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
    pub fn valid(&self) -> bool {
        let first = self.values.first();
        match first {
            None => false,
            Some(first_value) => {
                let first_value_type = first_value.value_type();
                for value in &self.values {
                    if value.value_type() != first_value_type {
                        return false;
                    }
                }
                true
            }
        }
    }

    pub fn value_valid(&self, value: Value<'static>) -> bool {
        self.contains(&value)
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        for other_value in &other.values {
            if !self.contains(&other_value) {
                return false;
            }
        }
        true
    }

    fn contains(&self, value: &Value<'static>) -> bool {
        self.values.contains(value)
    }
}

impl Hash for AnnotationValues {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value::hash_value_vec(&self.values, state);
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
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
    const fn to_default(&self) -> Annotation {
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
            AnnotationCategory::Unique => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            AnnotationCategory::Cardinality => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            AnnotationCategory::Key => match other {
                | AnnotationCategory::Unique | AnnotationCategory::Cardinality => false,
                _ => true,
            },
            | AnnotationCategory::Abstract
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Regex
            | AnnotationCategory::Cascade
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }

    pub fn declarable_below(&self, other: AnnotationCategory) -> bool {
        match self {
            AnnotationCategory::Unique => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            AnnotationCategory::Cardinality => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            | AnnotationCategory::Abstract
            | AnnotationCategory::Key
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Regex
            | AnnotationCategory::Cascade
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }

    pub fn inheritable_alongside(&self, other: AnnotationCategory) -> bool {
        // Note: this function implies that all the compared annotations already processed
        // the type manager validations (other "declarable" methods) and only considers
        // valid inheritance scenarios.
        match self {
            AnnotationCategory::Unique => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            AnnotationCategory::Cardinality => match other {
                AnnotationCategory::Key => false,
                _ => true,
            },
            | AnnotationCategory::Abstract
            | AnnotationCategory::Key
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Regex
            | AnnotationCategory::Cascade
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }

    pub fn inheritable(&self) -> bool {
        match self {
            AnnotationCategory::Abstract => false,

            | AnnotationCategory::Key
            | AnnotationCategory::Unique
            | AnnotationCategory::Cardinality
            | AnnotationCategory::Distinct
            | AnnotationCategory::Independent
            | AnnotationCategory::Regex
            | AnnotationCategory::Cascade
            | AnnotationCategory::Range
            | AnnotationCategory::Values => true,
        }
    }
}

pub trait DefaultFrom<FromType, ErrorType> {
    fn try_getting_default(from: FromType) -> Result<Self, ErrorType>
        where
            Self: Sized;
}

impl<T> DefaultFrom<AnnotationCategory, AnnotationError> for T
    where
        Result<T, AnnotationError>: From<Annotation>,
{
    // Note: creating default annotation from category is a workaround for creating new category types per Attribute/Entity/Relation/etc
    fn try_getting_default(from: AnnotationCategory) -> Result<Self, AnnotationError> {
        from.to_default().into()
    }
}

macro_rules! empty_type_vertex_property_encoding {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeVertexPropertyEncoding<'a> for $property {
            const INFIX: Infix = $infix;

            fn from_value_bytes<'b>(value: ByteReference<'b>) -> $property {
                debug_assert!(value.bytes().is_empty());
                $property
            }

            fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

empty_type_vertex_property_encoding!(AnnotationAbstract, PropertyAnnotationAbstract);
empty_type_vertex_property_encoding!(AnnotationIndependent, PropertyAnnotationIndependent);
empty_type_vertex_property_encoding!(AnnotationDistinct, PropertyAnnotationDistinct);

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationCardinality {
    const INFIX: Infix = Infix::PropertyAnnotationCardinality;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationRegex {
    const INFIX: Infix = Infix::PropertyAnnotationRegex;

    fn from_value_bytes<'b>(value: ByteReference<'b>) -> AnnotationRegex {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        AnnotationRegex::new(std::str::from_utf8(value.bytes()).unwrap().to_owned())
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(self.regex().as_bytes())))
    }
}

empty_type_vertex_property_encoding!(AnnotationCascade, PropertyAnnotationCascade);

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationRange {
    const INFIX: Infix = Infix::PropertyAnnotationRange;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for AnnotationValues {
    const INFIX: Infix = Infix::PropertyAnnotationValues;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

macro_rules! empty_type_edge_property_encoder {
    ($property:ident, $infix:ident) => {
        impl<'a> TypeEdgePropertyEncoding<'a> for $property {
            const INFIX: Infix = $infix;

            fn from_value_bytes<'b>(value: ByteReference<'b>) -> $property {
                debug_assert!(value.bytes().is_empty());
                $property
            }

            fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

empty_type_edge_property_encoder!(AnnotationDistinct, PropertyAnnotationDistinct);
empty_type_edge_property_encoder!(AnnotationKey, PropertyAnnotationKey);
empty_type_edge_property_encoder!(AnnotationUnique, PropertyAnnotationUnique);

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationCardinality {
    const INFIX: Infix = Infix::PropertyAnnotationCardinality;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationRegex {
    const INFIX: Infix = Infix::PropertyAnnotationRegex;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        AnnotationRegex::new(std::str::from_utf8(value.bytes()).unwrap().to_owned())
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(self.regex().as_bytes())))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationRange {
    const INFIX: Infix = Infix::PropertyAnnotationRange;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for AnnotationValues {
    const INFIX: Infix = Infix::PropertyAnnotationValues;
    fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
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
        }
    }
}

mod hash_value {
    use std::hash::{Hash, Hasher};
    use encoding::value::value::Value;

    // WARN: Use this function only for Annotations containing Values to allow its hashing,
    // not while precisely working with real values.
    fn hash_value<H: Hasher>(value: &Value<'static>, state: &mut H) {
        match value {
            Value::Boolean(value) => value.hash(state),
            Value::Long(value) => value.hash(state),
            Value::Double(value) => value.to_bits().hash(state),
            Value::Decimal(value) => value.hash(state),
            Value::Date(value) => value.hash(state),
            Value::DateTime(value) => value.hash(state),
            Value::DateTimeTZ(value) => value.hash(state),
            Value::String(value) => value.hash(state),
            Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
            Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
        }
    }

    pub(crate) fn hash_value_opt<H: Hasher>(value_opt: &Option<Value<'static>>, state: &mut H) {
        const NONE_HASH_MARKER: u64 = 0xDEADBEEFDEADBEEF;

        match value_opt {
            None => NONE_HASH_MARKER.hash(state),
            Some(value) => hash_value(value, state)
        }
    }

    pub(crate) fn hash_value_vec<H: Hasher>(value_vec: &Vec<Value<'static>>, state: &mut H) {
        value_vec.iter().for_each(|value| hash_value(value, state))
    }
}

mod serialise_range {
    use std::borrow::Cow;
    use std::fmt;
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{MapAccess, SeqAccess, Visitor};
    use serde::ser::SerializeStruct;
    use encoding::value::boolean_bytes::BooleanBytes;
    use encoding::value::date_bytes::DateBytes;
    use encoding::value::date_time_bytes::DateTimeBytes;
    use encoding::value::date_time_tz_bytes::DateTimeTZBytes;
    use encoding::value::decimal_bytes::DecimalBytes;
    use encoding::value::double_bytes::DoubleBytes;
    use encoding::value::long_bytes::LongBytes;
    use encoding::value::string_bytes::StringBytes;
    use encoding::value::value::Value;
    use encoding::value::value_type::ValueTypeCategory;
    use encoding::value::ValueEncodable;
    use crate::type_::annotation::AnnotationRange;
    use bytes::Bytes;

    enum Field {
        StartInclusive,
        EndInclusive,
    }

    impl Field {
        const NAMES: [&'static str; 2] = [
            Self::StartInclusive.name(),
            Self::EndInclusive.name(),
        ];

        const fn name(&self) -> &str {
            match self {
                Field::StartInclusive => "StartInclusive",
                Field::EndInclusive => "EndInclusive",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "StartInclusive" => Some(Field::StartInclusive),
                "EndInclusive" => Some(Field::EndInclusive),
                _ => None,
            }
        }
    }

    const INLINE_LENGTH: usize = 128;

    fn serialize_optional_value_field(value: Option<Value<'_>>) -> Option<Vec<u8>> {
        match &value {
            None => None,
            Some(start_inclusive) => Some(
                match start_inclusive {
                    | Value::Boolean(_)
                    | Value::Long(_)
                    | Value::Double(_)
                    | Value::Decimal(_)
                    | Value::Date(_)
                    | Value::DateTime(_)
                    | Value::DateTimeTZ(_)
                    | Value::String(_) => start_inclusive.encode_bytes::<INLINE_LENGTH>().bytes().to_owned(),
                    Value::Duration(_) => unreachable!("Can't use duration for AnnotationRange"),
                    Value::Struct(_) => unreachable!("Can't use struct for AnnotationRange"),
                }
            )
        }
    }

    impl Serialize for AnnotationRange {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let mut state = serializer.serialize_struct("AnnotationRange", Field::NAMES.len())?;
            state.serialize_field(Field::StartInclusive.name(), &serialize_optional_value_field(self.start_inclusive.clone()))?;
            state.serialize_field(Field::EndInclusive.name(), &serialize_optional_value_field(self.end_inclusive.clone()))?;
            state.end()
        }
    }

    fn deserialize_optional_value_field(bytes_opt: Option<&[u8]>, value_type_category: ValueTypeCategory) -> Option<Value<'static>> {
        match bytes_opt {
            None => None,
            Some(bytes) => Some(
                match &value_type_category {
                    ValueTypeCategory::Boolean => Value::Boolean(BooleanBytes::new(bytes.try_into().unwrap()).as_bool()),
                    ValueTypeCategory::Long => Value::Long(LongBytes::new(bytes.try_into().unwrap()).as_i64()),
                    ValueTypeCategory::Double => Value::Double(DoubleBytes::new(bytes.try_into().unwrap()).as_f64()),
                    ValueTypeCategory::Decimal => Value::Decimal(DecimalBytes::new(bytes.try_into().unwrap()).as_decimal()),
                    ValueTypeCategory::Date => Value::Date(DateBytes::new(bytes.try_into().unwrap()).as_naive_date()),
                    ValueTypeCategory::DateTime => Value::DateTime(DateTimeBytes::new(bytes.try_into().unwrap()).as_naive_date_time()),
                    ValueTypeCategory::DateTimeTZ => Value::DateTimeTZ(DateTimeTZBytes::new(bytes.try_into().unwrap()).as_date_time()),
                    ValueTypeCategory::String => Value::String(Cow::Owned(StringBytes::new(Bytes::<INLINE_LENGTH>::copy(bytes)).as_str().to_owned())),
                    ValueTypeCategory::Duration => unreachable!("Can't use duration for AnnotationRange"),
                    ValueTypeCategory::Struct => unreachable!("Can't use struct for AnnotationRange"),
                }
            )
        }
    }

    impl<'de> Deserialize<'de> for AnnotationRange {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                    where
                        D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                            where
                                E: de::Error,
                        {
                            Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct AnnotationRangeVisitor {
                value_type_category: ValueTypeCategory,
            }

            impl<'de> Visitor<'de> for AnnotationRangeVisitor {
                type Value = AnnotationRange;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct AnnotationRangeVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<AnnotationRange, V::Error>
                    where
                        V: SeqAccess<'de>,
                {
                    let start_inclusive = deserialize_optional_value_field(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                        self.value_type_category.clone(),
                    );
                    let end_inclusive = deserialize_optional_value_field(
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?,
                        self.value_type_category.clone(),
                    );

                    Ok(AnnotationRange {
                        start_inclusive,
                        end_inclusive,
                    })
                }

                fn visit_map<V>(self, mut map: V) -> Result<AnnotationRange, V::Error>
                    where
                        V: MapAccess<'de>,
                {
                    let mut start_inclusive: Option<Option<Value<'static>>> = None;
                    let mut end_inclusive: Option<Option<Value<'static>>> = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::StartInclusive => {
                                if start_inclusive.is_some() {
                                    return Err(de::Error::duplicate_field(Field::StartInclusive.name()));
                                }
                                start_inclusive = Some(deserialize_optional_value_field(
                                    map.next_value()?,
                                    self.value_type_category.clone(),
                                ));
                            }
                            Field::EndInclusive => {
                                if end_inclusive.is_some() {
                                    return Err(de::Error::duplicate_field(Field::EndInclusive.name()));
                                }
                                end_inclusive = Some(deserialize_optional_value_field(
                                    map.next_value()?,
                                    self.value_type_category.clone(),
                                ));
                            }
                        }
                    }

                    Ok(AnnotationRange {
                        start_inclusive: start_inclusive
                            .ok_or_else(|| de::Error::missing_field(Field::StartInclusive.name()))?,
                        end_inclusive: end_inclusive
                            .ok_or_else(|| de::Error::missing_field(Field::EndInclusive.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("AnnotationRange", &Field::NAMES, AnnotationRangeVisitor { value_type_category: ValueTypeCategory::Boolean })
        }
    }
}

mod serialise_values {
    use std::borrow::Cow;
    use std::fmt;
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{MapAccess, SeqAccess, Visitor};
    use serde::ser::SerializeStruct;
    use encoding::value::boolean_bytes::BooleanBytes;
    use encoding::value::date_bytes::DateBytes;
    use encoding::value::date_time_bytes::DateTimeBytes;
    use encoding::value::date_time_tz_bytes::DateTimeTZBytes;
    use encoding::value::decimal_bytes::DecimalBytes;
    use encoding::value::double_bytes::DoubleBytes;
    use encoding::value::long_bytes::LongBytes;
    use encoding::value::string_bytes::StringBytes;
    use encoding::value::value::Value;
    use encoding::value::value_type::ValueTypeCategory;
    use encoding::value::ValueEncodable;
    use crate::type_::annotation::AnnotationValues;
    use bytes::Bytes;
    use encoding::value::duration_bytes::DurationBytes;

    enum Field {
        Values,
    }

    impl Field {
        const NAMES: [&'static str; 1] = [
            Self::Values.name(),
        ];

        const fn name(&self) -> &str {
            match self {
                Field::Values => "Values",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "Values" => Some(Field::Values),
                _ => None,
            }
        }
    }

    const INLINE_LENGTH: usize = 128;

    fn serialize_value_vec_field(values: &Vec<Value<'_>>) -> Vec<Vec<u8>> {
        values.iter().map(|value| match value {
            | Value::Boolean(_)
            | Value::Long(_)
            | Value::Double(_)
            | Value::Decimal(_)
            | Value::Date(_)
            | Value::DateTime(_)
            | Value::DateTimeTZ(_)
            | Value::String(_)
            | Value::Duration(_) => value.encode_bytes::<INLINE_LENGTH>().bytes().to_owned(),
            Value::Struct(_) => unreachable!("Can't use struct for AnnotationValues"),
        }).collect()
    }

    impl Serialize for AnnotationValues {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let mut state = serializer.serialize_struct("AnnotationValues", Field::NAMES.len())?;
            state.serialize_field(Field::Values.name(), &serialize_value_vec_field(&self.values))?;
            state.end()
        }
    }

    fn deserialize_value(bytes: &[u8], value_type_category: ValueTypeCategory) -> Value<'static> {
        match value_type_category {
            ValueTypeCategory::Boolean => Value::Boolean(BooleanBytes::new(bytes.try_into().unwrap()).as_bool()),
            ValueTypeCategory::Long => Value::Long(LongBytes::new(bytes.try_into().unwrap()).as_i64()),
            ValueTypeCategory::Double => Value::Double(DoubleBytes::new(bytes.try_into().unwrap()).as_f64()),
            ValueTypeCategory::Decimal => Value::Decimal(DecimalBytes::new(bytes.try_into().unwrap()).as_decimal()),
            ValueTypeCategory::Date => Value::Date(DateBytes::new(bytes.try_into().unwrap()).as_naive_date()),
            ValueTypeCategory::DateTime => Value::DateTime(DateTimeBytes::new(bytes.try_into().unwrap()).as_naive_date_time()),
            ValueTypeCategory::DateTimeTZ => Value::DateTimeTZ(DateTimeTZBytes::new(bytes.try_into().unwrap()).as_date_time()),
            ValueTypeCategory::Duration => Value::Duration(DurationBytes::new(bytes.try_into().unwrap()).as_duration()),
            ValueTypeCategory::String => Value::String(Cow::Owned(StringBytes::new(Bytes::<INLINE_LENGTH>::copy(bytes)).as_str().to_owned())),
            ValueTypeCategory::Struct => unreachable!("Can't use struct for AnnotationValues"),
        }
    }

    fn deserialize_value_vec_field(bytes_vec: &Vec<&[u8]>, value_type_category: ValueTypeCategory) -> Vec<Value<'static>> {
        bytes_vec.iter().map(|bytes| deserialize_value(bytes, value_type_category)).collect()
    }

    impl<'de> Deserialize<'de> for AnnotationValues {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                    where
                        D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                            where
                                E: de::Error,
                        {
                            Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct AnnotationValuesVisitor {
                value_type_category: ValueTypeCategory,
            }

            impl<'de> Visitor<'de> for AnnotationValuesVisitor {
                type Value = AnnotationValues;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct AnnotationValuesVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<AnnotationValues, V::Error>
                    where
                        V: SeqAccess<'de>,
                {
                    let values = deserialize_value_vec_field(
                        &seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?,
                        self.value_type_category.clone(),
                    );

                    Ok(AnnotationValues {
                        values,
                    })
                }

                fn visit_map<V>(self, mut map: V) -> Result<AnnotationValues, V::Error>
                    where
                        V: MapAccess<'de>,
                {
                    let mut values: Option<Vec<Value<'static>>> = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::Values => {
                                if values.is_some() {
                                    return Err(de::Error::duplicate_field(Field::Values.name()));
                                }
                                values = Some(deserialize_value_vec_field(
                                    &map.next_value()?,
                                    self.value_type_category.clone(),
                                ));
                            }
                        }
                    }

                    Ok(AnnotationValues {
                        values: values
                            .ok_or_else(|| de::Error::missing_field(Field::Values.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("AnnotationValues", &Field::NAMES, AnnotationValuesVisitor { value_type_category: ValueTypeCategory::Boolean })
        }
    }
}
