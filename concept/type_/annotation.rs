/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, error::Error, fmt};

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

#[derive(Debug, Clone, Eq, PartialEq)]
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
    // TODO: Subkey
    // TODO: Values
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
    pub const CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, Some(1));
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

    pub fn is_valid(&self, count: u64) -> bool {
        self.value_satisfies_start(count) && self.value_satisfies_end(Some(count))
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
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCascade;

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

    pub fn is_valid(&self, value: Value<'static>) -> bool {
        self.value_satisfies_start(Some(value.clone())) && self.value_satisfies_end(Some(value))
    }

    pub fn start(&self) -> Option<Value<'static>> {
        self.start_inclusive.clone()
    }

    pub fn end(&self) -> Option<Value<'static>> {
        self.end_inclusive.clone()
    }

    pub fn narrowed_correctly_by(&self, other: &Self) -> bool {
        self.value_satisfies_start(other.start()) && self.value_satisfies_end(other.end())
    }

    fn value_satisfies_start(&self, value: Option<Value<'static>>) -> bool {
        match self.start() {
            None => true,
            Some(start) => match &value {
                Value::Boolean(value) => &start.unwrap().unwrap_boolean() <= value,
                Value::Long(value) => &start.unwrap().unwrap_long() <= value,
                Value::Double(value) => &start.unwrap().unwrap_double() <= value,
                Value::Decimal(value) => &start.unwrap().unwrap_decimal() <= value,
                Value::Date(value) => &start.unwrap().unwrap_date() <= value,
                Value::DateTime(value) => &start.unwrap().unwrap_date_time() <= value,
                Value::DateTimeTZ(value) => &start.unwrap().unwrap_date_time_tz() <= value,
                Value::String(value) => &start.unwrap().unwrap_string() <= value,
                Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
                Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
            }
        }
    }

    fn value_satisfies_end(&self, value: Option<Value<'static>>) -> bool {
        match self.end() {
            None => true,
            Some(end) => match &value {
                Value::Boolean(value) => &end.unwrap().unwrap_boolean() >= value,
                Value::Long(value) => &end.unwrap().unwrap_long() >= value,
                Value::Double(value) => &end.unwrap().unwrap_double() >= value,
                Value::Decimal(value) => &end.unwrap().unwrap_decimal() >= value,
                Value::Date(value) => &end.unwrap().unwrap_date() >= value,
                Value::DateTime(value) => &end.unwrap().unwrap_date_time() >= value,
                Value::DateTimeTZ(value) => &end.unwrap().unwrap_date_time_tz() >= value,
                Value::String(value) => &end.unwrap().unwrap_string() >= value,
                Value::Duration(_) => unreachable!("Cannot use duration for AnnotationRange"),
                Value::Struct(_) => unreachable!("Cannot use structs for AnnotationRange"),
            }
        }
    }
}

mod serialise_range {
    use std::fmt;
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{MapAccess, SeqAccess, Visitor};
    use serde::ser::SerializeStruct;
    use bytes::byte_array::ByteArray;
    use encoding::value::boolean_bytes::BooleanBytes;
    use encoding::value::value::Value;
    use encoding::value::value_type::{ValueType};
    use encoding::value::ValueEncodable;
    use crate::type_::annotation::AnnotationRange;

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

    fn serialize_optional_value_field(value: Option<Value>) -> Option<ByteArray<INLINE_LENGTH>> {
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
                    | Value::DateTimeTZ(_) // TODO: Maybe unreachable!("Can't use datetime-tz for AnnotationRange")
                    | Value::Duration(_) // TODO: Maybe unreachable!("Can't use duration for AnnotationRange")
                    | Value::String(_) => start_inclusive.encode_bytes(),
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
            state.serialize_field(Field::EndInclusive.name(), &serialize_optional_value_field(self.start_inclusive.clone()))?;

            state.end()
        }
    }
    //
    // fn deserialize_optional_value_field(value: Option<&[u8]>, value_type: ValueType) -> Option<Value> {
    //     match value {
    //         None => None,
    //         Some(value) => Some(
    //             match &value_type {
    //                 ValueType::Boolean => Value::Boolean(value),
    //                 ValueType::Long => Value::Long(value),
    //                 ValueType::Double => Value::Double(value),
    //                 ValueType::Decimal => Value::Decimal(value),
    //                 ValueType::Date => Value::Date(value),
    //                 ValueType::DateTime => Value::DateTime(value),
    //                 ValueType::DateTimeTZ => Value::DateTimeTZ(value), // TODO: Maybe unreachable!("Can't use datetime-tz for AnnotationRange")
    //                 ValueType::Duration => Value::Duration(value), // TODO: Maybe unreachable!("Can't use duration for AnnotationRange")
    //                 ValueType::String => Value::String(value),
    //                 ValueType::Struct(_) => unreachable!("Can't use struct for AnnotationRange"),
    //             }
    //         )
    //     }
    // }
    //
    // impl<'de> Deserialize<'de> for AnnotationRange {
    //     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    //         where
    //             D: Deserializer<'de>,
    //     {
    //         impl<'de> Deserialize<'de> for Field {
    //             fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
    //                 where
    //                     D: Deserializer<'de>,
    //             {
    //                 struct FieldVisitor;
    //
    //                 impl<'de> Visitor<'de> for FieldVisitor {
    //                     type Value = Field;
    //
    //                     fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    //                         formatter.write_str("Unrecognised field")
    //                     }
    //
    //                     fn visit_str<E>(self, value: &str) -> Result<Field, E>
    //                         where
    //                             E: de::Error,
    //                     {
    //                         Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
    //                     }
    //                 }
    //
    //                 deserializer.deserialize_identifier(FieldVisitor)
    //             }
    //         }
    //
    //         struct AnnotationRangeVisitor {
    //             value_type: ValueType
    //         }
    //
    //         impl<'de> Visitor<'de> for AnnotationRangeVisitor {
    //             type Value = AnnotationRange;
    //
    //             fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    //                 formatter.write_str("struct AnnotationRangeVisitor")
    //             }
    //
    //             fn visit_seq<V>(self, mut seq: V) -> Result<AnnotationRange, V::Error>
    //                 where
    //                     V: SeqAccess<'de>,
    //             {
    //                 // TODO: Is it saved automatically??
    //                 let _annotation_regex_version = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
    //
    //                 let start_inclusive = deserialize_optional_value_field(
    //                     seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?,
    //                     self.value_type.clone()
    //                 );
    //                 let end_inclusive = deserialize_optional_value_field(
    //                     seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?,
    //                     self.value_type.clone()
    //                 );
    //
    //                 Ok(AnnotationRange {
    //                     start_inclusive,
    //                     end_inclusive,
    //                 })
    //             }
    //
    //             fn visit_map<V>(self, mut map: V) -> Result<AnnotationRange, V::Error>
    //                 where
    //                     V: MapAccess<'de>,
    //             {
    //                 let mut start_inclusive = None;
    //                 let mut end_inclusive = None;
    //                 while let Some(key) = map.next_key()? {
    //                     match key {
    //                         Field::StartInclusive => {
    //                             if start_inclusive.is_some() {
    //                                 return Err(de::Error::duplicate_field(Field::StartInclusive.name()));
    //                             }
    //                             start_inclusive = Some(map.next_value()?);
    //                         }
    //                         Field::EndInclusive => {
    //                             if end_inclusive.is_some() {
    //                                 return Err(de::Error::duplicate_field(Field::EndInclusive.name()));
    //                             }
    //                             end_inclusive = Some(map.next_value()?);
    //                         }
    //                     }
    //                 }
    //
    //                 Ok(AnnotationRange {
    //                     start_inclusive: start_inclusive
    //                         .ok_or_else(|| de::Error::missing_field(Field::StartInclusive.name()))?,
    //                     end_inclusive: end_inclusive
    //                         .ok_or_else(|| de::Error::missing_field(Field::EndInclusive.name()))?,
    //                 })
    //             }
    //         }
    //
    //         deserializer.deserialize_struct("AnnotationRange", &Field::NAMES, AnnotationRangeVisitor)
    //     }
    // }
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
    // TODO: Subkey
    // TODO: Values
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
            | AnnotationCategory::Range => true,
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
            | AnnotationCategory::Range => true,
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
            | AnnotationCategory::Range => true,
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
            | AnnotationCategory::Range => true,
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
        todo!()
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
    // fn from_value_bytes<'b>(value: ByteReference<'b>) -> Self {
    //     // TODO this .unwrap() should be handled as an error
    //     // although it does indicate data corruption
    //     bincode::deserialize(value.bytes()).unwrap()
    // }
    //
    // fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
    //     Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    // }
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
