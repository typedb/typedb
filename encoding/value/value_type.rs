/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, mem, ops::Range};

use bytes::{Bytes, byte_array::ByteArray};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Unexpected, Visitor},
};
use structural_equality::StructuralEquality;

use crate::{
    AsBytes,
    graph::{definition::definition_key::DefinitionKey, type_::property::TypeVertexPropertyEncoding},
    layout::infix::Infix,
};

// We can support Prefix::ATTRIBUTE_MAX - Prefix::ATTRIBUTE_MIN different built-in value types
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ValueType {
    Boolean,
    Integer,
    Double,
    Decimal,

    Date,
    DateTime,
    DateTimeTZ,
    Duration,

    String,

    Struct(DefinitionKey),

    // A fixed-length, single-precision vector, e.g. `vector(384, "float32")`. `length` is the
    // number of elements declared for the attribute type; `precision` is the element encoding.
    Vector(VectorTypeParameters),
}

/// The parameters that make a vector value type concrete: its declared length and element precision.
/// These are packed into the value type's tail bytes (see [`ValueTypeBytes`]), analogously to how
/// [`ValueType::Struct`] carries a [`DefinitionKey`].
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct VectorTypeParameters {
    pub length: u16,
    pub precision: VectorPrecision,
}

impl VectorTypeParameters {
    pub const fn new(length: u16, precision: VectorPrecision) -> Self {
        Self { length, precision }
    }
}

impl fmt::Display for VectorTypeParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vector({}, \"{}\")", self.length, self.precision.name())
    }
}

/// The element encoding of a vector value type. Mirrors `typeql::common::token::VectorPrecision`.
/// Currently only single-precision float (`"float32"`) is supported by the spec.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum VectorPrecision {
    Float32,
}

impl VectorPrecision {
    pub const fn to_byte(&self) -> u8 {
        match self {
            VectorPrecision::Float32 => 0,
        }
    }

    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0 => VectorPrecision::Float32,
            _ => panic!("Unrecognised vector precision byte: {byte}"),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            VectorPrecision::Float32 => "float32",
        }
    }
}

impl fmt::Display for VectorPrecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ValueType {
    pub fn category(&self) -> ValueTypeCategory {
        match self {
            ValueType::Boolean => ValueTypeCategory::Boolean,
            ValueType::Integer => ValueTypeCategory::Integer,
            ValueType::Double => ValueTypeCategory::Double,
            ValueType::Decimal => ValueTypeCategory::Decimal,
            ValueType::Date => ValueTypeCategory::Date,
            ValueType::DateTime => ValueTypeCategory::DateTime,
            ValueType::DateTimeTZ => ValueTypeCategory::DateTimeTZ,
            ValueType::Duration => ValueTypeCategory::Duration,
            ValueType::String => ValueTypeCategory::String,
            ValueType::Struct(_) => ValueTypeCategory::Struct,
            ValueType::Vector(_) => ValueTypeCategory::Vector,
        }
    }

    pub fn keyable(&self) -> bool {
        match self {
            | ValueType::Boolean
            | ValueType::Integer
            | ValueType::Decimal
            | ValueType::Date
            | ValueType::DateTime
            | ValueType::DateTimeTZ
            | ValueType::Duration
            | ValueType::String => true,

            // Vectors are variable-length and hash-stored (like Struct), so they are not keyable.
            | ValueType::Double | ValueType::Struct(_) | ValueType::Vector(_) => false,
        }
    }

    fn from_category_and_tail(category: ValueTypeCategory, tail: [u8; ValueTypeBytes::TAIL_LENGTH]) -> Self {
        match category {
            ValueTypeCategory::Boolean => Self::Boolean,
            ValueTypeCategory::Integer => Self::Integer,
            ValueTypeCategory::Double => Self::Double,
            ValueTypeCategory::Decimal => Self::Decimal,
            ValueTypeCategory::Date => Self::Date,
            ValueTypeCategory::DateTime => Self::DateTime,
            ValueTypeCategory::DateTimeTZ => Self::DateTimeTZ,
            ValueTypeCategory::Duration => Self::Duration,
            ValueTypeCategory::String => Self::String,
            ValueTypeCategory::Struct => {
                let definition_key = DefinitionKey::new(Bytes::Array(ByteArray::copy(&tail)));
                Self::Struct(definition_key)
            }
            ValueTypeCategory::Vector => {
                // Tail layout: [length_lo, length_hi, precision] (see ValueTypeBytes::build).
                let length = u16::from_le_bytes([tail[0], tail[1]]);
                let precision = VectorPrecision::from_byte(tail[2]);
                Self::Vector(VectorTypeParameters::new(length, precision))
            }
        }
    }

    pub fn is_trivially_castable_to(&self, other: ValueTypeCategory) -> bool {
        if self.category() == other {
            return true;
        }
        match self {
            ValueType::Integer => other == ValueTypeCategory::Double || other == ValueTypeCategory::Decimal,
            ValueType::Decimal => other == ValueTypeCategory::Double,
            ValueType::Date => other == ValueTypeCategory::DateTime,
            _ => false,
        }
    }

    /// Like [`Self::is_trivially_castable_to`], but compares against a full value type rather than
    /// just a category. Parameterized value types need this: a `vector(2, "float32")` value shares a
    /// category with a `vector(3, "float32")` attribute type but is not a legal value for it.
    pub fn is_trivially_castable_to_value_type(&self, other: &ValueType) -> bool {
        match (self, other) {
            (ValueType::Vector(params), ValueType::Vector(other_params)) => params == other_params,
            _ => self.is_trivially_castable_to(other.category()),
        }
    }

    // we can approximately cast any numerical type to any other numerical type
    pub fn is_approximately_castable_to(&self, other: ValueTypeCategory) -> bool {
        if self.category() == other {
            return true;
        }
        match self {
            ValueType::Integer => other == ValueTypeCategory::Double || other == ValueTypeCategory::Decimal,
            ValueType::Decimal => other == ValueTypeCategory::Double || other == ValueTypeCategory::Integer,
            ValueType::Double => other == ValueTypeCategory::Decimal || other == ValueTypeCategory::Integer,
            // TODO: we will have to decide if we consider date datatypes to be approximately castable to each other
            ValueType::Date => other == ValueTypeCategory::DateTime,
            _ => false,
        }
    }
}

impl StructuralEquality for ValueType {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            & match self {
                ValueType::Struct(key) => (key.definition_id().as_uint() as usize).hash(),
                ValueType::Vector(params) => {
                    ((params.length as usize) ^ ((params.precision.to_byte() as usize) << 16)).hash()
                }
                _ => 0,
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean, Self::Boolean) => true,
            (Self::Integer, Self::Integer) => true,
            (Self::Double, Self::Double) => true,
            (Self::Decimal, Self::Decimal) => true,
            (Self::Date, Self::Date) => true,
            (Self::DateTime, Self::DateTime) => true,
            (Self::DateTimeTZ, Self::DateTimeTZ) => true,
            (Self::Duration, Self::Duration) => true,
            (Self::String, Self::String) => true,
            (Self::Struct(key), Self::Struct(other_key)) => {
                (key.definition_id().as_uint() as usize).equals(&(other_key.definition_id().as_uint() as usize))
            }
            (Self::Vector(params), Self::Vector(other_params)) => params == other_params,
            // note: this style forces updating the match when the variants change
            (Self::Boolean { .. }, _)
            | (Self::Integer { .. }, _)
            | (Self::Double { .. }, _)
            | (Self::Decimal { .. }, _)
            | (Self::Date { .. }, _)
            | (Self::DateTime { .. }, _)
            | (Self::DateTimeTZ { .. }, _)
            | (Self::Duration { .. }, _)
            | (Self::String { .. }, _)
            | (Self::Struct { .. }, _)
            | (Self::Vector { .. }, _) => false,
        }
    }
}

impl fmt::Debug for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Don't we want to display structs differently?
        match self {
            ValueType::Vector(params) => write!(f, "{params}"),
            _ => write!(f, "{}", self.category().name()),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ValueTypeCategory {
    Boolean,
    Integer,
    Double,
    Decimal,
    Date,
    DateTime,
    DateTimeTZ,
    Duration,
    String,
    Struct,
    Vector,
}

impl ValueTypeCategory {
    pub const fn to_bytes(&self) -> [u8; ValueTypeBytes::CATEGORY_LENGTH] {
        // WARNING: Order here must align with AttributeID enum ordering!!
        match self {
            Self::Boolean => [0],
            Self::Integer => [1],
            Self::Double => [2],
            Self::Decimal => [3],
            Self::Date => [4],
            Self::DateTime => [5],
            Self::DateTimeTZ => [6],
            Self::Duration => [7],
            Self::String => [8],
            Self::Struct => [40],
            Self::Vector => [41],
        }
    }

    pub fn from_bytes(bytes: [u8; ValueTypeBytes::CATEGORY_LENGTH]) -> Self {
        let category = match bytes {
            [0] => ValueTypeCategory::Boolean,
            [1] => ValueTypeCategory::Integer,
            [2] => ValueTypeCategory::Double,
            [3] => ValueTypeCategory::Decimal,
            [4] => ValueTypeCategory::Date,
            [5] => ValueTypeCategory::DateTime,
            [6] => ValueTypeCategory::DateTimeTZ,
            [7] => ValueTypeCategory::Duration,
            [8] => ValueTypeCategory::String,
            [40] => ValueTypeCategory::Struct,
            [41] => ValueTypeCategory::Vector,
            _ => panic!("Unrecognised value type category byte: {:?}", bytes),
        };
        debug_assert_eq!(bytes, category.to_bytes());
        category
    }

    pub fn comparable_categories(category: ValueTypeCategory) -> &'static [ValueTypeCategory] {
        match category {
            ValueTypeCategory::Boolean => &[ValueTypeCategory::Boolean],
            ValueTypeCategory::Integer => {
                &[ValueTypeCategory::Integer, ValueTypeCategory::Double, ValueTypeCategory::Decimal]
            }
            ValueTypeCategory::Double => {
                &[ValueTypeCategory::Integer, ValueTypeCategory::Double, ValueTypeCategory::Decimal]
            }
            ValueTypeCategory::Decimal => {
                &[ValueTypeCategory::Integer, ValueTypeCategory::Double, ValueTypeCategory::Decimal]
            }
            ValueTypeCategory::DateTime => &[ValueTypeCategory::DateTime],
            ValueTypeCategory::DateTimeTZ => &[ValueTypeCategory::DateTimeTZ],
            ValueTypeCategory::Duration => &[ValueTypeCategory::Duration],
            ValueTypeCategory::String => &[ValueTypeCategory::String],
            ValueTypeCategory::Struct => &[ValueTypeCategory::Struct],
            ValueTypeCategory::Date => &[ValueTypeCategory::Date],
            ValueTypeCategory::Vector => &[ValueTypeCategory::Vector],
        }
    }

    pub fn try_into_value_type(self) -> Option<ValueType> {
        match self {
            ValueTypeCategory::Boolean => Some(ValueType::Boolean),
            ValueTypeCategory::Integer => Some(ValueType::Integer),
            ValueTypeCategory::Double => Some(ValueType::Double),
            ValueTypeCategory::Decimal => Some(ValueType::Decimal),
            ValueTypeCategory::Date => Some(ValueType::Date),
            ValueTypeCategory::DateTime => Some(ValueType::DateTime),
            ValueTypeCategory::DateTimeTZ => Some(ValueType::DateTimeTZ),
            ValueTypeCategory::Duration => Some(ValueType::Duration),
            ValueTypeCategory::String => Some(ValueType::String),
            // Struct and Vector are parameterized: the category alone is insufficient to
            // reconstruct the full value type (Struct needs a DefinitionKey, Vector needs a
            // length + precision), so they cannot be materialized from a bare category.
            ValueTypeCategory::Struct => None,
            ValueTypeCategory::Vector => None,
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            ValueTypeCategory::Boolean => "boolean",
            ValueTypeCategory::Integer => "integer",
            ValueTypeCategory::Double => "double",
            ValueTypeCategory::Decimal => "decimal",
            ValueTypeCategory::Date => "date",
            ValueTypeCategory::DateTime => "datetime",
            ValueTypeCategory::DateTimeTZ => "datetime-tz",
            ValueTypeCategory::Duration => "duration",
            ValueTypeCategory::String => "string",
            ValueTypeCategory::Struct => "struct",
            ValueTypeCategory::Vector => "vector",
        }
    }
}

impl fmt::Display for ValueTypeCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ValueTypeBytes {
    bytes: [u8; Self::LENGTH],
}

impl ValueTypeBytes {
    pub const CATEGORY_LENGTH: usize = 1;
    const TAIL_LENGTH: usize = DefinitionKey::LENGTH;
    const LENGTH: usize = Self::CATEGORY_LENGTH + Self::TAIL_LENGTH;
    const RANGE_CATEGORY: Range<usize> = 0..Self::CATEGORY_LENGTH;
    const RANGE_TAIL: Range<usize> = Self::RANGE_CATEGORY.end..Self::RANGE_CATEGORY.end + Self::TAIL_LENGTH;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(value_type: &ValueType) -> Self {
        let mut array = [0; Self::LENGTH];
        array[Self::RANGE_CATEGORY].copy_from_slice(&value_type.category().to_bytes());
        match value_type {
            ValueType::Struct(definition_key) => {
                array[Self::RANGE_TAIL].copy_from_slice(&definition_key.clone().to_bytes());
            }
            ValueType::Vector(params) => {
                // Tail layout: [length_lo, length_hi, precision]. TAIL_LENGTH is 3 bytes, exactly
                // enough for a u16 length plus a single precision byte.
                let length_bytes = params.length.to_le_bytes();
                array[Self::RANGE_TAIL.start] = length_bytes[0];
                array[Self::RANGE_TAIL.start + 1] = length_bytes[1];
                array[Self::RANGE_TAIL.start + 2] = params.precision.to_byte();
            }
            _ => {}
        }
        Self { bytes: array }
    }

    pub fn to_value_type(&self) -> ValueType {
        ValueType::from_category_and_tail(
            ValueTypeCategory::from_bytes(self.bytes[Self::RANGE_CATEGORY].try_into().unwrap()),
            self.bytes[Self::RANGE_TAIL].try_into().unwrap(),
        )
    }

    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

impl Serialize for ValueType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&ValueTypeBytes::build(self).into_bytes())
    }
}

impl TypeVertexPropertyEncoding for ValueType {
    const INFIX: Infix = Infix::PropertyValueType;

    fn from_key_value_bytes(key: &[u8], value: &[u8]) -> Self {
        debug_assert!(key.is_empty());
        let mut bytes: [u8; ValueTypeBytes::LENGTH] = [0; ValueTypeBytes::LENGTH];
        bytes.copy_from_slice(&value[0..ValueTypeBytes::LENGTH]);
        ValueTypeBytes::new(bytes).to_value_type()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(&ValueTypeBytes::build(self).into_bytes())))
    }
}

impl<'de> Deserialize<'de> for ValueType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueTypeVisitor;

        impl Visitor<'_> for ValueTypeVisitor {
            type Value = ValueType;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`ValueType`")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<ValueType, E>
            where
                E: de::Error,
            {
                if let Ok(value_bytes) = v.try_into() {
                    Ok(ValueTypeBytes::new(value_bytes).to_value_type())
                } else {
                    Err(E::invalid_value(Unexpected::Bytes(v), &self))
                }
            }
        }
        deserializer.deserialize_bytes(ValueTypeVisitor)
    }
}
