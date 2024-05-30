/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::byte_array::ByteArray;
use bytes::Bytes;

use crate::AsBytes;
use crate::graph::definition::definition_key::DefinitionKey;

// We can support Prefix::ATTRIBUTE_MAX - Prefix::ATTRIBUTE_MIN different built-in value types
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValueType {
    Boolean,
    Long,
    Double,

    DateTime,
    DateTimeTZ,
    Duration,

    String,

    Struct(DefinitionKey<'static>),
}

impl ValueType {
    pub fn category(&self) -> ValueTypeCategory {
        match self {
            ValueType::Boolean => ValueTypeCategory::Boolean,
            ValueType::Long => ValueTypeCategory::Long,
            ValueType::Double => ValueTypeCategory::Double,
            ValueType::DateTime => ValueTypeCategory::DateTime,
            ValueType::DateTimeTZ => ValueTypeCategory::DateTimeTZ,
            ValueType::Duration => ValueTypeCategory::Duration,
            ValueType::String => ValueTypeCategory::String,
            ValueType::Struct(_) => ValueTypeCategory::Struct,
        }
    }

    fn from_category_and_tail(category: ValueTypeCategory, tail: [u8; ValueTypeBytes::TAIL_LENGTH]) -> Self {
        match category {
            ValueTypeCategory::Boolean => Self::Boolean,
            ValueTypeCategory::Long => Self::Long,
            ValueTypeCategory::Double => Self::Double,
            ValueTypeCategory::DateTime => Self::DateTime,
            ValueTypeCategory::DateTimeTZ => Self::DateTimeTZ,
            ValueTypeCategory::Duration => Self::Duration,
            ValueTypeCategory::String => Self::String,
            ValueTypeCategory::Struct => {
                let definition_key = DefinitionKey::new(Bytes::Array(ByteArray::copy(&tail)));
                Self::Struct(definition_key)
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ValueTypeCategory {
    Boolean,
    Long,
    Double,
    DateTime,
    DateTimeTZ,
    Duration,
    String,
    Struct,
}

impl ValueTypeCategory {

    fn to_bytes(&self) -> [u8; ValueTypeBytes::CATEGORY_LENGTH] {
        match self {
            Self::Boolean => [0],
            Self::Long => [1],
            Self::Double => [2],
            Self::DateTime => [3],
            Self::DateTimeTZ => [4],
            Self::Duration => [5],
            Self::String => [6],
            Self::Struct => [40]
        }
    }

    fn from_bytes(bytes: [u8; ValueTypeBytes::CATEGORY_LENGTH]) -> Self {
        let category = match bytes {
            [0] => ValueTypeCategory::Boolean,
            [1] => ValueTypeCategory::Long,
            [2] => ValueTypeCategory::Double,
            [3] => ValueTypeCategory::DateTime,
            [4] => ValueTypeCategory::DateTimeTZ,
            [5] => ValueTypeCategory::Duration,
            [6] => ValueTypeCategory::String,
            [40] => ValueTypeCategory::Struct,
            _ => panic!("Unrecognised value type category byte: {:?}", bytes),
        };
        debug_assert_eq!(bytes, category.to_bytes());
        category
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ValueTypeBytes {
    bytes: [u8; Self::LENGTH],
}

impl ValueTypeBytes {
    const CATEGORY_LENGTH: usize = 1;
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
                array[Self::RANGE_TAIL].copy_from_slice(&definition_key.bytes().bytes());
            }
            _ => {}
        }

        Self { bytes: array }
    }

    pub fn to_value_type(&self) -> ValueType {
        ValueType::from_category_and_tail(
            ValueTypeCategory::from_bytes(self.bytes[Self::RANGE_CATEGORY].try_into().unwrap()),
            self.bytes[Self::RANGE_TAIL].try_into().unwrap()
        )
    }

    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
