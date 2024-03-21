/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, byte_array_or_ref::ByteArrayOrRef, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::{type_::vertex::TypeID, Typed},
    layout::prefix::{PrefixID, PrefixType},
    value::value_type::ValueType,
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> AttributeVertex<'a> {
    pub(crate) const LENGTH_PREFIX_PREFIX: usize = PrefixID::LENGTH;
    pub(crate) const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() > Self::LENGTH_PREFIX_TYPE);
        AttributeVertex { bytes }
    }

    fn value_type_to_prefix_type(value_type: ValueType) -> PrefixType {
        match value_type {
            ValueType::Boolean => PrefixType::VertexAttributeBoolean,
            ValueType::Long => PrefixType::VertexAttributeLong,
            ValueType::Double => PrefixType::VertexAttributeDouble,
            ValueType::String => PrefixType::VertexAttributeString,
        }
    }

    pub(crate) fn build(value_type: ValueType, type_id: TypeID, attribute_id: AttributeID) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE + attribute_id.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX]
            .copy_from_slice(&Self::value_type_to_prefix_type(value_type).prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        bytes.bytes_mut()[Self::range_for_attribute_id(attribute_id.length())].copy_from_slice(attribute_id.bytes());
        Self { bytes: ByteArrayOrRef::Array(bytes) }
    }

    pub(crate) fn build_prefix_type_attribute_id(
        value_type: ValueType,
        type_id: TypeID,
        attribute_id_part: &[u8],
    ) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE + attribute_id_part.len());
        bytes.bytes_mut()[Self::RANGE_PREFIX]
            .copy_from_slice(&Self::value_type_to_prefix_type(value_type).prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        bytes.bytes_mut()[Self::range_for_attribute_id(attribute_id_part.len())].copy_from_slice(attribute_id_part);
        StorageKey::new_owned(Self::KEYSPACE_ID, bytes)
    }

    pub fn build_prefix_type(
        value_type: ValueType,
        type_id: TypeID,
    ) -> StorageKey<'static, { AttributeVertex::LENGTH_PREFIX_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX]
            .copy_from_slice(&Self::value_type_to_prefix_type(value_type).prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        StorageKey::new_owned(Self::KEYSPACE_ID, bytes)
    }

    pub fn build_prefix_prefix(prefix: PrefixID) -> StorageKey<'static, { AttributeVertex::LENGTH_PREFIX_PREFIX }> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_PREFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        StorageKey::new(Self::KEYSPACE_ID, ByteArrayOrRef::Array(array))
    }

    pub fn value_type(&self) -> ValueType {
        match self.prefix() {
            PrefixType::VertexAttributeLong => ValueType::Long,
            PrefixType::VertexAttributeString => ValueType::String,
            _ => unreachable!("Unexpected prefix."),
        }
    }

    pub fn attribute_id(&self) -> AttributeID {
        AttributeID::new(&self.bytes.bytes()[self.range_of_attribute_id()])
    }

    fn range_of_attribute_id(&self) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..self.length()
    }

    fn range_for_attribute_id(id_length: usize) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + id_length
    }

    pub fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn into_owned(self) -> AttributeVertex<'static> {
        AttributeVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    const KEYSPACE_ID: EncodingKeyspace = EncodingKeyspace::Data;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AttributeID {
    Bytes8(AttributeID8),
    Bytes16(AttributeID16),
}

impl AttributeID {
    pub(crate) fn new(bytes: &[u8]) -> Self {
        match bytes.len() {
            8 => Self::Bytes8(AttributeID8::new(bytes.try_into().unwrap())),
            16 => Self::Bytes16(AttributeID16::new(bytes.try_into().unwrap())),
            _ => panic!("Unknown Attribute ID encoding length: {}", bytes.len()),
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            AttributeID::Bytes8(id_8) => &id_8.bytes,
            AttributeID::Bytes16(id_16) => &id_16.bytes,
        }
    }

    pub(crate) const fn length(&self) -> usize {
        match self {
            AttributeID::Bytes8(_) => AttributeID8::LENGTH,
            AttributeID::Bytes16(_) => AttributeID16::LENGTH,
        }
    }

    pub fn unwrap_bytes_16(self) -> AttributeID16 {
        match self {
            AttributeID::Bytes8(_) => panic!("Cannot unwrap bytes_16 from AttributeID::Bytes_8"),
            AttributeID::Bytes16(bytes) => bytes,
        }
    }

    pub fn unwrap_bytes_8(self) -> AttributeID8 {
        match self {
            AttributeID::Bytes8(bytes) => bytes,
            AttributeID::Bytes16(_) => panic!("Cannot unwrap bytes_8 from AttributeID::Bytes_16"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AttributeID8 {
    bytes: [u8; Self::LENGTH],
}

impl AttributeID8 {
    const LENGTH: usize = 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AttributeID16 {
    bytes: [u8; Self::LENGTH],
}

impl AttributeID16 {
    pub(crate) const LENGTH: usize = 16;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
