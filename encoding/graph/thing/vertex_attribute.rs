/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::{type_::vertex::TypeID, Typed},
    layout::prefix::{PrefixID, Prefix},
    value::value_type::ValueType,
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};
use crate::graph::thing::VertexID;

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct AttributeVertex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> AttributeVertex<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;

    pub(crate) const LENGTH_PREFIX_PREFIX: usize = PrefixID::LENGTH;
    pub(crate) const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() > Self::LENGTH_PREFIX_TYPE);
        AttributeVertex { bytes }
    }

    pub(crate) fn build(value_type: ValueType, type_id: TypeID, attribute_id: AttributeID) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE + attribute_id.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX]
            .copy_from_slice(&Self::value_type_to_prefix_type(value_type).prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        bytes.bytes_mut()[Self::range_for_attribute_id(attribute_id.length())].copy_from_slice(attribute_id.bytes());
        Self { bytes: Bytes::Array(bytes) }
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
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn build_prefix_type(
        value_type: ValueType,
        type_id: TypeID,
    ) -> StorageKey<'static, { AttributeVertex::LENGTH_PREFIX_TYPE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE);
        bytes.bytes_mut()[Self::RANGE_PREFIX]
            .copy_from_slice(&Self::value_type_to_prefix_type(value_type).prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    fn value_type_to_prefix_type(value_type: ValueType) -> Prefix {
        match value_type {
            ValueType::Boolean => Prefix::VertexAttributeBoolean,
            ValueType::Long => Prefix::VertexAttributeLong,
            ValueType::Double => Prefix::VertexAttributeDouble,
            ValueType::String => Prefix::VertexAttributeString,
        }
    }

    pub fn build_prefix_prefix(prefix: PrefixID) -> StorageKey<'static, { AttributeVertex::LENGTH_PREFIX_PREFIX }> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_PREFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        StorageKey::new(Self::KEYSPACE, Bytes::Array(array))
    }

    pub fn value_type(&self) -> ValueType {
        match self.prefix() {
            Prefix::VertexAttributeLong => ValueType::Long,
            Prefix::VertexAttributeString => ValueType::String,
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

    pub(crate) fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn as_reference<'this: 'a>(&'this self) -> AttributeVertex<'this> {
        Self::new(Bytes::Reference(self.bytes.as_reference()))
    }

    pub fn into_owned(self) -> AttributeVertex<'static> {
        AttributeVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Data
    }
}

pub(crate) trait AsAttributeID {
    type AttributeIDType: VertexID;

    fn as_attribute_id(&self) -> AttributeID;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AttributeID {
    Bytes8(AttributeID8),
    Bytes17(AttributeID17),
}

impl AttributeID {
    pub(crate) fn new(bytes: &[u8]) -> Self {
        match bytes.len() {
            AttributeID8::LENGTH => Self::Bytes8(AttributeID8::new(bytes.try_into().unwrap())),
            AttributeID17::LENGTH => Self::Bytes17(AttributeID17::new(bytes.try_into().unwrap())),
            _ => panic!("Unknown Attribute ID encoding length: {}", bytes.len()),
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            AttributeID::Bytes8(id_8) => &id_8.bytes,
            AttributeID::Bytes17(id_17) => &id_17.bytes,
        }
    }

    pub(crate) const fn length(&self) -> usize {
        match self {
            AttributeID::Bytes8(_) => AttributeID8::LENGTH,
            AttributeID::Bytes17(_) => AttributeID17::LENGTH,
        }
    }

    pub fn unwrap_bytes_17(self) -> AttributeID17 {
        match self {
            AttributeID::Bytes8(_) => panic!("Cannot unwrap bytes_17 from AttributeID::Bytes_8"),
            AttributeID::Bytes17(bytes) => bytes,
        }
    }

    pub fn unwrap_bytes_8(self) -> AttributeID8 {
        match self {
            AttributeID::Bytes8(bytes) => bytes,
            AttributeID::Bytes17(_) => panic!("Cannot unwrap bytes_8 from AttributeID::Bytes_17"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AttributeID8 {
    bytes: [u8; Self::LENGTH],
}

impl AttributeID8 {
    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

impl VertexID for AttributeID8 {
    const LENGTH: usize = 8;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AttributeID17 {
    bytes: [u8; Self::LENGTH],
}

///
/// 17 bytes lets us use 1 bit of the last byte to determine if the previous 16 bytes represent a hash,
/// or an inline value. Leaving 16 bytes for an inline attribute value is important since many standardised data types,
/// such as UUID, IPv6, are created to fit in a 16 byte encoding scheme.
///
impl AttributeID17 {

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

impl VertexID for AttributeID17 {
    const LENGTH: usize = 17;
}
