/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::mem;
use std::ops::Range;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable, Prefixed};
use crate::graph::type_::vertex::TypeID;
use crate::graph::Typed;
use crate::layout::prefix::PrefixID;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> ObjectVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ObjectNumber::LENGTH;
    const LENGTH_PREFIX_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> ObjectVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        ObjectVertex { bytes: bytes }
    }

    pub fn build(prefix: &PrefixID<'_>, type_number: &TypeID<'_>, object_number: ObjectNumber<'_>) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_number.bytes().bytes());
        array.bytes_mut()[Self::range_object_number()].copy_from_slice(object_number.bytes().bytes());
        ObjectVertex { bytes: ByteArrayOrRef::Array(array) }
    }

    pub fn prefix_prefix(prefix: &PrefixID<'_>) -> StorageKey<'static, {ObjectVertex::LENGTH_PREFIX_PREFIX}> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_PREFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        StorageKey::new(Self::keyspace_id(), ByteArrayOrRef::Array(array))
    }
    fn prefix_type(prefix: &PrefixID<'_>, type_number: &TypeID<'_>) -> StorageKey<'static, {ObjectVertex::LENGTH_PREFIX_TYPE}> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_number.bytes().bytes());
        StorageKey::new(Self::keyspace_id(), ByteArrayOrRef::Array(array))
    }

    fn keyspace_id() -> KeyspaceId {
        // TODO: partition
        EncodingKeyspace::Data.id()
    }

    pub fn object_number(&'a self) -> ObjectNumber<'a> {
        ObjectNumber::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_object_number()])))
    }
    const fn range_object_number() -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + ObjectNumber::LENGTH
    }

    pub fn into_owned(self) -> ObjectVertex<'static> {
        ObjectVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for ObjectVertex<'a> {}

#[derive(Debug, PartialEq, Eq)]
pub struct ObjectNumber<'a> {
    bytes: ByteArrayOrRef<'a, { ObjectNumber::LENGTH }>,
}

impl<'a> ObjectNumber<'a> {
    const LENGTH: usize = 8;

    fn new(bytes: ByteArrayOrRef<'a, { ObjectNumber::LENGTH }>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        ObjectNumber { bytes: bytes }
    }

    pub fn build(id: u64) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), Self::LENGTH);
        ObjectNumber { bytes: ByteArrayOrRef::Array(ByteArray::inline(id.to_be_bytes(), Self::LENGTH)) }
    }
}

impl<'a> AsBytes<'a, { ObjectNumber::LENGTH }> for ObjectNumber<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { ObjectNumber::LENGTH }> {
        self.bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> AttributeVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + AttributeID::LENGTH;
    pub(crate) const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub(crate) fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        AttributeVertex { bytes: bytes }
    }

    fn build(prefix: &PrefixID<'a>, type_number: &TypeID<'_>, attribute_number: AttributeID) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_number.bytes().bytes());
        bytes.bytes_mut()[Self::range_attribute_number()].copy_from_slice(attribute_number.bytes().bytes());
        Self { bytes: ByteArrayOrRef::Array(bytes) }
    }

    pub fn attribute_number(&'a self) -> AttributeID<'a> {
        AttributeID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_attribute_number()])))
    }

    const fn range_attribute_number() -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + AttributeID::LENGTH
    }

    fn to_owned(&self) -> AttributeVertex<'static> {
        AttributeVertex { bytes: self.bytes.to_owned() }
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

#[derive(Debug, PartialEq, Eq)]
struct AttributeID<'a> {
    bytes: ByteArrayOrRef<'a, { AttributeID::LENGTH }>,
}

impl<'a> AttributeID<'a> {
    const HEADER_LENGTH: usize = 4;
    const NUMBER_LENGTH: usize = 8;
    const LENGTH: usize = Self::HEADER_LENGTH + Self::NUMBER_LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, { AttributeID::LENGTH }>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        AttributeID { bytes: bytes }
    }

    pub fn build(header: &[u8; AttributeID::HEADER_LENGTH], id: u64) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), Self::NUMBER_LENGTH);
        let id_bytes = id.to_be_bytes();
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::range_header()].copy_from_slice(header);
        array.bytes_mut()[Self::range_number()].copy_from_slice(&id_bytes);
        AttributeID { bytes: ByteArrayOrRef::Array(array) }
    }

    fn header(&'a self) -> ByteReference<'a> {
        ByteReference::new(&self.bytes.bytes()[Self::range_header()])
    }

    fn number(&'a self) -> ByteReference<'a> {
        ByteReference::new(&self.bytes.bytes()[Self::range_number()])
    }

    const fn range_header() -> Range<usize> {
        0..Self::HEADER_LENGTH
    }

    const fn range_number() -> Range<usize> {
        Self::range_header().end..Self::range_header().end + Self::NUMBER_LENGTH
    }
}

impl<'a> AsBytes<'a, { AttributeID::LENGTH }> for AttributeID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { AttributeID::LENGTH }> {
        self.bytes
    }
}
