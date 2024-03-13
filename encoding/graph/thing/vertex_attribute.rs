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

use std::mem;
use std::ops::Range;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{AsBytes, Prefixed};
use crate::graph::type_::vertex::TypeID;
use crate::graph::Typed;
use crate::layout::prefix::{PrefixID, PrefixType};
use crate::property::value_type::ValueType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> AttributeVertex<'a> {
    pub(crate) const LENGTH_PREFIX_TYPE: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub(crate) fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() > Self::LENGTH_PREFIX_TYPE);
        AttributeVertex { bytes: bytes }
    }

    fn build(prefix: PrefixID, type_id: &TypeID<'_>, attribute_id: AttributeID) -> Self {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_TYPE + attribute_id.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(type_id.bytes().bytes());
        bytes.bytes_mut()[Self::range_for_attribute_id(&attribute_id)].copy_from_slice(attribute_id.bytes().bytes());
        Self { bytes: ByteArrayOrRef::Array(bytes) }
    }

    pub fn value_type(&self) -> ValueType {
        match self.prefix() {
            PrefixType::VertexAttributeLong => ValueType::Long,
            _ => unreachable!("Unexpected prefix.")
        }
    }

    fn attribute_id(&'a self) -> AttributeID<'a> {
        AttributeID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[self.range_of_attribute_id()])))
    }

    fn range_of_attribute_id(&self) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..self.length()
    }

    fn range_for_attribute_id(attribute_id: &AttributeID) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + attribute_id.length()
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
        array.bytes_mut()[Self::range_id()].copy_from_slice(&id_bytes);
        AttributeID { bytes: ByteArrayOrRef::Array(array) }
    }

    fn header(&'a self) -> ByteReference<'a> {
        ByteReference::new(&self.bytes.bytes()[Self::range_header()])
    }

    fn id(&'a self) -> ByteReference<'a> {
        ByteReference::new(&self.bytes.bytes()[Self::range_id()])
    }

    fn length(&self) -> usize {
        self.bytes.length()
    }

    const fn range_header() -> Range<usize> {
        0..Self::HEADER_LENGTH
    }

    const fn range_id() -> Range<usize> {
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
