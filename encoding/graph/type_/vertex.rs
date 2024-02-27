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
use storage::keyspace::keyspace::KeyspaceId;
use storage::snapshot::buffer::BUFFER_INLINE_KEY;

use crate::{AsBytes, EncodingKeyspace, Keyable, Prefixed};
use crate::layout::prefix::PrefixID;

#[derive(Debug, PartialEq, Eq)]
pub struct TypeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>,
}

impl<'a> TypeVertex<'a> {
    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>) -> TypeVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeVertex { bytes: bytes }
    }

    pub fn build(prefix: &PrefixID<'a>, type_id: &TypeID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(prefix.bytes().bytes());
        array.bytes_mut()[Self::range_type_id()].copy_from_slice(type_id.bytes().bytes());
        TypeVertex { bytes: ByteArrayOrRef::Array(array) }
    }

    pub fn type_id(&'a self) -> TypeID<'a> {
        TypeID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_type_id()])))
    }

    const fn range_type_id() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeID::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_INLINE_KEY> for TypeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_ref()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_KEY> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_INLINE_KEY> for TypeVertex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }
}

impl<'a> Prefixed<'a, BUFFER_INLINE_KEY> for TypeVertex<'a> { }

#[derive(Debug, PartialEq, Eq)]
pub struct TypeID<'a> {
    bytes: ByteArrayOrRef<'a, { TypeID::LENGTH }>,
}

pub type TypeIdUInt = u16;

impl<'a> TypeID<'a> {
    pub(crate) const LENGTH: usize = std::mem::size_of::<TypeIdUInt>();

    pub fn new(bytes: ByteArrayOrRef<'a, { TypeID::LENGTH }>) -> TypeID<'a> {
        debug_assert_eq!(bytes.length(), TypeID::LENGTH);
        TypeID { bytes: bytes }
    }

    pub fn build(id: TypeIdUInt) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), TypeID::LENGTH);
        TypeID { bytes: ByteArrayOrRef::Array(ByteArray::inline(id.to_be_bytes(), TypeID::LENGTH)) }
    }

    pub(crate) fn as_u16(&self) -> u16 {
        u16::from_be_bytes(self.bytes.bytes()[0..Self::LENGTH].try_into().unwrap())
    }
}

impl<'a> AsBytes<'a, { TypeID::LENGTH }> for TypeID<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_ref()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, { TypeID::LENGTH }> {
        self.bytes
    }
}

