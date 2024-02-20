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
use storage::key_value::StorageKey;
use storage::keyspace::keyspace::KeyspaceId;
use storage::snapshot::buffer::BUFFER_INLINE_KEY;
use crate::EncodingKeyspace;

use crate::layout::prefix::Prefix;

#[derive(Debug, PartialEq, Eq)]
pub struct TypeVertex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>,
}

impl<'a> TypeVertex<'a> {
    pub(crate) const LENGTH: usize = Prefix::LENGTH + TypeID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>) -> TypeVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeVertex {
            bytes: bytes,
        }
    }

    pub(crate) fn build(prefix: &Prefix<'a>, type_id: &TypeID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::range_prefix()].copy_from_slice(prefix.bytes().bytes());
        array.bytes_mut()[Self::range_type_id()].copy_from_slice(type_id.bytes().bytes());
        TypeVertex { bytes: ByteArrayOrRef::Array(array) }
    }

    pub fn prefix(&'a self) -> Prefix<'a> {
        Prefix::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_prefix()])))
    }

    pub fn as_storage_key(&'a self) -> StorageKey<'a, BUFFER_INLINE_KEY> {
        StorageKey::new_ref(self.keyspace_id(), &self.bytes)
    }

    pub fn into_storage_key(self) -> StorageKey<'a, BUFFER_INLINE_KEY> {
        StorageKey::new_owned(self.keyspace_id(), self.into_bytes())
    }

    pub fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_KEY> {
        self.bytes
    }

    pub fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_ref()
    }

    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }

    const fn range_prefix() -> Range<usize> {
        0..Prefix::LENGTH
    }

    const fn range_type_id() -> Range<usize> {
        Self::range_prefix().end..Self::range_prefix().end + TypeID::LENGTH
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TypeID<'a> {
    bytes: ByteArrayOrRef<'a, { TypeID::LENGTH }>,
}

pub type TypeIdUInt = u16;

impl<'a> TypeID<'a> {
    const LENGTH: usize = std::mem::size_of::<TypeIdUInt>();

    pub fn build(id: TypeIdUInt) -> Self {
        debug_assert_eq!(mem::size_of_val(&id), TypeID::LENGTH);
        TypeID { bytes: ByteArrayOrRef::Array(ByteArray::inline(id.to_be_bytes(), TypeID::LENGTH)) }
    }

    pub(crate) fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_ref()
    }

    pub(crate) fn as_u16(&self) -> u16 {
        u16::from_be_bytes(self.bytes.bytes()[0..Self::LENGTH].try_into().unwrap())
    }
}


