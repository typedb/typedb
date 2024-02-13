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

use std::borrow::Borrow;
use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;

use crate::keyspace::keyspace::KeyspaceId;

#[derive(Debug)]
pub enum StorageKey<'bytes, const INLINE_SIZE: usize> {
    Array(StorageKeyArray<INLINE_SIZE>),
    Reference(StorageKeyReference<'bytes>),
}

impl<'bytes, const INLINE_SIZE: usize> StorageKey<'bytes, INLINE_SIZE> {
    pub fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            StorageKey::Array(array) => array.bytes(),
            StorageKey::Reference(reference) => reference.bytes(),
        }
    }

    pub(crate) fn keyspace_id(&self) -> KeyspaceId {
        match self {
            StorageKey::Array(bytes) => bytes.keyspace_id,
            StorageKey::Reference(bytes) => bytes.keyspace_id,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageKeyArray<const INLINE_SIZE: usize> {
    keyspace_id: KeyspaceId,
    byte_array: ByteArray<INLINE_SIZE>,
}

impl<const INLINE_SIZE: usize> StorageKeyArray<INLINE_SIZE> {
    pub(crate) fn new(keyspace_id: KeyspaceId, array: ByteArray<INLINE_SIZE>) -> StorageKeyArray<INLINE_SIZE> {
        StorageKeyArray {
            keyspace_id: keyspace_id,
            byte_array: array,
        }
    }

    pub(crate) fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    pub fn bytes(&self) -> &[u8] {
        self.byte_array.bytes()
    }

    pub(crate) fn byte_array(&self) -> &ByteArray<INLINE_SIZE> {
        &self.byte_array
    }

    pub fn into_byte_array(self) -> ByteArray<INLINE_SIZE> {
        self.byte_array
    }
}


// TODO: we may want to fix the INLINE_SIZE for all storage keys here
#[derive(Debug, PartialEq, Eq)]
pub struct StorageKeyReference<'bytes> {
    keyspace_id: KeyspaceId,
    reference: ByteReference<'bytes>,
}

impl<'bytes> StorageKeyReference<'bytes> {
    pub(crate) fn new(keyspace_id: KeyspaceId, reference: ByteReference<'bytes>) -> StorageKeyReference<'bytes> {
        StorageKeyReference {
            keyspace_id: keyspace_id,
            reference: reference,
        }
    }

    pub(crate) fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    pub fn bytes(&self) -> &[u8] {
        self.reference.bytes()
    }

    pub(crate) fn byte_ref(&self) -> &ByteReference<'bytes> {
        &self.reference
    }

    pub(crate) fn into_byte_ref(self) -> ByteReference<'bytes> {
        self.reference
    }
}

impl<'bytes, const INLINE_SIZE: usize> From<&'bytes StorageKeyArray<INLINE_SIZE>> for StorageKeyReference<'bytes> {
    fn from(array_ref: &'bytes StorageKeyArray<INLINE_SIZE>) -> Self {
        StorageKeyReference::new(array_ref.keyspace_id, ByteReference::from(array_ref.byte_array()))
    }
}

impl<const INLINE_SIZE: usize> From<(Vec<u8>, u8)> for StorageKeyArray<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (Vec<u8>, u8)) -> Self {
        StorageKeyArray::from((bytes.as_slice(), section_id))
    }
}

impl<const INLINE_SIZE: usize> From<(&[u8], u8)> for StorageKeyArray<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (&[u8], u8)) -> Self {
        let bytes = ByteArray::<INLINE_SIZE>::from(bytes);
        StorageKeyArray {
            keyspace_id: section_id,
            byte_array: bytes,
        }
    }
}

impl<'bytes, const INLINE_SIZE: usize> PartialEq<Self> for StorageKey<'bytes, INLINE_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.keyspace_id() == other.keyspace_id() && self.bytes() == other.bytes()
    }
}

impl<'bytes, const INLINE_SIZE: usize> Eq for StorageKey<'bytes, INLINE_SIZE> {}

impl<'bytes, const INLINE_SIZE: usize> PartialOrd<Self> for StorageKey<'bytes, INLINE_SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO: should this take into account Keyspace ID?
        Some(self.cmp(other))
    }
}

impl<'bytes, const INLINE_SIZE: usize> Ord for StorageKey<'bytes, INLINE_SIZE> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(&other.bytes())
    }
}

#[derive(Debug)]
pub enum StorageValue<'bytes, const INLINE_SIZE: usize> {
    Array(StorageValueArray<INLINE_SIZE>),
    Reference(StorageValueReference<'bytes>),
}

impl<'bytes, const INLINE_SIZE: usize> StorageValue<'bytes, INLINE_SIZE> {
    pub fn empty() -> StorageValue<'bytes, INLINE_SIZE> {
        StorageValue::Array(StorageValueArray::empty())
    }

    pub fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            StorageValue::Array(array) => array.bytes(),
            StorageValue::Reference(reference) => reference.bytes(),
        }
    }
}

impl<'bytes, const INLINE_SIZE: usize> PartialEq<Self> for StorageValue<'bytes, INLINE_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<'bytes, const INLINE_SIZE: usize> Eq for StorageValue<'bytes, INLINE_SIZE> {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageValueArray<const INLINE_SIZE: usize> {
    byte_array: ByteArray<INLINE_SIZE>,
}

impl<const INLINE_SIZE: usize> StorageValueArray<INLINE_SIZE> {
    pub const fn empty() -> StorageValueArray<INLINE_SIZE> {
        StorageValueArray {
            byte_array: ByteArray::empty()
        }
    }

    pub fn new(array: ByteArray<INLINE_SIZE>) -> StorageValueArray<INLINE_SIZE> {
        StorageValueArray {
            byte_array: array
        }
    }

    pub(crate) fn byte_array(&self) -> &ByteArray<INLINE_SIZE> {
        &self.byte_array
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        self.byte_array.bytes()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StorageValueReference<'bytes> {
    reference: ByteReference<'bytes>,
}

impl<'bytes> StorageValueReference<'bytes> {
    pub(crate) fn new(reference: ByteReference<'bytes>) -> StorageValueReference<'bytes> {
        StorageValueReference {
            reference: reference
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        self.reference.bytes()
    }
}

impl <'bytes, const INLINE_SIZE: usize> From<&'bytes StorageValueArray<INLINE_SIZE>> for StorageValueReference<'bytes> {
    fn from(array_ref: &'bytes StorageValueArray<INLINE_SIZE>) -> Self {
        StorageValueReference::new(ByteReference::from(array_ref.byte_array()))
    }
}

impl<'bytes, const INLINE_SIZE: usize> From<Option<Box<[u8]>>> for StorageValue<'bytes, INLINE_SIZE> {
    fn from(value: Option<Box<[u8]>>) -> Self {
        value.map_or_else(|| StorageValue::empty(), |bytes| StorageValue::Array(StorageValueArray::new(ByteArray::boxed(bytes))))
    }
}
