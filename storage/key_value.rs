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

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;

use crate::keyspace::keyspace::KeyspaceId;

#[derive(Debug)]
pub enum StorageKey<'bytes, const INLINE_SIZE: usize> {
    Array(StorageKeyArray<INLINE_SIZE>),
    Reference(StorageKeyReference<'bytes>),
}

impl<'bytes, const INLINE_SIZE: usize> StorageKey<'bytes, INLINE_SIZE> {
    pub fn new(keyspace_id: KeyspaceId, bytes: ByteArrayOrRef<'bytes, INLINE_SIZE>) -> Self {
        match bytes {
            ByteArrayOrRef::Array(array) => StorageKey::Array(StorageKeyArray::new(keyspace_id, array)),
            ByteArrayOrRef::Reference(reference) => StorageKey::Reference(StorageKeyReference::new(keyspace_id, reference)),
        }
    }

    pub fn new_ref(keyspace_id: KeyspaceId, bytes: ByteReference<'bytes>) -> Self {
        StorageKey::Reference(StorageKeyReference::new(keyspace_id, bytes))
    }

    pub fn new_owned(keyspace_id: KeyspaceId, bytes: ByteArrayOrRef<'bytes, INLINE_SIZE>) -> Self {
        StorageKey::Array(StorageKeyArray::new(keyspace_id, bytes.into_owned()))
    }

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

    pub fn as_reference(&'bytes self) -> StorageKeyReference<'bytes> {
        match self {
            StorageKey::Array(array) => StorageKeyReference::from(array),
            StorageKey::Reference(reference) => StorageKeyReference::new(reference.keyspace_id(), reference.byte_ref().clone()),
        }
    }

    pub fn to_owned(&self) -> StorageKeyArray<INLINE_SIZE> {
        match self {
            StorageKey::Array(array) => array.clone(),
            StorageKey::Reference(reference) => StorageKeyArray::from(reference.clone()),
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

// TODO: we may want to fix the INLINE_SIZE for all storage keys here
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageKeyArray<const INLINE_SIZE: usize> {
    keyspace_id: KeyspaceId,
    byte_array: ByteArray<INLINE_SIZE>,
}

impl<const INLINE_SIZE: usize> StorageKeyArray<INLINE_SIZE> {
    pub fn new(keyspace_id: KeyspaceId, array: ByteArray<INLINE_SIZE>) -> StorageKeyArray<INLINE_SIZE> {
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

impl<const INLINE_SIZE: usize> From<StorageKeyReference<'_>> for StorageKeyArray<INLINE_SIZE> {
    fn from(key: StorageKeyReference<'_>) -> Self {
        StorageKeyArray {
            keyspace_id: key.keyspace_id(),
            byte_array: ByteArray::copy(key.bytes()),
        }
    }
}

impl<const INLINE_SIZE: usize> From<(Vec<u8>, u8)> for StorageKeyArray<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (Vec<u8>, u8)) -> Self {
        From::from((bytes.as_slice(), section_id))
    }
}

impl<const INLINE_SIZE: usize> From<(&[u8], u8)> for StorageKeyArray<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (&[u8], u8)) -> Self {
        let bytes = ByteArray::<INLINE_SIZE>::copy(bytes);
        StorageKeyArray {
            keyspace_id: section_id,
            byte_array: bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageKeyReference<'bytes> {
    keyspace_id: KeyspaceId,
    reference: ByteReference<'bytes>,
}

impl<'bytes> StorageKeyReference<'bytes> {
    pub fn new(keyspace_id: KeyspaceId, reference: ByteReference<'bytes>) -> StorageKeyReference<'bytes> {
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


impl<'bytes> PartialEq<Self> for StorageKeyReference<'bytes> {
    fn eq(&self, other: &Self) -> bool {
        self.keyspace_id() == other.keyspace_id() && self.bytes() == other.bytes()
    }
}

impl<'bytes> Eq for StorageKeyReference<'bytes> {}

impl<'bytes> PartialOrd<Self> for StorageKeyReference<'bytes> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO: should this take into account Keyspace ID?
        Some(self.cmp(other))
    }
}

impl<'bytes> Ord for StorageKeyReference<'bytes> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(&other.bytes())
    }
}
