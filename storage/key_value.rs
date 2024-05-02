/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, cmp::Ordering};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use primitive::prefix::Prefix;
use serde::{Deserialize, Serialize};

use crate::keyspace::{KeyspaceId, KeyspaceSet};

#[derive(Debug, Clone)]
pub enum StorageKey<'bytes, const S: usize> {
    Array(StorageKeyArray<S>),
    Reference(StorageKeyReference<'bytes>),
}

impl<'bytes, const S: usize> StorageKey<'bytes, S> {
    pub fn new<KS: KeyspaceSet>(keyspace: KS, bytes: Bytes<'bytes, S>) -> Self {
        match bytes {
            Bytes::Array(array) => Self::Array(StorageKeyArray::new(keyspace, array)),
            Bytes::Reference(reference) => Self::Reference(StorageKeyReference::new(keyspace, reference)),
        }
    }

    pub fn new_ref<KS: KeyspaceSet>(keyspace: KS, bytes: ByteReference<'bytes>) -> Self {
        StorageKey::Reference(StorageKeyReference::new(keyspace, bytes))
    }

    pub fn new_owned<KS: KeyspaceSet>(keyspace: KS, bytes: ByteArray<S>) -> Self {
        StorageKey::Array(StorageKeyArray::new(keyspace, bytes))
    }

    pub fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            StorageKey::Array(array) => array.bytes(),
            StorageKey::Reference(reference) => reference.bytes(),
        }
    }

    pub fn into_byte_array_or_ref(self) -> Bytes<'bytes, S> {
        match self {
            StorageKey::Array(array) => Bytes::Array(array.into_byte_array()),
            StorageKey::Reference(reference) => Bytes::Reference(reference.into_byte_ref()),
        }
    }

    pub fn keyspace_id(&self) -> KeyspaceId {
        match self {
            StorageKey::Array(bytes) => bytes.keyspace_id,
            StorageKey::Reference(bytes) => bytes.keyspace_id,
        }
    }

    pub fn length(&self) -> usize {
        match self {
            StorageKey::Array(key) => key.length(),
            StorageKey::Reference(key) => key.length(),
        }
    }

    pub fn as_reference(&'bytes self) -> StorageKeyReference<'bytes> {
        match self {
            StorageKey::Array(array) => StorageKeyReference::from(array),
            StorageKey::Reference(reference) => {
                StorageKeyReference::new_raw(reference.keyspace_id(), reference.byte_ref())
            }
        }
    }

    pub fn into_owned_array(self) -> StorageKeyArray<S> {
        match self {
            StorageKey::Array(array) => array,
            StorageKey::Reference(reference) => StorageKeyArray::from(reference),
        }
    }
}

impl<'bytes, const SZ: usize> PartialEq<Self> for StorageKey<'bytes, SZ> {
    fn eq(&self, other: &Self) -> bool {
        self.keyspace_id() == other.keyspace_id() && self.bytes() == other.bytes()
    }
}

impl<'bytes, const SZ: usize> Eq for StorageKey<'bytes, SZ> {}

impl<'bytes, const SZ: usize> PartialOrd<Self> for StorageKey<'bytes, SZ> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO: should this take into account Keyspace ID?
        Some(self.cmp(other))
    }
}

impl<'bytes, const SZ: usize> Ord for StorageKey<'bytes, SZ> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

impl<'bytes, const SZ: usize> Prefix for StorageKey<'bytes, SZ> {
    fn starts_with(&self, other: &Self) -> bool {
        self.bytes().starts_with(other.bytes())
    }
}

// TODO: we may want to fix the SZ for all storage keys here
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StorageKeyArray<const SZ: usize> {
    keyspace_id: KeyspaceId,
    byte_array: ByteArray<SZ>,
}

impl<const SZ: usize> StorageKeyArray<SZ> {
    pub fn new<KS: KeyspaceSet>(keyspace: KS, array: ByteArray<SZ>) -> Self {
        Self::new_raw(keyspace.id(), array)
    }

    pub(crate) fn new_raw(keyspace_id: KeyspaceId, array: ByteArray<SZ>) -> Self {
        Self { keyspace_id, byte_array: array }
    }

    pub(crate) fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    pub fn length(&self) -> usize {
        self.byte_array.length()
    }

    pub fn bytes(&self) -> &[u8] {
        self.byte_array.bytes()
    }

    pub fn byte_array(&self) -> &ByteArray<SZ> {
        &self.byte_array
    }

    pub fn into_byte_array(self) -> ByteArray<SZ> {
        self.byte_array
    }
}

impl<const SZ: usize> PartialEq<Self> for StorageKeyArray<SZ> {
    fn eq(&self, other: &Self) -> bool {
        (self.keyspace_id(), self.bytes()) == (other.keyspace_id(), other.bytes())
    }
}

impl<const SZ: usize> Eq for StorageKeyArray<SZ> {}

impl<const SZ: usize> PartialOrd<Self> for StorageKeyArray<SZ> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const SZ: usize> Ord for StorageKeyArray<SZ> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.keyspace_id(), self.bytes()).cmp(&(other.keyspace_id(), other.bytes()))
    }
}

impl<const SZ: usize> Borrow<[u8]> for StorageKeyArray<SZ> {
    fn borrow(&self) -> &[u8] {
        self.bytes()
    }
}

impl<const SZ: usize> From<StorageKeyReference<'_>> for StorageKeyArray<SZ> {
    fn from(key: StorageKeyReference<'_>) -> Self {
        Self { keyspace_id: key.keyspace_id(), byte_array: ByteArray::copy(key.bytes()) }
    }
}

impl<const SZ: usize, KS: KeyspaceSet> From<(KS, Vec<u8>)> for StorageKeyArray<SZ> {
    fn from((keyspace, bytes): (KS, Vec<u8>)) -> Self {
        Self::new(keyspace, ByteArray::boxed(bytes.into_boxed_slice()))
    }
}

impl<const SZ: usize, KS: KeyspaceSet, const A: usize> From<(KS, [u8; A])> for StorageKeyArray<SZ> {
    fn from((keyspace, bytes): (KS, [u8; A])) -> Self {
        Self::new(keyspace, ByteArray::boxed(Box::new(bytes)))
    }
}

impl<const SZ: usize, KS: KeyspaceSet, const A: usize> From<(KS, &[u8; A])> for StorageKeyArray<SZ> {
    fn from((keyspace, bytes): (KS, &[u8; A])) -> Self {
        Self::new(keyspace, ByteArray::copy(bytes))
    }
}

impl<const SZ: usize, KS: KeyspaceSet> From<(KS, &[u8])> for StorageKeyArray<SZ> {
    fn from((keyspace, bytes): (KS, &[u8])) -> Self {
        Self::new(keyspace, ByteArray::copy(bytes))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageKeyReference<'bytes> {
    keyspace_id: KeyspaceId,
    reference: ByteReference<'bytes>,
}

impl<'bytes> StorageKeyReference<'bytes> {
    pub fn new<KS: KeyspaceSet>(keyspace: KS, reference: ByteReference<'bytes>) -> StorageKeyReference<'bytes> {
        Self::new_raw(keyspace.id(), reference)
    }

    pub(crate) const fn new_raw(keyspace_id: KeyspaceId, reference: ByteReference<'bytes>) -> Self {
        Self { keyspace_id, reference }
    }

    pub fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    pub fn length(&self) -> usize {
        self.reference.length()
    }

    pub fn bytes(&self) -> &[u8] {
        self.reference.bytes()
    }

    pub fn byte_ref(&self) -> ByteReference<'bytes> {
        self.reference
    }

    pub(crate) fn into_byte_ref(self) -> ByteReference<'bytes> {
        self.reference
    }
}

impl<'bytes, const SZ: usize> From<&'bytes StorageKeyArray<SZ>> for StorageKeyReference<'bytes> {
    fn from(array_ref: &'bytes StorageKeyArray<SZ>) -> Self {
        StorageKeyReference::new_raw(array_ref.keyspace_id, array_ref.byte_array().as_ref())
    }
}

impl<'bytes> PartialEq<Self> for StorageKeyReference<'bytes> {
    fn eq(&self, other: &Self) -> bool {
        (self.keyspace_id(), self.bytes()) == (other.keyspace_id(), other.bytes())
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
        (self.keyspace_id(), self.bytes()).cmp(&(other.keyspace_id(), other.bytes()))
    }
}
