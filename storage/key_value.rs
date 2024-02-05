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
use serde::de::Visitor;

use bytes::byte_array::ByteArray;

use crate::keyspace::keyspace::KeyspaceId;

// TODO: we may want to fix the INLINE_SIZE for all storage keys here
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StorageKey<const INLINE_SIZE: usize> {
    keyspace_id: KeyspaceId,
    bytes: ByteArray<INLINE_SIZE>,
}

impl<const INLINE_SIZE: usize> StorageKey<INLINE_SIZE> {
    pub fn new(keyspace_id: KeyspaceId, bytes: ByteArray<INLINE_SIZE>) -> StorageKey<INLINE_SIZE> {
        StorageKey {
            keyspace_id: keyspace_id,
            bytes: bytes,
        }
    }

    pub fn bytes(&self) -> &ByteArray<INLINE_SIZE> {
        &self.bytes
    }

    pub(crate) fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }
}

impl<const INLINE_SIZE: usize> From<(Vec<u8>, u8)> for StorageKey<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (Vec<u8>, u8)) -> Self {
        StorageKey::from((bytes.as_slice(), section_id))
    }
}

impl<const INLINE_SIZE: usize> From<(&[u8], u8)> for StorageKey<INLINE_SIZE> {
    // For tests
    fn from((bytes, section_id): (&[u8], u8)) -> Self {
        let bytes = ByteArray::<INLINE_SIZE>::from(bytes);
        StorageKey {
            keyspace_id: section_id,
            bytes: bytes,
        }
    }
}

// TODO: should these take into account Keyspace ID?
impl<const INLINE_SIZE: usize> PartialOrd<Self> for StorageKey<INLINE_SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const INLINE_SIZE: usize> Ord for StorageKey<INLINE_SIZE> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StorageValue {
    Empty,
    Value(Box<[u8]>),
}

impl StorageValue {
    pub fn bytes(&self) -> &[u8] {
        match self {
            StorageValue::Empty => &[0; 0],
            StorageValue::Value(bytes) => bytes,
        }
    }

    pub fn has_value(&self) -> bool {
        match self {
            StorageValue::Empty => false,
            StorageValue::Value(_) => true,
        }
    }
}

impl From<Option<Box<[u8]>>> for StorageValue {
    fn from(value: Option<Box<[u8]>>) -> Self {
        value.map_or_else(|| StorageValue::Empty, |bytes| StorageValue::Value(bytes))
    }
}
