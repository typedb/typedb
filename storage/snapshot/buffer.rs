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

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{Bound, BTreeMap};
use bytes::byte_array::ByteArray;
use crate::key_value::{StorageKeyArray, StorageValueArray};
use crate::keyspace::keyspace::{KEYSPACE_ID_MAX, KeyspaceId};

pub(crate) const BUFFER_INLINE_KEY: usize = 48;
pub(crate) const BUFFER_INLINE_VALUE: usize = 128;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct KeyspaceBuffers {
    buffers: [RwLock<BTreeMap<ByteArray<BUFFER_INLINE_KEY>, Write>>; KEYSPACE_ID_MAX],
}

// TODO: implement our own alternative to BTreeMap, which
//       1) allows storing StorageKeyArray's directly, while doing lookup with any StorageKey. Then we would not need to allocate one buffer per keyspace ahead of time.
//       2) stores an initial set of ordered keys inline - BTreeMap immediately allocates on the heap for the first element and amortize allocating all Writes into one.
//       3) We would benefit hugely from a table where writes are never moved, so we can freely take references to existing writes without having to Clone them out every time... This might lead us to a RocksDB-like Buffer+Index structure
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub(crate) struct KeyspaceBuffer {
    buffer: RwLock<BTreeMap<ByteArray<BUFFER_INLINE_KEY>, Write>>
}

impl KeyspaceBuffer {
    pub(crate) fn new() -> KeyspaceBuffer {
        KeyspaceBuffer {
            buffer: RwLock::new(BTreeMap::new())
        }
    }

    // fn new() -> KeyspaceBuffers {
    //     KeyspaceBuffers {
    //         buffers: core::array::from_fn(|i| RwLock::new(BTreeMap::new()))
    //     }
    // }
    //
    // fn get_keyspace_writes(&self, keyspace_id: KeyspaceId) -> RwLock<BTreeMap<ByteArray<BUFFER_INLINE_KEY>, Write>> {
    //     self.buffers[keyspace_id as usize]
    // }

    pub(crate) fn insert(&self, key: ByteArray<BUFFER_INLINE_KEY>, value: StorageValueArray<BUFFER_INLINE_VALUE>) {
        let mut map = self.buffer.write().unwrap();
        map.insert(key, Write::Insert(value));
    }

    pub(crate) fn insert_preexisting(&self, key: ByteArray<BUFFER_INLINE_KEY>, value: StorageValueArray<BUFFER_INLINE_VALUE>) {
        let mut map = self.buffer.write().unwrap();
        map.insert(key, Write::InsertPreexisting(value, Arc::new(AtomicBool::new(false))));
    }

    pub(crate) fn require_exists(&self, key: ByteArray<BUFFER_INLINE_KEY>, value: StorageValueArray<BUFFER_INLINE_VALUE>) {
        let mut map = self.buffer.write().unwrap();
        // TODO: what if it already has been inserted? Ie. InsertPreexisting?
        map.insert(key, Write::RequireExists(value));
    }

    pub(crate) fn delete(&self, key: ByteArray<BUFFER_INLINE_KEY>) {
        let mut map = self.buffer.write().unwrap();
        if map.get(key.bytes()).map_or(false, |write| write.is_insert()) {
            // undo previous insert
            map.remove(key.bytes());
        } else {
            map.insert(key, Write::Delete);
        }
    }

    pub(crate) fn contains(&self, key: &ByteArray<BUFFER_INLINE_KEY>) -> bool {
        let map = self.buffer.read().unwrap();
        map.get(key.bytes()).is_some()
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<StorageValueArray<BUFFER_INLINE_VALUE>> {
        let map = self.buffer.read().unwrap();
        let existing = map.get(key);
        if let Some(write) = existing {
            match write {
                Write::Insert(value) => Some(value.clone()),
                Write::InsertPreexisting(value, _) => Some(value.clone()),
                Write::RequireExists(value) => Some(value.clone()),
                Write::Delete => None,
            }
        } else {
            None
        }
    }

    pub(crate) fn iterate_prefix<'this>(&'this self, keyspace_id: KeyspaceId, prefix: &[u8]) -> impl Iterator<Item=(StorageKeyArray<BUFFER_INLINE_KEY>, Write)> + 'this {
        let map = self.buffer.read().unwrap();


        // TODO: stop iterator after prefix+1


        // TODO: hold read lock while iterating so avoid collecting into array
        map.range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded)).map(|(key, val)| {
            // TODO: we can avoid allocation here once we settle on a Key/Value struct
            (StorageKeyArray::new(keyspace_id, key.clone()), val.clone())
        }).collect::<Vec<_>>().into_iter()
    }

    pub(crate) fn map(&self) -> &RwLock<BTreeMap<ByteArray<BUFFER_INLINE_KEY>, Write>> {
        &self.buffer
    }

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Write {
    // Insert KeyValue with a new version. Never conflicts.
    Insert(StorageValueArray<BUFFER_INLINE_VALUE>),
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    InsertPreexisting(StorageValueArray<BUFFER_INLINE_VALUE>, Arc<AtomicBool>),
    // TODO what happens during replay
    // Mark Key as required from storage. Caches existing storage Value. Conflicts with Delete.
    RequireExists(StorageValueArray<BUFFER_INLINE_VALUE>),
    // Delete with a new version. Conflicts with Require.
    Delete,
}

impl PartialEq<Self> for Write {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Write::Insert(value) => {
                if let Write::Insert(other_value) = other {
                    value == other_value
                } else {
                    false
                }
            }
            Write::InsertPreexisting(value, reinsert) => {
                if let Write::InsertPreexisting(other_value, other_reinsert) = other {
                    other_value == value &&
                        reinsert.load(Ordering::SeqCst) == other_reinsert.load(Ordering::SeqCst)
                } else {
                    false
                }
            }
            Write::RequireExists(value) => {
                if let Write::RequireExists(other_value) = other {
                    value == other_value
                } else {
                    false
                }
            }
            Write::Delete => {
                matches!(other, Write::Delete)
            }
        }
    }
}

impl Eq for Write {}

impl Write {
    pub(crate) fn is_insert(&self) -> bool {
        matches!(self, Write::Insert(_))
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Write::Delete)
    }
}

