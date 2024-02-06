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

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::RangeFrom;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

use itertools::{EitherOrBoth, Itertools};
use serde::{Deserialize, Serialize};

use bytes::byte_array::ByteArray;
use durability::SequenceNumber;

use crate::error::MVCCStorageError;
use crate::isolation_manager::CommitRecord;
use crate::key_value::{StorageKey, StorageKeyArray, StorageKeyReference, StorageValue, StorageValueArray, StorageValueReference};
use crate::keyspace::keyspace::KeyspaceId;
use crate::MVCCStorage;

pub enum Snapshot<'storage> {
    Read(ReadSnapshot<'storage>),
    Write(WriteSnapshot<'storage>),
}

impl<'storage> Snapshot<'storage> {
    pub fn get<'snapshot>(&'snapshot self, key: &StorageKeyReference<SNAPSHOT_INLINE_KEY>) -> Option<StorageValue> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get(key),
            Snapshot::Write(snapshot) => snapshot.get(key),
        }
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKeyReference<SNAPSHOT_INLINE_KEY>) -> Box<dyn Iterator<Item=(Box<[u8]>, StorageValue)> + 'snapshot> {
        match self {
            Snapshot::Read(snapshot) => Box::new(snapshot.iterate_prefix(prefix)),
            Snapshot::Write(snapshot) => Box::new(snapshot.iterate_prefix(prefix)),
        }
    }
}

pub struct ReadSnapshot<'storage> {
    storage: &'storage MVCCStorage,
    open_sequence_number: SequenceNumber,
}

impl<'storage> ReadSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage MVCCStorage, open_sequence_number: SequenceNumber) -> ReadSnapshot {
        // Note: for serialisability, we would need to register the open transaction to the IsolationManager
        ReadSnapshot {
            storage: storage,
            open_sequence_number: open_sequence_number,
        }
    }

    fn get<'snapshot>(&self, key: &StorageKeyReference<'_>) -> Option<StorageValueReference<'_>> {
        self.storage.get_direct(key)
    }

    fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKeyReference<'_>) -> impl Iterator<Item=(Box<[u8]>, StorageValue<'_, SNAPSHOT_INLINE_VALUE>)> + 'snapshot {
        self.storage.iterate_prefix_direct(prefix)
    }
}

pub struct WriteSnapshot<'storage> {
    storage: &'storage MVCCStorage,
    // TODO: replace with BTree Left-Right structure to allow concurrent read/write
    writes: KeyspaceWrites,
    open_sequence_number: SequenceNumber,
}

impl<'storage> WriteSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage MVCCStorage, open_sequence_number: SequenceNumber) -> WriteSnapshot {
        storage.isolation_manager.opened(&open_sequence_number);
        WriteSnapshot {
            storage: storage,
            writes: KeyspaceWrites::new(),
            open_sequence_number: open_sequence_number,
        }
    }

    /// Insert a key with a new version
    pub fn insert(&self, key: StorageKeyArray<SNAPSHOT_INLINE_KEY>) {
        self.insert_val(key, StorageValueArray::empty())
    }

    pub fn insert_val(&self, key: StorageKeyArray<SNAPSHOT_INLINE_KEY>, value: StorageValueArray<SNAPSHOT_INLINE_VALUE>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.writes.insert(keyspace_id, byte_array, value);
    }

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    pub fn put(&self, key: StorageKeyArray<SNAPSHOT_INLINE_KEY>) {
        self.put_val(key, StorageValueArray::empty())
    }

    pub fn put_val(&self, key: StorageKeyArray<SNAPSHOT_INLINE_KEY>, value: StorageValueArray<SNAPSHOT_INLINE_VALUE>) {
        let existing_buffered = self.writes.contains(key.keyspace_id(), key.byte_array());
        if !existing_buffered {
            let existing_stored = self.storage.get(&key, &self.open_sequence_number);
            let keyspace_id = key.keyspace_id();
            let byte_array = key.into_byte_array();
            if existing_stored.is_some() && existing_stored.as_ref().unwrap() == value {
                self.writes.insert_preexisting(keyspace_id, byte_array, existing_stored.unwrap());
            } else {
                self.writes.insert(keyspace_id, byte_array, value)
            }
        } else {
            // TODO: replace existing buffered write. If it contains a preexisting, we can continue to use it
            todo!()
        }
    }

    /// Insert a delete marker for the key with a new version
    pub fn delete(&self, key: StorageKeyArray<SNAPSHOT_INLINE_KEY>) {
        let keyspace_id = key.keyspace_id();
        let byte_array = key.into_byte_array();
        self.writes.delete(keyspace_id, byte_array);
    }

    /// Get a Value, and mark it as a required key
    pub fn get_required(&self, key: StorageKey<'_, SNAPSHOT_INLINE_KEY>) -> StorageValueArray<SNAPSHOT_INLINE_VALUE> {
        let keyspace_id = key.keyspace_id();
        let key_bytes = key.bytes();
        let existing = self.writes.get(keyspace_id, key_bytes);
        if existing.is_none() {
            let storage_value = self.storage.get(&key, &self.open_sequence_number);
            if storage_value.is_some() {
                self.writes.require_exists(keyspace_id, storage_value.as_ref().unwrap().clone());
                return storage_value.unwrap();
            } else {
                // TODO: what if the user concurrent requires a concept while deleting it in another query
                unreachable!("Require key exists in snapshot or in storage.");
            }
        } else {
            let write = existing.unwrap();
            match write {
                Write::Insert(value) => value, // exists
                Write::InsertPreexisting(value, _) => value, // exists
                Write::RequireExists(value) => value, // cached
                Write::Delete => unreachable!("Require key has been marked for deletion"),
            }
        }
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    pub fn get(&self, key: &StorageKey<'_, SNAPSHOT_INLINE_KEY>) -> Option<StorageValueArray<SNAPSHOT_INLINE_VALUE> {


        self.writes.get(key.keyspace_id(), key).map_or_else(
            || self.storage.get_direct(key),
            |write| match write {
                Write::Insert(value) => Some(value.clone()),
                Write::InsertPreexisting(value, _) => Some(value.clone()),
                Write::RequireExists(value) => Some(value.clone()),
                Write::Delete => None,
            },
        )
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKeyReference<SNAPSHOT_INLINE_KEY>) -> impl Iterator<Item=(Box<[u8]>, StorageValue)> + 'snapshot {
        let storage_iterator = self.storage.iterate_prefix_direct(prefix);
        let buffered_iterator = self.iterate_prefix_buffered(prefix);
        storage_iterator.merge_join_by(
            buffered_iterator,
            |(k1, v1), (k2, v2)| k1.cmp(k2),
        ).filter_map(|ordering| match ordering {
            EitherOrBoth::Both((k1, v1), (k2, write2)) => match write2 {
                Write::Insert(v2) => Some((k2, v2)),
                Write::InsertPreexisting(v2, _) => Some((k2, v2)),
                Write::RequireExists(v2) => {
                    debug_assert_eq!(v1, v2);
                    Some((k1, v1))
                }
                Write::Delete => None,
            },
            EitherOrBoth::Left((k1, v1)) => Some((k1, v1)),
            EitherOrBoth::Right((k2, write2)) => match write2 {
                Write::Insert(v2) => Some((k2, v2)),
                Write::InsertPreexisting(v2, _) => Some((k2, v2)),
                Write::RequireExists(_) => unreachable!("Invalid state: a key required to exist must also exists in Storage."),
                Write::Delete => None,
            },
        })
    }

    fn iterate_prefix_buffered<'s>(&'s self, prefix: &StorageKeyReference<SNAPSHOT_INLINE_KEY>) -> impl Iterator<Item=(Box<[u8]>, Write)> + 's {
        let map = self.writes.read().unwrap();
        let range = RangeFrom { start: prefix };
        // TODO: hold read lock while iterating so avoid collecting into array
        map.range::<StorageKeyReference<SNAPSHOT_INLINE_KEY>, _>(range).map(|(key, val)| {
            // TODO: we can avoid allocation here once we settle on a Key/Value struct
            (key.bytes().bytes().into(), val.clone())
        }).collect::<Vec<_>>().into_iter()
    }

    pub fn commit(self) {
        self.storage.snapshot_commit(self);
    }

    pub(crate) fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(
            self.writes.into_inner().unwrap(),
            self.open_sequence_number,
        )
    }
}

pub(crate) const SNAPSHOT_INLINE_KEY: usize = 48;
pub(crate) const SNAPSHOT_INLINE_VALUE: usize = 128;

#[derive(Serialize, Deserialize, Debug)]
struct KeyspaceWrites {
    writes: [RwLock<BTreeMap<ByteArray<SNAPSHOT_INLINE_KEY>, Write>>; KeyspaceId::MAX as usize],
}

impl KeyspaceWrites {
    fn new() -> KeyspaceWrites {
        KeyspaceWrites {
            writes: core::array::from_fn(|i| RwLock::new(BTreeMap::new()))
        }
    }

    pub(crate) fn insert(&self, keyspace_id: KeyspaceId, key: ByteArray<SNAPSHOT_INLINE_KEY>, value: StorageValueArray<SNAPSHOT_INLINE_VALUE>) {
        let mut map = self.writes[keyspace_id].write().unwrap();
        map.insert(key, Write::Insert(value));
    }

    pub(crate) fn insert_preexisting(&self, keyspace_id: KeyspaceId, key: ByteArray<SNAPSHOT_INLINE_KEY>, value: StorageValueArray<SNAPSHOT_INLINE_VALUE>) {
        let mut map = self.writes[keyspace_id].write().unwrap();
        map.insert(key, Write::InsertPreexisting(value, Arc::new(AtomicBool::new(false))));
    }

    pub(crate) fn require_exists(&self, keyspace_id: KeyspaceId, key: ByteArray<SNAPSHOT_INLINE_KEY>, value: StorageValueArray<SNAPSHOT_INLINE_VALUE>) {
        let mut map = self.writes[keyspace_id].write().unwrap();
        // TODO: what if it already has been inserted? Ie. InsertPreexisting?
        map.insert(key, Write::RequireExists(value));
    }

    pub(crate) fn delete(&self, keyspace_id: KeyspaceId, key: ByteArray<SNAPSHOT_INLINE_KEY>) {
        let mut map = self.writes[keyspace_id].write().unwrap();
        if map.get(key.bytes()).map_or(false, |write| write.is_insert()) {
            // undo previous insert
            map.remove(&key);
        } else {
            map.insert(key, Write::Delete);
        }
    }

    pub(crate) fn contains(&self, keyspace_id: KeyspaceId, key: &ByteArray<SNAPSHOT_INLINE_KEY>) -> bool {
        let map = self.writes[keyspace_id].read().unwrap();
        map.get(key).is_some()
    }

    pub(crate) fn get(&self, keyspace_id: KeyspaceId, key: &[u8]) -> Option<StorageValueArray<SNAPSHOT_INLINE_VALUE>> {
        let map = self.writes[keyspace_id].read().unwrap();
        let existing = map.get(key);
        existing.map(
            |write| match write {
                Write::Insert(value) => Some(value.clone()),
                Write::InsertPreexisting(value, _) => Some(value.clone()),
                Write::RequireExists(value) => Some(value.clone()),
                Write::Delete => None,
            },
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Write {
    // Insert KeyValue with a new version. Never conflicts.
    Insert(StorageValueArray<SNAPSHOT_INLINE_VALUE>),
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    InsertPreexisting(StorageValueArray<SNAPSHOT_INLINE_VALUE>, Arc<AtomicBool>),
    // TODO what happens during replay
    // Mark Key as required from storage. Caches existing storage Value. Conflicts with Delete.
    RequireExists(StorageValueArray<SNAPSHOT_INLINE_VALUE>),
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

impl<'a> Eq for Write<'a> {}

impl<'a> Write<'a> {
    pub(crate) fn is_insert(&self) -> bool {
        matches!(self, Write::Insert(_))
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Write::Delete)
    }
}


#[derive(Debug)]
pub struct WriteSnapshotError {
    pub kind: WriteSnapshotErrorKind,
}

#[derive(Debug)]
pub enum WriteSnapshotErrorKind {
    FailedGet { source: MVCCStorageError },
    FailedPut { source: MVCCStorageError },
}

impl Display for WriteSnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteSnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            WriteSnapshotErrorKind::FailedGet { source, .. } => Some(source),
            WriteSnapshotErrorKind::FailedPut { source, .. } => Some(source),
        }
    }
}