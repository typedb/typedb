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

use durability::SequenceNumber;

use crate::error::MVCCStorageError;
use crate::isolation_manager::CommitRecord;
use crate::key_value::{StorageKey, StorageValue};
use crate::MVCCStorage;

pub enum Snapshot<'storage> {
    Read(ReadSnapshot<'storage>),
    Write(WriteSnapshot<'storage>),
}

impl<'storage> Snapshot<'storage> {
    pub fn get<'snapshot>(&'snapshot self, key: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> Option<StorageValue> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get(key),
            Snapshot::Write(snapshot) => snapshot.get(key),
        }
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> Box<dyn Iterator<Item=(Box<[u8]>, StorageValue)> + 'snapshot> {
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

    fn get<'snapshot>(&self, key: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> Option<StorageValue> {
        self.storage.get_direct(key)
    }

    fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> impl Iterator<Item=(Box<[u8]>, StorageValue)> + 'snapshot {
        self.storage.iterate_prefix_direct(prefix)
    }
}

pub struct WriteSnapshot<'storage> {
    storage: &'storage MVCCStorage,
    // TODO: replace with BTree Left-Right structure to allow concurrent read/write
    writes: RwLock<WriteData>,
    open_sequence_number: SequenceNumber,
}

impl<'storage> WriteSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage MVCCStorage, open_sequence_number: SequenceNumber) -> WriteSnapshot {
        storage.isolation_manager.opened(&open_sequence_number);
        WriteSnapshot {
            storage: storage,
            writes: RwLock::new(WriteData::new()),
            open_sequence_number: open_sequence_number,
        }
    }

    /// Insert a key with a new version
    pub fn insert(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, Write::Insert(StorageValue::Empty));
    }

    pub fn insert_val(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>, value: StorageValue) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, Write::Insert(value));
    }

    /// Insert a key with a new version if it does not already exist.
    /// If the key exists, mark it as a preexisting insertion to escalate to Insert if there is a concurrent Delete.
    pub fn put(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>) {
        let existing = self.get(&key);
        if existing.is_some() {
            let mut map = self.writes.write().unwrap();
            map.insert(key, Write::InsertPreexisting(StorageValue::Empty, Arc::new(AtomicBool::new(false))));
        } else {
            self.insert(key)
        }
    }

    pub fn put_val(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>, value: StorageValue) {
        let existing = self.get(&key);
        if existing.is_some() {
            let mut map = self.writes.write().unwrap();
            map.insert(key, Write::InsertPreexisting(value, Arc::new(AtomicBool::new(false))));
        } else {
            self.insert_val(key, value);
        }
    }

    /// Insert a delete marker for the key with a new version
    pub fn delete(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>) {
        let mut map = self.writes.write().unwrap();
        if map.get(&key).map_or(false, |write| write.is_insert()) {
            map.remove(&key);
        } else {
            map.insert(key, Write::Delete);
        }
    }

    /// Get a Value, and mark it as a required key
    pub fn get_required(&self, key: StorageKey<SNAPSHOT_INLINE_KEYS>) -> StorageValue {
        let map = self.writes.read().unwrap();
        let existing = map.get(&key);
        if existing.is_none() {
            let storage_value = self.storage.get_direct(&key);
            if storage_value.is_some() {
                let mut map = self.writes.write().unwrap();
                map.insert(key, Write::RequireExists(storage_value.as_ref().unwrap().clone()));
                return storage_value.unwrap();
            } else {
                unreachable!("Require key exists in snapshot or in storage.");
            }
        } else {
            let write = existing.unwrap();
            match write {
                Write::Insert(value) => value.clone(), // exists
                Write::InsertPreexisting(value, _) => value.clone(), // exists
                Write::RequireExists(value) => value.clone(), // cached
                Write::Delete => unreachable!("Require key has been marked for deletion"),
            }
        }
    }

    /// Get the Value for the key, returning an empty Option if it does not exist
    pub fn get(&self, key: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> Option<StorageValue> {
        let map = self.writes.read().unwrap();
        let write = map.get(key);
        write.map_or_else(
            || self.storage.get_direct(key),
            |write| match write {
                Write::Insert(value) => Some(value.clone()),
                Write::InsertPreexisting(value, _) => Some(value.clone()),
                Write::RequireExists(value) => Some(value.clone()),
                Write::Delete => None,
            },
        )
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> impl Iterator<Item=(Box<[u8]>, StorageValue)> + 'snapshot {
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

    fn iterate_prefix_buffered<'s>(&'s self, prefix: &StorageKey<SNAPSHOT_INLINE_KEYS>) -> impl Iterator<Item=(Box<[u8]>, Write)> + 's {
        let map = self.writes.read().unwrap();
        let range = RangeFrom { start: prefix };
        // TODO: hold read lock while iterating so avoid collecting into array
        map.range::<StorageKey<SNAPSHOT_INLINE_KEYS>, _>(range).map(|(key, val)| {
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

pub(crate) const SNAPSHOT_INLINE_KEYS: usize = 48;

pub(crate) type WriteData = BTreeMap<StorageKey<SNAPSHOT_INLINE_KEYS>, Write>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Write {
    // Insert KeyValue with a new version. Never conflicts.
    Insert(StorageValue),
    // Insert KeyValue with new version if a concurrent Txn deletes Key. Boolean indicates requires re-insertion. Never conflicts.
    InsertPreexisting(StorageValue, Arc<AtomicBool>),
    // TODO what happens during replay
    // Mark Key as required from storage. Caches existing storage Value. Conflicts with Delete.
    RequireExists(StorageValue),
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