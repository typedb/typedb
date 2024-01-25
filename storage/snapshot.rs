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

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::RangeFrom;
use std::sync::{Mutex, RwLock};

use durability::SequenceNumber;
use itertools::{EitherOrBoth, Itertools};

use crate::error::StorageError;
use crate::isolation_manager::CommitRecord;
use crate::key_value::{Key, Value};
use crate::Storage;

pub enum Snapshot<'storage> {
    Read(ReadSnapshot<'storage>),
    Write(WriteSnapshot<'storage>),
}

impl<'storage> Snapshot<'storage> {
    pub fn get<'snapshot>(&'snapshot self, key: &Key) -> Option<Value> {
        match self {
            Snapshot::Read(snapshot) => snapshot.get(key),
            Snapshot::Write(snapshot) => snapshot.get(key),
        }
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &Key) -> Box<dyn Iterator<Item=(Box<[u8]>, Value)> + 'snapshot> {
        match self {
            Snapshot::Read(snapshot) => Box::new(snapshot.iterate_prefix(prefix)),
            Snapshot::Write(snapshot) => Box::new(snapshot.iterate_prefix(prefix)),
        }
    }
}

pub struct ReadSnapshot<'storage> {
    storage: &'storage Storage,
    open_sequence_number: SequenceNumber,
}

impl<'storage> ReadSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage Storage, open_sequence_number: SequenceNumber) -> ReadSnapshot {
        // Note: for serialisability, we would need to register the open transaction to the IsolationManager
        ReadSnapshot {
            storage: storage,
            open_sequence_number: open_sequence_number,
        }
    }

    fn get<'snapshot>(&self, key: &Key) -> Option<Value> {
        self.storage.get(key)
    }

    fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Value)> + 'snapshot {
        self.storage.iterate_prefix(prefix)
    }
}

pub struct WriteSnapshot<'storage> {
    storage: &'storage Storage,
    // TODO: replace with BTree Left-Right structure to allow concurrent read/write
    writes: RwLock<WriteData>,
    modifications: Mutex<ModifyData>,
    open_sequence_number: SequenceNumber,
}

impl<'storage> WriteSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage Storage, open_sequence_number: SequenceNumber) -> WriteSnapshot {
        storage.isolation_manager.notify_open(&open_sequence_number);
        WriteSnapshot {
            storage: storage,
            writes: RwLock::new(WriteData::new()),
            modifications: Mutex::new(ModifyData::new()),
            open_sequence_number: open_sequence_number,
        }
    }

    pub fn put(&self, key: Key) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, Write::Insert(Value::Empty));
    }

    pub fn put_val(&self, key: Key, value: Value) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, Write::Insert(value));
    }

    pub fn delete(&self, key: Key) {
        let mut map = self.writes.write().unwrap();
        if map.get(&key).map_or(false, |write| write.is_insert()) {
            map.remove(&key);
        } else {
            map.insert(key, Write::Delete);
        }
    }

    pub fn get(&self, key: &Key) -> Option<Value> {
        let map = self.writes.read().unwrap();
        let write = map.get(key);
        write.map_or_else(
            || self.storage.get(key),
            |write| match write {
                // TODO: don't want to require clone
                Write::Insert(value) => Some(value.clone()),
                Write::Delete => None,
            },
        )
    }

    pub fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Value)> + 'snapshot {
        let storage_iterator = self.storage.iterate_prefix(prefix);
        let buffered_iterator = self.iterate_prefix_buffered(prefix);
        storage_iterator.merge_join_by(
            buffered_iterator,
            |(k1, v1), (k2, v2)| k1.cmp(k2),
        ).filter_map(|ordering| match ordering {
            EitherOrBoth::Both((k1, v1), (k2, write2)) => match write2 {
                Write::Insert(v2) => Some((k2, v2)),
                Write::Delete => None,
            },
            EitherOrBoth::Left((k1, v1)) => Some((k1, v1)),
            EitherOrBoth::Right((k2, write2)) => match write2 {
                Write::Insert(v2) => Some((k2, v2)),
                Write::Delete => None,
            },
        })
    }

    fn iterate_prefix_buffered<'s>(&'s self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Write)> + 's {
        let map = self.writes.read().unwrap();
        let range = RangeFrom { start: prefix };
        // TODO: hold read lock while iterating so avoid collecting into array
        map.range::<Key, _>(range).map(|(key, val)| {
            // TODO: we can avoid allocation here once we settle on a Key/Value struct
            (key.bytes().into(), val.clone())
        }).collect::<Vec<_>>().into_iter()
    }

    pub fn commit(self) {
        self.storage.snapshot_commit(self);
    }

    pub(crate) fn into_commit_record(self) -> CommitRecord {
        CommitRecord::new(
            self.writes.into_inner().unwrap(), self.modifications.into_inner().unwrap(),
            self.open_sequence_number,
        )
    }
}


pub(crate) type WriteData = BTreeMap<Key, Write>;
pub(crate) type ModifyData = BTreeSet<Key>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Write {
    Insert(Value),
    Delete,
}

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
    FailedGet { source: StorageError },
    FailedPut { source: StorageError },
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