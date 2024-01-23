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
use std::sync::RwLock;

use itertools::Itertools;
use wal::SequenceNumber;

use crate::Storage;
use crate::error::StorageError;
use crate::key_value::{Key, Value};

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
    writes: RwLock<BTreeMap<Key, Value>>,
    open_sequence_number: SequenceNumber,
}

impl<'storage> WriteSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage Storage, open_sequence_number: SequenceNumber) -> WriteSnapshot {
        WriteSnapshot {
            storage: storage,
            writes: RwLock::new(BTreeMap::new()),
            open_sequence_number: open_sequence_number,
        }
    }

    pub fn put(&self, key: Key) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, Value::Empty);
    }

    pub fn put_val(&self, key: Key, value: Value) {
        let mut map = self.writes.write().unwrap();
        map.insert(key, value);
    }

    fn get(&self, key: &Key) -> Option<Value> {
        let map = self.writes.read().unwrap();
        // let write = map.get(key);
        //
        // if write.is_some() {
        //
        // } else {
            self.storage.get(key)
        // }
        // TODO merge with inserts & deletes
    }

    fn iterate_prefix<'snapshot>(&'snapshot self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Value)> + 'snapshot {
        // TODO merge with inserts & deletes
        self.storage.iterate_prefix(prefix).merge_by(
            self.iterate_prefix_buffered(prefix),
            |(k1, v1), (k2, v2)| k1.cmp(k2).is_le()
        )
    }

    fn iterate_prefix_buffered<'s>(&'s self, prefix: &Key) -> impl Iterator<Item=(Box<[u8]>, Value)> + 's {
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

    pub(crate) fn into_data(self) -> (BTreeMap<Key, Value>, SequenceNumber) {
        (self.writes.into_inner().unwrap(), self.open_sequence_number)
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