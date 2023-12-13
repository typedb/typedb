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
 *
 */

use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use wal::SequenceNumber;

use crate::error::StorageError;
use crate::key::Key;
use crate::Storage;

pub enum Snapshot<'storage> {
    Read(ReadSnapshot<'storage>),
    Write(WriteSnapshot<'storage>),
}

pub struct ReadSnapshot<'storage> {
    storage: &'storage Storage,
}

pub struct WriteSnapshot<'storage> {
    storage: &'storage Storage,
    inserts: BTreeMap<Vec<u8>, Vec<u8>>,
    // TODO: replace with BTree Left-Right structure to allow concurrent read/write
    open_sequence_number: SequenceNumber,
}

impl<'storage> WriteSnapshot<'storage> {
    pub(crate) fn new(storage: &'storage Storage, open_sequence_number: SequenceNumber) -> WriteSnapshot {
        WriteSnapshot {
            storage: storage,
            inserts: BTreeMap::new(),
            open_sequence_number: open_sequence_number,
        }
    }

    fn put(&mut self, key: &Key) {
        let _ = self.inserts.insert(key.data.clone(), Storage::BYTES_EMPTY_VEC);
    }

    fn get(&self, key: &Key) -> Option<Vec<u8>> {
        // TODO merge
        self.storage.get(key)
    }

    fn iterate_prefix<'s>(&'s self, prefix: &Vec<u8>) -> impl Iterator<Item=(Box<[u8]>, Box<[u8]>)> + 's {
        self.storage.iterate_prefix(prefix).merge(self.iterate_prefix_buffered(prefix))
    }

    fn iterate_prefix_buffered<'s>(&'s self, prefix: &Vec<u8>) -> impl Iterator<Item=(Box<[u8]>, Box<[u8]>)> + 's {
        self.inserts.range::<Vec<u8>, _>(prefix..).map(|(key, val)| {
            // TODO: we can avoid allocation here once we settle on a Key/Value struct
            (key.clone().into_boxed_slice(), val.clone().into_boxed_slice())
        })
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