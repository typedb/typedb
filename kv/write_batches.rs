/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    iter,
    ops::{Deref, DerefMut},
};

use crate::keyspaces::KEYSPACE_MAXIMUM_COUNT;

pub enum KVWriteBatch {
    RocksDB(rocksdb::WriteBatch),
}

impl KVWriteBatch {
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        match self {
            Self::RocksDB(b) => rocksdb::WriteBatch::put(b, key, value),
        }
    }
}

impl Default for KVWriteBatch {
    fn default() -> Self {
        Self::RocksDB(rocksdb::WriteBatch::default())
    }
}

pub struct WriteBatches {
    pub batches: [Option<KVWriteBatch>; KEYSPACE_MAXIMUM_COUNT],
}

impl IntoIterator for WriteBatches {
    type Item = (usize, KVWriteBatch);
    type IntoIter = iter::FilterMap<
        iter::Enumerate<<[Option<KVWriteBatch>; KEYSPACE_MAXIMUM_COUNT] as IntoIterator>::IntoIter>,
        fn((usize, Option<KVWriteBatch>)) -> Option<(usize, KVWriteBatch)>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter().enumerate().filter_map(|(index, batch)| Some((index, batch?)))
    }
}

impl Deref for WriteBatches {
    type Target = [Option<KVWriteBatch>];
    fn deref(&self) -> &Self::Target {
        &self.batches
    }
}

impl DerefMut for WriteBatches {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.batches
    }
}

impl Default for WriteBatches {
    fn default() -> Self {
        Self { batches: std::array::from_fn(|_| None) }
    }
}
