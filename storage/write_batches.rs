/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    iter,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::Ordering,
};

use kv::{KVStore, KVWriteBatch};

use super::{MVCCKey, StorageOperation};
use crate::{
    keyspace::KEYSPACE_MAXIMUM_COUNT,
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write},
};

pub(crate) struct WriteBatches<KV: KVStore> {
    pub(crate) batches: [Option<KV::WriteBatch>; KEYSPACE_MAXIMUM_COUNT],
    _phantom: PhantomData<KV>,
}

impl<KV: KVStore> WriteBatches<KV> {
    pub(crate) fn from_operations(seq: SequenceNumber, operations: &OperationsBuffer) -> Self {
        let mut write_batches = Self::default();

        for (index, buffer) in operations.write_buffers().enumerate() {
            let writes = buffer.writes();
            if !writes.is_empty() {
                let write_batch = write_batches.batches[index].insert(KV::WriteBatch::default());
                for (key, write) in writes {
                    match write {
                        Write::Insert { value } => {
                            write_batch.put(MVCCKey::build(key, seq, StorageOperation::Insert).bytes(), value)
                        }
                        Write::Put { value, reinsert, .. } => {
                            if reinsert.load(Ordering::SeqCst) {
                                write_batch.put(MVCCKey::build(key, seq, StorageOperation::Insert).bytes(), value)
                            }
                        }
                        Write::Delete => {
                            write_batch.put(MVCCKey::build(key, seq, StorageOperation::Delete).bytes(), [])
                        }
                    }
                }
            }
        }
        write_batches
    }
}

impl<KV: KVStore> IntoIterator for WriteBatches<KV> {
    type Item = (usize, KV::WriteBatch);
    type IntoIter = iter::FilterMap<
        iter::Enumerate<<[Option<KV::WriteBatch>; KEYSPACE_MAXIMUM_COUNT] as IntoIterator>::IntoIter>,
        fn((usize, Option<KV::WriteBatch>)) -> Option<(usize, KV::WriteBatch)>,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter().enumerate().filter_map(|(index, batch)| Some((index, batch?)))
    }
}

impl<KV: KVStore> Deref for WriteBatches<KV> {
    type Target = [Option<KV::WriteBatch>];
    fn deref(&self) -> &Self::Target {
        &self.batches
    }
}

impl<KV: KVStore> DerefMut for WriteBatches<KV> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.batches
    }
}

impl<KV: KVStore> Default for WriteBatches<KV> {
    fn default() -> Self {
        Self { batches: std::array::from_fn(|_| None), _phantom: PhantomData }
    }
}
