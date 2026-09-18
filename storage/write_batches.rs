/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    iter,
    ops::{Deref, DerefMut},
    sync::atomic::Ordering,
};

use rocksdb::WriteBatch;

use super::{MVCCKey, StorageOperation};
use crate::{
    CommitObserver,
    keyspace::KEYSPACE_MAXIMUM_COUNT,
    sequence_number::SequenceNumber,
    snapshot::{buffer::OperationsBuffer, write::Write},
};

pub(crate) struct WriteBatches {
    pub(crate) batches: [Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT],
}

impl WriteBatches {
    pub(crate) fn from_operations(
        seq: SequenceNumber,
        operations: &OperationsBuffer,
        commit_observer: Option<&dyn CommitObserver>,
    ) -> Self {
        let mut write_batches = Self::default();

        for (index, buffer) in operations.write_buffers().enumerate() {
            let writes = buffer.writes();
            if !writes.is_empty() {
                let keyspace_id = buffer.keyspace_id;
                let write_batch = write_batches[index].insert(WriteBatch::default());
                for (key, write) in writes {
                    // externally-owned values (e.g. vectors) are not materialised in the KV store:
                    // the key is written with an empty value; the value lives with the CommitObserver
                    let value_owned_externally =
                        commit_observer.is_some_and(|observer| observer.owns_value(keyspace_id, key));
                    match write {
                        Write::Insert { value } => {
                            let value: &[u8] = if value_owned_externally { &[] } else { value };
                            write_batch.put(MVCCKey::build(key, seq, StorageOperation::Insert).bytes(), value)
                        }
                        Write::Put { value, reinsert, .. } => {
                            if reinsert.load(Ordering::SeqCst) {
                                let value: &[u8] = if value_owned_externally { &[] } else { value };
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

impl IntoIterator for WriteBatches {
    type Item = (usize, WriteBatch);
    type IntoIter = iter::FilterMap<
        iter::Enumerate<<[Option<WriteBatch>; KEYSPACE_MAXIMUM_COUNT] as IntoIterator>::IntoIter>,
        fn((usize, Option<WriteBatch>)) -> Option<(usize, WriteBatch)>,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter().enumerate().filter_map(|(index, batch)| Some((index, batch?)))
    }
}

impl Deref for WriteBatches {
    type Target = [Option<WriteBatch>];
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
