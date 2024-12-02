/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) use keyspace::{Keyspace, KeyspaceCheckpointError, KeyspaceError, Keyspaces, KEYSPACE_MAXIMUM_COUNT};
pub use keyspace::{KeyspaceDeleteError, KeyspaceId, KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError};
use rocksdb::DBRawIterator;

use crate::snapshot::pool::{PoolRecycleGuard, Poolable, SinglePool};

pub mod iterator;
mod keyspace;
mod raw_iterator;

impl<'a> Poolable for DBRawIterator<'a> {}

pub(crate) struct IteratorPool<'snapshot> {
    pools_per_keyspace: [SinglePool<DBRawIterator<'snapshot>>; KEYSPACE_MAXIMUM_COUNT],
}
impl<'snapshot> IteratorPool<'snapshot> {
    pub(crate) fn new() -> Self {
        let pools_per_keyspace = std::array::from_fn(|i| {
            SinglePool::new()
            // let pool: fn(&Keyspace) -> DBRawIterator<'a> = SinglePool::new(Self::create_iterator);
            // pool
        });
        Self { pools_per_keyspace }
    }
    fn get_iterator(&self, keyspace: &'snapshot Keyspace) -> PoolRecycleGuard<'_, DBRawIterator<'snapshot>> {
        self.pools_per_keyspace[keyspace.id().0 as usize].get_or_create(|| {
            keyspace.kv_storage.raw_iterator_opt(keyspace.new_read_options()) // It is safe to read later RocksDB snapshots since our MVCC will
        })
    }
}
