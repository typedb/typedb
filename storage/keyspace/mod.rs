/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) use keyspace::{Keyspace, KeyspaceCheckpointError, KeyspaceError, Keyspaces, KEYSPACE_MAXIMUM_COUNT};
pub use keyspace::{KeyspaceDeleteError, KeyspaceId, KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError};
use rocksdb::{DB, DBRawIterator};

use crate::snapshot::pool::{PoolRecycleGuard, Poolable, SinglePool};

pub mod iterator;
mod keyspace;
mod raw_iterator;

impl Poolable for DBRawIterator<'static> {}

pub struct IteratorPool {
    pools_per_keyspace: [SinglePool<DBRawIterator<'static>>; KEYSPACE_MAXIMUM_COUNT],
}
impl IteratorPool {
    pub fn new() -> Self {
        let pools_per_keyspace = std::array::from_fn(|i| {
            SinglePool::new()
        });
        Self { pools_per_keyspace }
    }
    fn get_iterator(&self, keyspace: &Keyspace) -> PoolRecycleGuard<DBRawIterator<'static>> {
        self.pools_per_keyspace[keyspace.id().0 as usize].get_or_create(|| {
            let kv_storage: &'static DB = unsafe { std::mem::transmute(&keyspace.kv_storage) };
            kv_storage.raw_iterator_opt(keyspace.new_read_options()) // It is safe to read later RocksDB snapshots since our MVCC will
        })
    }
}
