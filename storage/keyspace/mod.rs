/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) use keyspace::{Keyspace, KeyspaceCheckpointError, KeyspaceError, Keyspaces, KEYSPACE_MAXIMUM_COUNT};
pub use keyspace::{KeyspaceDeleteError, KeyspaceId, KeyspaceOpenError, KeyspaceSet, KeyspaceValidationError};
use rocksdb::{DBRawIterator, DB};

use crate::snapshot::pool::{PoolRecycleGuard, Poolable, SinglePool};

mod constants;
pub mod iterator;
mod keyspace;
mod raw_iterator;

impl Poolable for DBRawIterator<'static> {}

#[derive(Default)]
pub struct IteratorPool {
    unprefixed_iterators_per_keyspace: [SinglePool<DBRawIterator<'static>>; KEYSPACE_MAXIMUM_COUNT],
    prefixed_iterators_per_keyspace: [SinglePool<DBRawIterator<'static>>; KEYSPACE_MAXIMUM_COUNT],
}

impl IteratorPool {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_iterator_unprefixed(&self, keyspace: &Keyspace) -> PoolRecycleGuard<DBRawIterator<'static>> {
        self.unprefixed_iterators_per_keyspace[keyspace.id().0 as usize].get_or_create(|| {
            let kv_storage: &'static DB = unsafe { std::mem::transmute(&keyspace.kv_storage) };
            kv_storage.raw_iterator_opt(keyspace.new_read_options())
        })
    }

    fn get_iterator_prefixed(&self, keyspace: &Keyspace) -> PoolRecycleGuard<DBRawIterator<'static>> {
        self.prefixed_iterators_per_keyspace[keyspace.id().0 as usize].get_or_create(|| {
            let kv_storage: &'static DB = unsafe { std::mem::transmute(&keyspace.kv_storage) };
            let mut read_options = keyspace.new_read_options();
            read_options.set_prefix_same_as_start(true);
            read_options.set_total_order_seek(false);
            kv_storage.raw_iterator_opt(read_options)
        })
    }
}
