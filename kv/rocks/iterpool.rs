/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use rocksdb::{DBRawIterator, DB};
use crate::rocks::pool::{PoolRecycleGuard, Poolable, LIFOPool};
use crate::rocks::RocksKVStore;

impl Poolable for DBRawIterator<'static> {}

#[derive(Default)]
pub struct RocksRawIteratorPool {
    unprefixed_iterators: LIFOPool<DBRawIterator<'static>>,
    prefixed_iterator: LIFOPool<DBRawIterator<'static>>,
}

impl RocksRawIteratorPool {
    pub fn new() -> Self {
        Self {
            // force never pooling by capping at size 0
            unprefixed_iterators: LIFOPool::new_capped(0),
            prefixed_iterator: LIFOPool::new_capped(0),
        }
    }

    pub(super) fn get_iterator_unprefixed(&self, rocks_store: &RocksKVStore) -> PoolRecycleGuard<DBRawIterator<'static>> {
        let iterator = self.unprefixed_iterators.get_or_create(|| {
            let kv_storage: &'static DB = unsafe { std::mem::transmute(&rocks_store.rocks) };
            kv_storage.raw_iterator_opt(rocks_store.default_read_options())
        });

        // TODO: our rocks bindings dont have refresh()
        // if let Err(err) = iterator.refresh();

        iterator
    }

    pub(super) fn get_iterator_prefixed(&self, rocks_store: &RocksKVStore) -> PoolRecycleGuard<DBRawIterator<'static>> {
        let iterator = self.prefixed_iterator.get_or_create(|| {
            let kv_storage: &'static DB = unsafe { std::mem::transmute(&rocks_store.rocks) };
            kv_storage.raw_iterator_opt(rocks_store.bloom_read_options())
        });
        // TODO: our rocks bindings dont have refresh()
        // if let Err(err) = iterator.refresh();

        iterator
    }
}
