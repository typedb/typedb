/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;

use options::byte_size::ByteSize;
use rocksdb::{Cache, WriteBufferManager};

pub struct RocksResources {
    cache: Cache,
    write_buffer_manager: WriteBufferManager,

    cache_limit: ByteSize,
    write_buffers_limit: ByteSize,
}

impl RocksResources {
    /// Note: cache limit is a _soft_ limit
    /// it is possible to exceed it with pinned index and filter blocks
    pub fn new(cache_limit: ByteSize, write_buffers_limit: ByteSize) -> Self {
        let cache = Cache::new_lru_cache(cache_limit.as_usize());
        // `allow_stall=true` lets RocksDB pause writes when the buffer manager
        // is over budget, instead of OOM'ing the process.
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager(write_buffers_limit.as_usize(), true);
        Self { cache, write_buffer_manager, cache_limit, write_buffers_limit }
    }

    pub fn cache(&self) -> Cache {
        self.cache.clone()
    }

    pub fn write_buffer_manager(&self) -> WriteBufferManager {
        self.write_buffer_manager.clone()
    }

    pub fn cache_limit(&self) -> ByteSize {
        self.cache_limit
    }

    pub fn write_buffers_limit(&self) -> ByteSize {
        self.write_buffers_limit
    }
}

impl fmt::Debug for RocksResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Rocks LRU Cache size (shared by all databases): {}. \n
             Rocks write buffers size limit (shared by all databases): {}.",
            self.cache_limit, self.write_buffers_limit
        )
    }
}
