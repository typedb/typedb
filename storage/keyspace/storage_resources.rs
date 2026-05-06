/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use rocksdb::{Cache, WriteBufferManager};
use std::fmt::{Debug, Formatter};

pub struct RocksResources {
    cache: Cache,
    write_buffer_manager: WriteBufferManager,

    cache_limit_bytes: usize,
    write_buffers_limit_bytes: usize,
}

impl RocksResources {
    /// Note: cache limit is a _soft_ limit
    /// it is possible to exceed it with pinned index and filter blocks
    pub fn new(cache_limit_bytes: usize, write_buffers_limit_bytes: usize) -> Self {
        let cache = Cache::new_lru_cache(cache_limit_bytes);
        // `allow_stall=true` lets RocksDB pause writes when the buffer manager
        // is over budget, instead of OOM'ing the process.
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager(write_buffers_limit_bytes, true);
        Self { cache, write_buffer_manager, cache_limit_bytes, write_buffers_limit_bytes }
    }

    pub fn cache(&self) -> Cache {
        self.cache.clone()
    }

    pub fn write_buffer_manager(&self) -> WriteBufferManager {
        self.write_buffer_manager.clone()
    }

    pub fn cache_limit_bytes(&self) -> usize {
        self.cache_limit_bytes
    }

    pub fn write_buffers_limit_bytes(&self) -> usize {
        self.write_buffers_limit_bytes
    }
}

impl Debug for RocksResources {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rocks LRU Cache size (shared by all databases): {}. \
             Rocks write buffers size limit (shared by all databases): {}",
            self.cache_limit_bytes, self.write_buffers_limit_bytes
        )
    }
}
