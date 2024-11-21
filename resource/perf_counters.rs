/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// let's start with a simple and fast global list of NAME = COUNTER for now.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::constants::server::PERF_COUNTERS_ENABLED;

pub struct Counter {
    counter: AtomicU64,
    enabled: bool,
}

impl Counter {
    const fn new(enabled: bool) -> Self {
        Self { counter: AtomicU64::new(0), enabled }
    }

    pub fn increment(&self) {
        if self.enabled {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub static QUERY_CACHE_HITS: Counter = Counter::new(PERF_COUNTERS_ENABLED);
pub static QUERY_CACHE_MISSES: Counter = Counter::new(PERF_COUNTERS_ENABLED);
pub static QUERY_CACHE_FLUSH: Counter = Counter::new(PERF_COUNTERS_ENABLED);
