/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::metrics::HistogramMetrics;

#[derive(Debug, Clone)]
pub struct FsyncMetrics {
    fsync_histogram: Arc<HistogramMetrics>,
    bytes_counter: Arc<AtomicU64>,
}

impl FsyncMetrics {
    pub fn new() -> Self {
        Self { fsync_histogram: Arc::new(HistogramMetrics::new_duration()), bytes_counter: Arc::new(AtomicU64::new(0)) }
    }

    /// Alias for new(). Documents intent at call sites where the FsyncMetrics
    /// is standalone (tests, benches, internal DBs, disabled-collection) and
    /// nobody reads the storage it allocates.
    pub fn noop() -> Self {
        Self::new()
    }

    pub fn record_fsync_duration(&self, duration: Duration) {
        self.fsync_histogram.observe_duration(duration);
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_counter.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn fsync_histogram_snapshot(&self) -> crate::metrics::HistogramSnapshot {
        self.fsync_histogram.snapshot()
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_counter.load(Ordering::Relaxed)
    }
}
