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
    pub fn new(fsync_histogram: Arc<HistogramMetrics>, bytes_counter: Arc<AtomicU64>) -> Self {
        Self { fsync_histogram, bytes_counter }
    }

    /// Construct a no-op instance for tests, benches, internal databases,
    /// and the disabled-collection case.
    pub fn noop() -> Self {
        Self { fsync_histogram: Arc::new(HistogramMetrics::new_duration()), bytes_counter: Arc::new(AtomicU64::new(0)) }
    }

    pub fn record_fsync_duration(&self, duration: Duration) {
        self.fsync_histogram.observe_duration(duration);
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_counter.fetch_add(bytes, Ordering::Relaxed);
    }
}
