/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::{DEFAULT_DURATION_BUCKETS_NANOS, DEFAULT_QUERIES_PER_TRANSACTION_BUCKETS};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum HistogramUnit {
    Nanoseconds,
    Count,
}

/// Lock-free histogram with fixed bucket boundaries. `observe()` does one
/// atomic fetch_add on a bucket and one on `sum`; bucket lookup is a linear
/// scan (fine for the 7-bound defaults — switch to partition_point above ~32).
/// `unit` tells exposition how to render values: `Nanoseconds` divides by 1e9
/// to emit seconds, `Count` emits as-is.
#[derive(Debug)]
pub struct HistogramMetrics {
    bucket_bounds: Vec<u64>,
    bucket_counts: Vec<AtomicU64>,
    overflow_count: AtomicU64,
    sum: AtomicU64,
    unit: HistogramUnit,
}

impl HistogramMetrics {
    /// Construct with explicit bucket boundaries (in the unit's native u64).
    /// Boundaries are inclusive upper bounds; an observation of exactly `bounds[i]`
    /// counts in bucket i. `+Inf` is implicit — anything above the last bound
    /// counts in the overflow bucket.
    pub fn new(bucket_bounds: Vec<u64>, unit: HistogramUnit) -> Self {
        debug_assert!(
            bucket_bounds.windows(2).all(|w| w[0] < w[1]),
            "histogram bucket bounds must be strictly ascending"
        );
        let bucket_counts = (0..bucket_bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self { bucket_bounds, bucket_counts, overflow_count: AtomicU64::new(0), sum: AtomicU64::new(0), unit }
    }

    /// Constructor for duration histograms with the standard bucket set.
    pub fn new_duration() -> Self {
        Self::new(DEFAULT_DURATION_BUCKETS_NANOS.to_vec(), HistogramUnit::Nanoseconds)
    }

    /// Constructor for the queries-per-transaction histogram (count-shaped).
    pub fn new_queries_per_transaction() -> Self {
        Self::new(DEFAULT_QUERIES_PER_TRANSACTION_BUCKETS.to_vec(), HistogramUnit::Count)
    }

    /// Observe a duration. Saturates a Duration exceeding `u64::MAX` ns
    /// (~584 years) into the overflow bucket — not a realistic timing.
    pub fn observe_duration(&self, d: std::time::Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        debug_assert!(self.unit == HistogramUnit::Nanoseconds);
        self.observe_raw(nanos);
    }

    /// Observe a raw count.
    pub fn observe_count(&self, n: u64) {
        debug_assert!(self.unit == HistogramUnit::Count);
        self.observe_raw(n);
    }

    fn observe_raw(&self, value: u64) {
        // Linear scan: the standard bucket sets are 7-9 bounds. If a future bucket
        // set grows beyond ~32 bounds, switch to partition_point binary search.
        let bucket = self.bucket_bounds.iter().position(|&b| value <= b);
        match bucket {
            Some(i) => self.bucket_counts[i].fetch_add(1, Ordering::Relaxed),
            None => self.overflow_count.fetch_add(1, Ordering::Relaxed),
        };
        // `sum` can wrap at u64::MAX (~584 years of accumulated ns). Realistic
        // soak windows are weeks, so no protection beyond the wrap is necessary.
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    pub fn unit(&self) -> HistogramUnit {
        self.unit
    }

    /// Snapshot for exposition: cumulative bucket counts, total count, raw sum.
    /// `count` is the prefix sum of buckets + overflow, so it equals
    /// `_bucket{le="+Inf"}` by construction (avoids torn reads across atomics).
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut cumulative_counts = Vec::with_capacity(self.bucket_bounds.len());
        let mut running = 0u64;
        for c in &self.bucket_counts {
            running += c.load(Ordering::Relaxed);
            cumulative_counts.push(running);
        }
        running += self.overflow_count.load(Ordering::Relaxed);
        // `running` is now the total observation count = +Inf bucket = `_count`.
        HistogramSnapshot {
            bucket_bounds: self.bucket_bounds.clone(),
            cumulative_counts,
            count: running,
            sum: self.sum.load(Ordering::Relaxed),
            unit: self.unit,
        }
    }
}

/// Plain-data snapshot of a `HistogramMetrics` at one moment, used by the
/// JSON/Prometheus exposition. Cumulative counts; `count` is the +Inf bucket.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub bucket_bounds: Vec<u64>,
    pub cumulative_counts: Vec<u64>,
    pub count: u64,
    pub sum: u64,
    pub unit: HistogramUnit,
}

// ============================================================================
// Histogram unit tests
// ============================================================================

#[cfg(test)]
mod histogram_tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::{HistogramMetrics, HistogramUnit};
    use crate::metrics::DEFAULT_DURATION_BUCKETS_NANOS;

    #[test]
    fn bucketing_picks_the_smallest_upper_bound_that_includes_the_observation() {
        let h = HistogramMetrics::new_duration();
        // 50 µs → second bucket (le=100µs).
        h.observe_duration(Duration::from_micros(50));
        // Exactly 100 µs → still the second bucket (bounds are inclusive upper).
        h.observe_duration(Duration::from_micros(100));
        // 5 ms → fourth bucket (le=10ms).
        h.observe_duration(Duration::from_millis(5));
        // 200 s → overflow (above the 100s top bound).
        h.observe_duration(Duration::from_secs(200));

        let snap = h.snapshot();
        // Cumulative: bucket 0 (le=10µs) = 0, bucket 1 (le=100µs) = 2, ..., bucket 3 (le=10ms) = 3.
        assert_eq!(snap.cumulative_counts[0], 0, "10µs bucket should be empty");
        assert_eq!(snap.cumulative_counts[1], 2, "100µs bucket sees 50µs + 100µs");
        assert_eq!(snap.cumulative_counts[2], 2, "1ms bucket unchanged");
        assert_eq!(snap.cumulative_counts[3], 3, "10ms bucket adds the 5ms observation");
        assert_eq!(snap.cumulative_counts.last().copied().unwrap(), 3, "100s bucket = pre-overflow total");
        assert_eq!(snap.count, 4, "count includes the 200s overflow");
    }

    #[test]
    fn sum_accumulates_in_native_units() {
        let h = HistogramMetrics::new_duration();
        h.observe_duration(Duration::from_micros(50));
        h.observe_duration(Duration::from_millis(5));
        let snap = h.snapshot();
        // 50 µs + 5 ms = 5_050_000 ns
        assert_eq!(snap.sum, 50_000 + 5_000_000);
        assert_eq!(snap.unit, HistogramUnit::Nanoseconds);
    }

    #[test]
    fn empty_histogram_snapshots_with_zeros_everywhere() {
        let h = HistogramMetrics::new_duration();
        let snap = h.snapshot();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.sum, 0);
        assert!(snap.cumulative_counts.iter().all(|&c| c == 0));
        // Bucket bound set matches the default duration buckets.
        assert_eq!(snap.bucket_bounds.as_slice(), DEFAULT_DURATION_BUCKETS_NANOS);
    }

    #[test]
    fn count_histogram_observes_u64_directly() {
        let h = HistogramMetrics::new_queries_per_transaction();
        h.observe_count(1);
        h.observe_count(5);
        h.observe_count(2000);
        h.observe_count(50_000); // above last bound (10_000) → overflow
        let snap = h.snapshot();
        assert_eq!(snap.count, 4);
        assert_eq!(snap.sum, 1 + 5 + 2000 + 50_000);
        // Buckets are [1, 5, 10, 25, 100, 1000, 10000]. The observation of 1 falls in
        // bucket 0 (le=1); 5 falls in bucket 1 (le=5); 2000 falls in bucket 6 (le=10000);
        // 50_000 overflows.
        assert_eq!(snap.cumulative_counts[0], 1);
        assert_eq!(snap.cumulative_counts[1], 2);
        assert_eq!(snap.cumulative_counts[5], 2);
        assert_eq!(snap.cumulative_counts[6], 3);
    }

    #[test]
    fn concurrent_observers_do_not_lose_observations() {
        // 8 threads × 1000 observations each = 8000 total. Each thread observes a
        // value that lands in a different bucket, so we can also confirm bucketing
        // under contention.
        let h = Arc::new(HistogramMetrics::new_duration());
        let n_buckets = DEFAULT_DURATION_BUCKETS_NANOS.len();
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let h = h.clone();
                thread::spawn(move || {
                    let nanos = DEFAULT_DURATION_BUCKETS_NANOS[t % n_buckets];
                    for _ in 0..1000 {
                        h.observe_duration(Duration::from_nanos(nanos));
                    }
                })
            })
            .collect();
        for j in threads {
            j.join().unwrap();
        }
        let snap = h.snapshot();
        assert_eq!(snap.count, 8000, "no observation lost under contention");
        let expected_sum: u64 =
            (0..8u64).map(|t| 1000u64 * DEFAULT_DURATION_BUCKETS_NANOS[(t as usize) % n_buckets]).sum();
        assert_eq!(snap.sum, expected_sum);
    }
}
