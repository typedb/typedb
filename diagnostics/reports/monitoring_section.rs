/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub trait MonitoringSection: Send + Sync + std::fmt::Debug {
    /// Stable identifier for this metrics source. Used as the JSON section key
    /// and for de-duplication on registration.
    fn name(&self) -> &str;

    /// Append this source's metrics in Prometheus text format. Conventionally
    /// starts with a blank line and one or more `# HELP` / `# TYPE` blocks.
    fn write_prometheus(&self, out: &mut String);

    /// Return this source's metrics as a JSON object for the monitoring
    /// endpoint. The return type is `Map<String, Value>` (a JSON object).
    /// Default returns an empty object — implementers may opt in by overriding.
    fn write_json(&self) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use serde_json::{Value as JsonValue, json};

    use super::MonitoringSection;
    use crate::{
        Diagnostics,
        metrics::{HistogramMetrics, HistogramSnapshot, HistogramUnit},
    };

    #[derive(Debug)]
    struct CrossCheckedExt {
        invocations: AtomicU64,
        durations: HistogramMetrics,
        external_total_nanos: AtomicU64,
    }

    impl CrossCheckedExt {
        fn new() -> Self {
            Self {
                invocations: AtomicU64::new(0),
                durations: HistogramMetrics::new(
                    vec![1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000],
                    HistogramUnit::Nanoseconds,
                ),
                external_total_nanos: AtomicU64::new(0),
            }
        }

        fn observe(&self, d: Duration) {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
            self.external_total_nanos.fetch_add(nanos, Ordering::Relaxed);
            self.durations.observe_duration(d);
        }

        fn snapshot(&self) -> HistogramSnapshot {
            self.durations.snapshot()
        }
    }

    impl MonitoringSection for CrossCheckedExt {
        fn name(&self) -> &str {
            "cross_checked_ext"
        }
        fn write_prometheus(&self, out: &mut String) {
            let snap = self.snapshot();
            out.push_str("\n# HELP test_observations_total Total observations.\n");
            out.push_str("# TYPE test_observations_total counter\n");
            out.push_str(&format!("test_observations_total {}\n", self.invocations.load(Ordering::Relaxed)));
            out.push_str("# HELP test_duration_seconds Test duration histogram.\n");
            out.push_str("# TYPE test_duration_seconds histogram\n");
            for (i, &bound) in snap.bucket_bounds.iter().enumerate() {
                let le_s = bound as f64 / 1e9;
                out.push_str(&format!(
                    "test_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                    le_s, snap.cumulative_counts[i]
                ));
            }
            out.push_str(&format!("test_duration_seconds_bucket{{le=\"+Inf\"}} {}\n", snap.count));
            out.push_str(&format!("test_duration_seconds_count {}\n", snap.count));
            out.push_str(&format!("test_duration_seconds_sum {}\n", snap.sum as f64 / 1e9));
        }
        fn write_json(&self) -> serde_json::Map<String, JsonValue> {
            let snap = self.snapshot();
            let mut obj = serde_json::Map::new();
            obj.insert("invocations".to_string(), json!(self.invocations.load(Ordering::Relaxed)));
            obj.insert("duration".to_string(), json!({ "count": snap.count, "sum_nanos": snap.sum }));
            obj
        }
    }

    fn fresh_diagnostics() -> Diagnostics {
        Diagnostics::new(
            "deployment_id".to_string(),
            "server_id".to_string(),
            "test".to_string(),
            "0.0.0".to_string(),
            PathBuf::from("/tmp"),
            false,
            true,
        )
    }

    #[test]
    fn register_then_scrape_emits_extension_in_prometheus_and_json() {
        let diag = fresh_diagnostics();
        let ext = Arc::new(CrossCheckedExt::new());
        diag.register_monitoring_extension(ext.clone());
        ext.observe(Duration::from_micros(50));
        ext.observe(Duration::from_millis(5));

        let prom = diag.to_monitoring_prometheus();
        assert!(prom.contains("test_observations_total 2"), "missing counter in: {prom}");
        assert!(prom.contains("test_duration_seconds_bucket"), "missing histogram buckets");
        assert!(prom.contains("test_duration_seconds_count 2"), "missing histogram count");

        let j = diag.to_monitoring_json();
        let ext_obj = &j["extensions"]["cross_checked_ext"];
        assert_eq!(ext_obj["invocations"], json!(2));
        assert_eq!(ext_obj["duration"]["count"], json!(2));
        assert_eq!(ext_obj["duration"]["sum_nanos"], json!(50_000u64 + 5_000_000u64));
    }

    #[test]
    fn register_is_idempotent_on_name_and_has_monitoring_extension_reflects_it() {
        let diag = fresh_diagnostics();
        let ext1 = Arc::new(CrossCheckedExt::new());
        let ext2 = Arc::new(CrossCheckedExt::new());
        ext1.observe(Duration::from_millis(1));
        ext2.observe(Duration::from_millis(2));
        ext2.observe(Duration::from_millis(3));

        assert!(!diag.has_monitoring_extension("cross_checked_ext"));

        diag.register_monitoring_extension(ext1.clone());
        assert!(diag.has_monitoring_extension("cross_checked_ext"));

        diag.register_monitoring_extension(ext2.clone());
        assert!(diag.has_monitoring_extension("cross_checked_ext"));

        let j = diag.to_monitoring_json();
        assert_eq!(j["extensions"]["cross_checked_ext"]["invocations"], json!(2));
    }

    #[test]
    fn extensions_do_not_leak_into_posthog_payload() {
        let diag = fresh_diagnostics();
        let ext = Arc::new(CrossCheckedExt::new());
        ext.observe(Duration::from_millis(7));
        diag.register_monitoring_extension(ext);
        let posthog = diag.to_posthog_reporting_json_against_snapshot("test_api_key");
        let posthog_str = posthog.to_string();
        assert!(!posthog_str.contains("cross_checked_ext"), "extension leaked into PostHog payload: {posthog_str}");
        assert!(
            !posthog_str.contains("test_observations_total"),
            "extension prometheus name leaked into PostHog payload"
        );
    }

    #[test]
    fn concurrent_observers_do_not_lose_samples_and_durations_match_external_total() {
        let diag = Arc::new(fresh_diagnostics());
        let ext = Arc::new(CrossCheckedExt::new());
        diag.register_monitoring_extension(ext.clone());

        const THREADS: usize = 8;
        const PER_THREAD: usize = 2_000;

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let ext = ext.clone();
            handles.push(thread::spawn(move || {
                // Different durations across threads to exercise different buckets.
                let d = Duration::from_nanos(100 + (t as u64 % 7) * 250_000);
                for _ in 0..PER_THREAD {
                    ext.observe(d);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let snap = ext.snapshot();
        let invocations = ext.invocations.load(Ordering::SeqCst);
        let external = ext.external_total_nanos.load(Ordering::SeqCst);

        assert_eq!(snap.count as usize, THREADS * PER_THREAD, "histogram count diverged from expected");
        assert_eq!(invocations as usize, THREADS * PER_THREAD, "explicit counter diverged from expected");
        assert_eq!(snap.sum, external, "histogram sum diverged from external accumulator");

        // Trait-level surface sees the same numbers.
        let j = diag.to_monitoring_json();
        let ext_obj = &j["extensions"]["cross_checked_ext"];
        assert_eq!(ext_obj["invocations"], json!(invocations));
        assert_eq!(ext_obj["duration"]["count"], json!(snap.count));
        assert_eq!(ext_obj["duration"]["sum_nanos"], json!(snap.sum));
    }

    #[test]
    fn measured_durations_match_observation_within_tolerance() {
        let diag = fresh_diagnostics();
        let ext = Arc::new(CrossCheckedExt::new());
        diag.register_monitoring_extension(ext.clone());

        let mut external_sum = Duration::ZERO;
        for _ in 0..10 {
            let start = Instant::now();
            thread::sleep(Duration::from_millis(1));
            let elapsed = start.elapsed();
            external_sum += elapsed;
            ext.observe(elapsed);
        }
        let snap = ext.snapshot();

        assert_eq!(snap.count, 10);
        // Each observation is identical to what we added to external_sum:
        assert_eq!(snap.sum, external_sum.as_nanos() as u64);
        assert!(
            external_sum >= Duration::from_millis(10),
            "external clock did not record the expected minimum sleep total: {:?}",
            external_sum
        );
    }
}
