/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Background refresh scheduler for materialized projections.
//!
//! The [`BackgroundRefresher`] polls the WAL via a [`ChangeDetector`]
//! to detect new commits, then triggers re-materialization based on
//! each projection's [`RefreshPolicy`]:
//!
//! - **Eager** projections are refreshed immediately when new data
//!   commits are detected.
//! - **Periodic** projections are refreshed when their configured
//!   interval has elapsed.
//! - **Manual** projections are skipped (only refreshed via explicit
//!   API call).
//!
//! # Architecture
//!
//! The refresher is designed to be spawned as a tokio task. The
//! [`run_cycle`](BackgroundRefresher::run_cycle) method performs a single
//! poll-and-refresh cycle, while
//! [`run_loop`](BackgroundRefresher::run_loop) runs continuously until
//! a shutdown signal is received.
//!
//! # Versioning
//!
//! Each [`ProjectionRefreshConfig`] tracks its `last_sequence_number`.
//! After a successful refresh, the config is updated with the current
//! watermark. This provides snapshot-isolation-like semantics: each
//! projection's data reflects a consistent point in the WAL timeline.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    change_detector::{ChangeDetector, ChangeSummary},
    materializer::{MaterializationError, Materializer, RefreshResult, SourceQueryExecutor},
    refresh_policy::{ProjectionRefreshConfig, RefreshPolicy},
};

// ── Refresh decision ───────────────────────────────────────────────

/// The decision made for a single projection in a refresh cycle.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshDecision {
    /// Projection was refreshed successfully.
    Refreshed(RefreshResult),
    /// Projection was skipped (manual policy, or no new changes for non-eager).
    Skipped { reason: String },
    /// Refresh was attempted but failed.
    Failed(MaterializationError),
}

/// Result of a complete refresh cycle.
#[derive(Debug, Clone)]
pub struct CycleResult {
    /// Per-projection decisions.
    pub decisions: Vec<(String, RefreshDecision)>,
    /// Change summary from the WAL poll.
    pub changes: ChangeSummary,
}

impl CycleResult {
    pub fn refreshed_count(&self) -> usize {
        self.decisions.iter().filter(|(_, d)| matches!(d, RefreshDecision::Refreshed(_))).count()
    }

    pub fn skipped_count(&self) -> usize {
        self.decisions.iter().filter(|(_, d)| matches!(d, RefreshDecision::Skipped { .. })).count()
    }

    pub fn failed_count(&self) -> usize {
        self.decisions.iter().filter(|(_, d)| matches!(d, RefreshDecision::Failed(_))).count()
    }

    pub fn total(&self) -> usize {
        self.decisions.len()
    }
}

// ── Background refresher ──────────────────────────────────────────

/// Background refresh scheduler.
///
/// Holds the materializer, per-projection configs, and timing state.
/// Designed to be driven by an external loop (or a tokio task via
/// [`run_loop`](Self::run_loop)).
pub struct BackgroundRefresher {
    materializer: Materializer,
    configs: HashMap<String, ProjectionRefreshConfig>,
    /// Tracks when each periodic projection was last refreshed.
    last_refresh_times: HashMap<String, Instant>,
}

impl BackgroundRefresher {
    /// Create a refresher with no projection configs (add them later).
    pub fn new(materializer: Materializer) -> Self {
        Self { materializer, configs: HashMap::new(), last_refresh_times: HashMap::new() }
    }

    /// Reference to the underlying materializer.
    pub fn materializer(&self) -> &Materializer {
        &self.materializer
    }

    /// Number of registered projection configs.
    pub fn config_count(&self) -> usize {
        self.configs.len()
    }

    // ── Config management ──────────────────────────────────────

    /// Register a projection for background refresh.
    pub fn register(&mut self, config: ProjectionRefreshConfig) {
        let key = config.projection_name().to_lowercase();
        self.last_refresh_times.insert(key.clone(), Instant::now());
        self.configs.insert(key, config);
    }

    /// Unregister a projection from background refresh.
    pub fn unregister(&mut self, name: &str) -> Option<ProjectionRefreshConfig> {
        let key = name.to_lowercase();
        self.last_refresh_times.remove(&key);
        self.configs.remove(&key)
    }

    /// Get the config for a projection.
    pub fn get_config(&self, name: &str) -> Option<&ProjectionRefreshConfig> {
        self.configs.get(&name.to_lowercase())
    }

    /// List all registered projection names.
    pub fn registered_projections(&self) -> Vec<String> {
        self.configs.keys().cloned().collect()
    }

    // ── Single cycle ───────────────────────────────────────────

    /// Run a single poll-and-refresh cycle.
    ///
    /// 1. Polls the [`ChangeDetector`] for new WAL commits
    /// 2. Evaluates each projection's [`RefreshPolicy`] against the changes
    /// 3. Triggers re-materialization for eligible projections
    /// 4. Updates sequence numbers and timing state
    pub fn run_cycle(
        &mut self,
        detector: &dyn ChangeDetector,
        executor: &dyn SourceQueryExecutor,
    ) -> Result<CycleResult, String> {
        // 1. Determine the current watermark (safe read point).
        let watermark = detector.current_watermark()?;

        // 2. Find the minimum last_sequence_number across all eager configs
        //    to decide where to start polling from.
        let min_seq = self.configs.values().map(|c| c.last_sequence_number()).min().unwrap_or(0);

        // 3. Poll for changes since that point.
        let events = detector.poll_changes(min_seq)?;
        let summary = ChangeSummary::from_events(events);
        let has_data_changes = summary.data_change_count() > 0;

        // 4. Evaluate each projection.
        let now = Instant::now();
        let mut decisions = Vec::new();
        let config_keys: Vec<String> = self.configs.keys().cloned().collect();

        for key in &config_keys {
            // Clone the config snapshot to avoid borrow conflict when we
            // later need &mut self.configs for record_refresh().
            let config = self.configs.get(key).unwrap().clone();
            let decision = self.decide_refresh(&config, watermark, has_data_changes, now);

            match &decision {
                RefreshDecision::Skipped { .. } => {
                    decisions.push((config.projection_name().to_string(), decision));
                }
                _ => {
                    // Need to attempt refresh.
                    let proj_name = config.projection_name().to_string();
                    let result = self.materializer.refresh_one(&proj_name, executor);
                    let decision = match result {
                        Ok(r) => {
                            // Update bookkeeping.
                            if let Some(cfg) = self.configs.get_mut(key) {
                                cfg.record_refresh(watermark);
                            }
                            self.last_refresh_times.insert(key.clone(), now);
                            RefreshDecision::Refreshed(r)
                        }
                        Err(e) => RefreshDecision::Failed(e),
                    };
                    decisions.push((proj_name, decision));
                }
            }
        }

        Ok(CycleResult { decisions, changes: summary })
    }

    /// Determine whether a projection should be refreshed in this cycle.
    fn decide_refresh(
        &self,
        config: &ProjectionRefreshConfig,
        watermark: u64,
        has_data_changes: bool,
        now: Instant,
    ) -> RefreshDecision {
        match config.policy() {
            RefreshPolicy::Manual => RefreshDecision::Skipped { reason: "manual policy".to_string() },
            RefreshPolicy::Eager => {
                if !config.has_been_refreshed() {
                    // Never refreshed — backfill.
                    RefreshDecision::Refreshed(RefreshResult { projection_name: String::new(), row_count: 0 })
                } else if has_data_changes && config.needs_refresh(watermark) {
                    RefreshDecision::Refreshed(RefreshResult { projection_name: String::new(), row_count: 0 })
                } else {
                    RefreshDecision::Skipped { reason: "no new changes".to_string() }
                }
            }
            RefreshPolicy::Periodic(interval) => {
                let last_time = self.last_refresh_times.get(&config.projection_name().to_lowercase());
                let elapsed = last_time.map(|t| now.duration_since(*t)).unwrap_or(*interval);
                if !config.has_been_refreshed() {
                    // Never refreshed — backfill.
                    RefreshDecision::Refreshed(RefreshResult { projection_name: String::new(), row_count: 0 })
                } else if elapsed >= *interval {
                    RefreshDecision::Refreshed(RefreshResult { projection_name: String::new(), row_count: 0 })
                } else {
                    RefreshDecision::Skipped {
                        reason: format!(
                            "interval not elapsed ({:.1}s / {}s)",
                            elapsed.as_secs_f64(),
                            interval.as_secs()
                        ),
                    }
                }
            }
        }
    }

    // ── Manual refresh ─────────────────────────────────────────

    /// Force-refresh a single projection regardless of its policy.
    ///
    /// Useful for manual-policy projections or on-demand re-materialization.
    pub fn force_refresh(
        &mut self,
        name: &str,
        executor: &dyn SourceQueryExecutor,
        watermark: u64,
    ) -> Result<RefreshResult, MaterializationError> {
        let result = self.materializer.refresh_one(name, executor)?;
        let key = name.to_lowercase();
        if let Some(cfg) = self.configs.get_mut(&key) {
            cfg.record_refresh(watermark);
        }
        self.last_refresh_times.insert(key, Instant::now());
        Ok(result)
    }

    // ── Async run loop ─────────────────────────────────────────

    /// Run the background refresh loop until shutdown.
    ///
    /// Polls on the given `poll_interval`, executing refresh cycles.
    /// Stops when `shutdown` resolves.
    pub async fn run_loop(
        &mut self,
        detector: &dyn ChangeDetector,
        executor: &dyn SourceQueryExecutor,
        poll_interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {
                    let _ = self.run_cycle(detector, executor);
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use encoding::value::value_type::ValueTypeCategory;

    use super::*;
    use crate::{
        catalog::MaterializedCatalog,
        change_detector::ChangeEvent,
        definition::{ColumnDefinition, ProjectionDefinition},
    };

    // ── Test helpers ─────────────────────────────────────────────

    struct MockDetector {
        events: Vec<ChangeEvent>,
        watermark: u64,
    }

    impl ChangeDetector for MockDetector {
        fn poll_changes(&self, after_sequence_number: u64) -> Result<Vec<ChangeEvent>, String> {
            Ok(self.events.iter().filter(|e| e.sequence_number > after_sequence_number).cloned().collect())
        }

        fn current_watermark(&self) -> Result<u64, String> {
            Ok(self.watermark)
        }
    }

    struct MockExecutor {
        rows: Vec<Vec<Option<String>>>,
    }

    impl SourceQueryExecutor for MockExecutor {
        fn execute(&self, _db: &str, _query: &str) -> Result<Vec<Vec<Option<String>>>, String> {
            Ok(self.rows.clone())
        }
    }

    fn test_def(name: &str) -> ProjectionDefinition {
        ProjectionDefinition::new(
            name,
            vec![ColumnDefinition::new("id", ValueTypeCategory::Integer)],
            "match $x isa thing;",
        )
        .unwrap()
    }

    fn test_catalog_with(names: &[&str]) -> MaterializedCatalog {
        let catalog = MaterializedCatalog::new();
        for name in names {
            catalog.register(test_def(name));
        }
        catalog
    }

    fn test_refresher(names: &[&str]) -> BackgroundRefresher {
        let catalog = test_catalog_with(names);
        let mat = Materializer::new(catalog, "testdb");
        BackgroundRefresher::new(mat)
    }

    // ── Construction ─────────────────────────────────────────────

    #[test]
    fn new_has_no_configs() {
        let r = test_refresher(&[]);
        assert_eq!(r.config_count(), 0);
        assert!(r.registered_projections().is_empty());
    }

    #[test]
    fn register_adds_config() {
        let mut r = test_refresher(&["people"]);
        r.register(ProjectionRefreshConfig::new("people", RefreshPolicy::Eager));
        assert_eq!(r.config_count(), 1);
        assert!(r.get_config("people").is_some());
    }

    #[test]
    fn register_is_case_insensitive() {
        let mut r = test_refresher(&["people"]);
        r.register(ProjectionRefreshConfig::new("People", RefreshPolicy::Eager));
        assert!(r.get_config("PEOPLE").is_some());
    }

    #[test]
    fn unregister_removes_config() {
        let mut r = test_refresher(&["people"]);
        r.register(ProjectionRefreshConfig::new("people", RefreshPolicy::Eager));
        let removed = r.unregister("people");
        assert!(removed.is_some());
        assert_eq!(r.config_count(), 0);
    }

    #[test]
    fn unregister_nonexistent_returns_none() {
        let mut r = test_refresher(&[]);
        assert!(r.unregister("ghost").is_none());
    }

    // ── Cycle with eager policy ──────────────────────────────────

    #[test]
    fn cycle_refreshes_eager_on_data_change() {
        let mut r = test_refresher(&["A"]);
        r.register(ProjectionRefreshConfig::new("A", RefreshPolicy::Eager));
        // First cycle — backfill (never refreshed).
        let detector = MockDetector { events: vec![ChangeEvent::data(10)], watermark: 10 };
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.refreshed_count(), 1);
        assert_eq!(r.get_config("a").unwrap().last_sequence_number(), 10);
    }

    #[test]
    fn cycle_skips_eager_when_no_changes() {
        let mut r = test_refresher(&["A"]);
        let mut cfg = ProjectionRefreshConfig::new("A", RefreshPolicy::Eager);
        cfg.record_refresh(10); // pretend already refreshed
        r.register(cfg);
        let detector = MockDetector { events: vec![], watermark: 10 };
        let executor = MockExecutor { rows: vec![] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.skipped_count(), 1);
    }

    #[test]
    fn cycle_refreshes_eager_on_new_data() {
        let mut r = test_refresher(&["A"]);
        let mut cfg = ProjectionRefreshConfig::new("A", RefreshPolicy::Eager);
        cfg.record_refresh(10);
        r.register(cfg);
        let detector = MockDetector { events: vec![ChangeEvent::data(15)], watermark: 15 };
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.refreshed_count(), 1);
        assert_eq!(r.get_config("a").unwrap().last_sequence_number(), 15);
    }

    // ── Cycle with manual policy ─────────────────────────────────

    #[test]
    fn cycle_skips_manual() {
        let mut r = test_refresher(&["B"]);
        r.register(ProjectionRefreshConfig::new("B", RefreshPolicy::Manual));
        let detector = MockDetector { events: vec![ChangeEvent::data(10)], watermark: 10 };
        let executor = MockExecutor { rows: vec![] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.skipped_count(), 1);
        assert_eq!(result.refreshed_count(), 0);
    }

    // ── Cycle with periodic policy ───────────────────────────────

    #[test]
    fn cycle_backfills_periodic_on_first_run() {
        let mut r = test_refresher(&["C"]);
        r.register(ProjectionRefreshConfig::new("C", RefreshPolicy::Periodic(Duration::from_secs(60))));
        let detector = MockDetector { events: vec![], watermark: 5 };
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        // Never refreshed → backfill.
        assert_eq!(result.refreshed_count(), 1);
    }

    #[test]
    fn cycle_skips_periodic_before_interval() {
        let mut r = test_refresher(&["C"]);
        let mut cfg = ProjectionRefreshConfig::new("C", RefreshPolicy::Periodic(Duration::from_secs(300)));
        cfg.record_refresh(10);
        r.register(cfg);
        // last_refresh_times was set to now by register(), so interval hasn't elapsed.
        let detector = MockDetector { events: vec![ChangeEvent::data(15)], watermark: 15 };
        let executor = MockExecutor { rows: vec![] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.skipped_count(), 1);
    }

    // ── Multiple projections ─────────────────────────────────────

    #[test]
    fn cycle_handles_multiple_projections() {
        let mut r = test_refresher(&["eager_proj", "manual_proj"]);
        r.register(ProjectionRefreshConfig::new("eager_proj", RefreshPolicy::Eager));
        r.register(ProjectionRefreshConfig::new("manual_proj", RefreshPolicy::Manual));
        let detector = MockDetector { events: vec![ChangeEvent::data(10)], watermark: 10 };
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        let result = r.run_cycle(&detector, &executor).unwrap();
        assert_eq!(result.total(), 2);
        // Eager gets refreshed (backfill), manual gets skipped.
        assert_eq!(result.skipped_count(), 1);
    }

    // ── Force refresh ────────────────────────────────────────────

    #[test]
    fn force_refresh_works_for_manual() {
        let mut r = test_refresher(&["M"]);
        r.register(ProjectionRefreshConfig::new("M", RefreshPolicy::Manual));
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        let result = r.force_refresh("M", &executor, 50).unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(r.get_config("m").unwrap().last_sequence_number(), 50);
        assert_eq!(r.get_config("m").unwrap().refresh_count(), 1);
    }

    #[test]
    fn force_refresh_updates_sequence_number() {
        let mut r = test_refresher(&["X"]);
        r.register(ProjectionRefreshConfig::new("X", RefreshPolicy::Eager));
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };
        r.force_refresh("X", &executor, 100).unwrap();
        assert_eq!(r.get_config("x").unwrap().last_sequence_number(), 100);
    }

    #[test]
    fn force_refresh_not_found() {
        let mut r = test_refresher(&[]);
        let executor = MockExecutor { rows: vec![] };
        let err = r.force_refresh("ghost", &executor, 0).unwrap_err();
        assert!(matches!(err, MaterializationError::ProjectionNotFound(_)));
    }

    // ── CycleResult ──────────────────────────────────────────────

    #[test]
    fn cycle_result_counts() {
        let result = CycleResult {
            decisions: vec![
                ("a".into(), RefreshDecision::Refreshed(RefreshResult { projection_name: "a".into(), row_count: 5 })),
                ("b".into(), RefreshDecision::Skipped { reason: "manual".into() }),
                ("c".into(), RefreshDecision::Failed(MaterializationError::ProjectionNotFound("c".into()))),
            ],
            changes: ChangeSummary::from_events(vec![]),
        };
        assert_eq!(result.total(), 3);
        assert_eq!(result.refreshed_count(), 1);
        assert_eq!(result.skipped_count(), 1);
        assert_eq!(result.failed_count(), 1);
    }

    // ── Sequence number bookkeeping ──────────────────────────────

    #[test]
    fn multiple_cycles_advance_sequence() {
        let mut r = test_refresher(&["A"]);
        r.register(ProjectionRefreshConfig::new("A", RefreshPolicy::Eager));
        let executor = MockExecutor { rows: vec![vec![Some("1".into())]] };

        // Cycle 1: backfill at watermark 10.
        let d1 = MockDetector { events: vec![ChangeEvent::data(10)], watermark: 10 };
        r.run_cycle(&d1, &executor).unwrap();
        assert_eq!(r.get_config("a").unwrap().last_sequence_number(), 10);

        // Cycle 2: new data at 20.
        let d2 = MockDetector { events: vec![ChangeEvent::data(20)], watermark: 20 };
        r.run_cycle(&d2, &executor).unwrap();
        assert_eq!(r.get_config("a").unwrap().last_sequence_number(), 20);
        assert_eq!(r.get_config("a").unwrap().refresh_count(), 2);
    }

    // ── Async run_loop ───────────────────────────────────────────

    #[tokio::test]
    async fn run_loop_stops_on_shutdown() {
        let mut r = test_refresher(&[]);
        let (tx, rx) = tokio::sync::watch::channel(false);

        struct NoopDetector;
        impl ChangeDetector for NoopDetector {
            fn poll_changes(&self, _: u64) -> Result<Vec<ChangeEvent>, String> {
                Ok(vec![])
            }
            fn current_watermark(&self) -> Result<u64, String> {
                Ok(0)
            }
        }
        let executor = MockExecutor { rows: vec![] };

        // Signal shutdown almost immediately.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        tokio::time::timeout(
            Duration::from_secs(2),
            r.run_loop(&NoopDetector, &executor, Duration::from_millis(10), rx),
        )
        .await
        .expect("run_loop should stop within timeout");
    }
}
