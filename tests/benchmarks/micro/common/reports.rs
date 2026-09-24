/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, path::Path, time::Duration};

use resource::profile::{QueryProfile, StageProfile, SubstepProfile};
use serde::Serialize;
use tabled::Tabled;

use crate::templates::{MultiTxMultiQueryProfile, TxQueryProfile};

/// Wraps a Duration: displays as ms with 3dp, serializes as f64 ms for CSV.
#[derive(Clone, Copy)]
pub struct DurationMs(pub Duration);

impl fmt::Display for DurationMs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.3}", self.0.as_nanos() as f64 / 1_000_000.0)
    }
}

impl Serialize for DurationMs {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(self.0.as_nanos() as f64 / 1_000_000.0)
    }
}

impl tabled::Tabled for DurationMs {
    const LENGTH: usize = 1;
    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        vec![format!("{self}").into()]
    }
    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec!["ms".into()]
    }
}

// ---------------------------------------------------------------------------
// MultiTxMultiQueryProfile report
// ---------------------------------------------------------------------------

// --- Per-transaction row ---

#[derive(Tabled, Serialize)]
pub struct TxTimingRow {
    pub txn_index: usize,
    pub driver_wall_ms: DurationMs,
    pub mean_query_ms: DurationMs,
    pub p50_query_ms: DurationMs,
    pub p95_query_ms: DurationMs,
    pub p99_query_ms: DurationMs,
    pub commit_ms: DurationMs,
    pub commit_things_finalise_ms: DurationMs,
    pub commit_snapshot_put_statuses_check_ms: DurationMs,
    pub commit_snapshot_commit_record_create_ms: DurationMs,
    pub commit_snapshot_durable_write_data_submit_ms: DurationMs,
    pub commit_snapshot_isolation_validate_ms: DurationMs,
    pub commit_snapshot_durable_write_data_confirm_ms: DurationMs,
    pub commit_snapshot_storage_write_ms: DurationMs,
    pub commit_snapshot_isolation_manager_notify_ms: DurationMs,
    pub commit_snapshot_durable_write_commit_status_submit_ms: DurationMs,
}

// --- Summary stats across transactions ---

#[derive(Tabled, Serialize)]
pub struct TimingStats {
    pub metric: String,
    pub n: usize,
    pub mean_ms: DurationMs,
    pub p50_ms: DurationMs,
    pub p95_ms: DurationMs,
    pub p99_ms: DurationMs,
    pub pct_of_outer: String,
}

impl TimingStats {
    fn compute(metric: impl Into<String>, mut values: Vec<Duration>, outer_total: Option<Duration>) -> Self {
        let metric = metric.into();
        let n = values.len();
        if n == 0 {
            let z = DurationMs(Duration::ZERO);
            return Self { metric, n: 0, mean_ms: z, p50_ms: z, p95_ms: z, p99_ms: z, pct_of_outer: "—".into() };
        }
        let total: Duration = values.iter().sum();
        let mean = total / n as u32;
        values.sort();
        let pct = |p: f64| DurationMs(values[((p / 100.0) * (n - 1) as f64).round() as usize]);
        let pct_of_outer = outer_total
            .filter(|&o| !o.is_zero())
            .map_or("—".into(), |o| format!("{:.1}%", total.as_nanos() as f64 / o.as_nanos() as f64 * 100.0));
        Self {
            metric,
            n,
            mean_ms: DurationMs(mean),
            p50_ms: pct(50.0),
            p95_ms: pct(95.0),
            p99_ms: pct(99.0),
            pct_of_outer,
        }
    }
}

// --- Top-level report ---

pub struct MultiQueryTxProfileReport {
    pub per_txn: Vec<TxTimingRow>,
    pub summary: Vec<TimingStats>,
}

impl From<MultiTxMultiQueryProfile> for MultiQueryTxProfileReport {
    fn from(profile: MultiTxMultiQueryProfile) -> Self {
        Self::from_ref(&profile)
    }
}

impl MultiQueryTxProfileReport {
    pub fn from_ref(profile: &MultiTxMultiQueryProfile) -> Self {
        let n = profile.profiles.len();
        let mut per_txn = Vec::with_capacity(n);
        let mut wall: Vec<Duration> = Vec::with_capacity(n);
        let mut mean_query: Vec<Duration> = Vec::with_capacity(n);
        let mut commit: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_things_finalise: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_put_statuses_check: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_commit_record_create: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_durable_write_data_submit: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_isolation_validate: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_durable_write_data_confirm: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_storage_write: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_isolation_manager_notify: Vec<Duration> = Vec::with_capacity(n);
        let mut phase_snapshot_durable_write_commit_status_submit: Vec<Duration> = Vec::with_capacity(n);

        for (i, tx) in profile.profiles.iter().enumerate() {
            let commit_total = tx.tx_profile.commit_profile_ref().total();
            let phases = tx.tx_profile.commit_profile_ref().phases();

            let mut query_durations: Vec<Duration> = tx.query_profiles.iter().map(|q| q.total_duration()).collect();
            query_durations.sort();
            let n_q = query_durations.len();
            let mean_q = if n_q > 0 { query_durations.iter().sum::<Duration>() / n_q as u32 } else { Duration::ZERO };
            let pct_q = |p: f64| -> DurationMs {
                DurationMs(if n_q == 0 {
                    Duration::ZERO
                } else {
                    query_durations[((p / 100.0) * (n_q - 1) as f64).round() as usize]
                })
            };

            let z = phases.as_ref();
            let zd = |f: fn(&_) -> Duration| z.map_or(Duration::ZERO, |p| f(p));
            per_txn.push(TxTimingRow {
                txn_index: i,
                driver_wall_ms: DurationMs(tx.time_elapsed),
                mean_query_ms: DurationMs(mean_q),
                p50_query_ms: pct_q(50.0),
                p95_query_ms: pct_q(95.0),
                p99_query_ms: pct_q(99.0),
                commit_ms: DurationMs(commit_total),
                commit_things_finalise_ms: DurationMs(zd(|p| p.things_finalise)),
                commit_snapshot_put_statuses_check_ms: DurationMs(zd(|p| p.snapshot_put_statuses_check)),
                commit_snapshot_commit_record_create_ms: DurationMs(zd(|p| p.snapshot_commit_record_create)),
                commit_snapshot_durable_write_data_submit_ms: DurationMs(zd(|p| p.snapshot_durable_write_data_submit)),
                commit_snapshot_isolation_validate_ms: DurationMs(zd(|p| p.snapshot_isolation_validate)),
                commit_snapshot_durable_write_data_confirm_ms: DurationMs(zd(|p| {
                    p.snapshot_durable_write_data_confirm
                })),
                commit_snapshot_storage_write_ms: DurationMs(zd(|p| p.snapshot_storage_write)),
                commit_snapshot_isolation_manager_notify_ms: DurationMs(zd(|p| p.snapshot_isolation_manager_notify)),
                commit_snapshot_durable_write_commit_status_submit_ms: DurationMs(zd(|p| {
                    p.snapshot_durable_write_commit_status_submit
                })),
            });

            wall.push(tx.time_elapsed);
            mean_query.push(mean_q);
            commit.push(commit_total);
            phase_things_finalise.push(zd(|p| p.things_finalise));
            phase_snapshot_put_statuses_check.push(zd(|p| p.snapshot_put_statuses_check));
            phase_snapshot_commit_record_create.push(zd(|p| p.snapshot_commit_record_create));
            phase_snapshot_durable_write_data_submit.push(zd(|p| p.snapshot_durable_write_data_submit));
            phase_snapshot_isolation_validate.push(zd(|p| p.snapshot_isolation_validate));
            phase_snapshot_durable_write_data_confirm.push(zd(|p| p.snapshot_durable_write_data_confirm));
            phase_snapshot_storage_write.push(zd(|p| p.snapshot_storage_write));
            phase_snapshot_isolation_manager_notify.push(zd(|p| p.snapshot_isolation_manager_notify));
            phase_snapshot_durable_write_commit_status_submit
                .push(zd(|p| p.snapshot_durable_write_commit_status_submit));
        }

        let commit_total: Duration = commit.iter().sum();
        let summary = vec![
            TimingStats::compute("driver_wall", wall, None),
            TimingStats::compute("mean_query", mean_query, None),
            TimingStats::compute("commit", commit, None),
            TimingStats::compute("commit::things_finalise", phase_things_finalise, Some(commit_total)),
            TimingStats::compute(
                "commit::snapshot_put_statuses_check",
                phase_snapshot_put_statuses_check,
                Some(commit_total),
            ),
            TimingStats::compute(
                "commit::snapshot_commit_record_create",
                phase_snapshot_commit_record_create,
                Some(commit_total),
            ),
            TimingStats::compute(
                "commit::snapshot_durable_write_data_submit",
                phase_snapshot_durable_write_data_submit,
                Some(commit_total),
            ),
            TimingStats::compute(
                "commit::snapshot_isolation_validate",
                phase_snapshot_isolation_validate,
                Some(commit_total),
            ),
            TimingStats::compute(
                "commit::snapshot_durable_write_data_confirm",
                phase_snapshot_durable_write_data_confirm,
                Some(commit_total),
            ),
            TimingStats::compute("commit::snapshot_storage_write", phase_snapshot_storage_write, Some(commit_total)),
            TimingStats::compute(
                "commit::snapshot_isolation_manager_notify",
                phase_snapshot_isolation_manager_notify,
                Some(commit_total),
            ),
            TimingStats::compute(
                "commit::snapshot_durable_write_commit_status_submit",
                phase_snapshot_durable_write_commit_status_submit,
                Some(commit_total),
            ),
        ];

        Self { per_txn, summary }
    }
}

impl MultiQueryTxProfileReport {
    pub fn write_and_print(&self, name: &str) {
        let base = std::env::current_dir().unwrap().join("benchmark_reports");
        match self.write_csvs(&base, name) {
            Ok(folder) => eprintln!("Report written to: {}", folder.display()),
            Err(e) => eprintln!("Failed to write report: {e}"),
        }
        self.print_summary_table();
    }

    fn write_csvs(&self, output_dir: &Path, name: &str) -> std::io::Result<std::path::PathBuf> {
        let timestamp =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let folder = output_dir.join(format!("{}_{}", timestamp, name));
        std::fs::create_dir_all(&folder)?;
        write_csv(folder.join("per_txn.csv"), &self.per_txn)?;
        write_csv(folder.join("summary.csv"), &self.summary)?;
        Ok(folder)
    }

    fn print_summary_table(&self) {
        println!("{}", tabled::Table::new(&self.summary));
    }
}

// ---------------------------------------------------------------------------
// TxQueryProfile report
// ---------------------------------------------------------------------------

#[derive(Tabled, Serialize)]
pub struct QueryStepRow {
    pub stage_id: u64,
    pub step_description: String,
    pub total_ms: DurationMs,
    pub batches: u64,
    pub rows: u64,
    pub us_per_row: DurationMs,
}

pub struct TxQueryProfileReport {
    pub steps: Vec<QueryStepRow>,
    pub total_query: DurationMs,
}

impl From<&TxQueryProfile> for TxQueryProfileReport {
    fn from(profile: &TxQueryProfile) -> Self {
        let total_query = DurationMs(profile.query_profile.total_duration());
        let mut steps = Vec::new();
        collect_steps(&profile.query_profile, &mut steps);
        Self { steps, total_query }
    }
}

impl TxQueryProfileReport {
    pub fn write_and_print(&self, name: &str) {
        let base = std::env::current_dir().unwrap().join("benchmark_reports");
        match self.write_csvs(&base, name) {
            Ok(folder) => eprintln!("Report written to: {}", folder.display()),
            Err(e) => eprintln!("Failed to write report: {e}"),
        }
        self.print_steps_table();
    }

    fn write_csvs(&self, output_dir: &Path, name: &str) -> std::io::Result<std::path::PathBuf> {
        let timestamp =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let folder = output_dir.join(format!("{}_{}", timestamp, name));
        std::fs::create_dir_all(&folder)?;
        write_csv(folder.join("query_steps.csv"), &self.steps)?;
        Ok(folder)
    }

    fn print_steps_table(&self) {
        println!("Total query time: {} ms", self.total_query);
        println!("{}", tabled::Table::new(&self.steps));
    }
}

fn collect_steps(query: &QueryProfile, out: &mut Vec<QueryStepRow>) {
    for (&stage_id, stage) in query.stage_profiles().read().unwrap().iter() {
        collect_stage_steps(stage_id, stage, out);
    }
}

fn collect_stage_steps(stage_id: u64, stage: &StageProfile, out: &mut Vec<QueryStepRow>) {
    if let Some(pattern) = stage.pattern_profile() {
        for substep in pattern.substeps().read().unwrap().iter() {
            match substep {
                SubstepProfile::StepProfile(step) => {
                    let total = DurationMs(Duration::from_nanos(step.total_nanos()));
                    let rows = step.rows();
                    let us_per_row = DurationMs(if rows > 0 {
                        Duration::from_nanos(step.total_nanos() / rows)
                    } else {
                        Duration::ZERO
                    });
                    out.push(QueryStepRow {
                        stage_id,
                        step_description: step.description().unwrap_or("").to_owned(),
                        total_ms: total,
                        batches: step.batches(),
                        rows,
                        us_per_row,
                    });
                }
                SubstepProfile::QueryProfile { profile, description } => {
                    out.push(QueryStepRow {
                        stage_id,
                        step_description: format!("[fn] {description}"),
                        total_ms: DurationMs(profile.total_duration()),
                        batches: 0,
                        rows: 0,
                        us_per_row: DurationMs(Duration::ZERO),
                    });
                    collect_steps(profile, out);
                }
                SubstepProfile::PatternProfile(pattern) => {
                    for substep in pattern.substeps().read().unwrap().iter() {
                        if let SubstepProfile::StepProfile(step) = substep {
                            let total = DurationMs(Duration::from_nanos(step.total_nanos()));
                            let rows = step.rows();
                            let us_per_row = DurationMs(if rows > 0 {
                                Duration::from_nanos(step.total_nanos() / rows)
                            } else {
                                Duration::ZERO
                            });
                            out.push(QueryStepRow {
                                stage_id,
                                step_description: step.description().unwrap_or("").to_owned(),
                                total_ms: total,
                                batches: step.batches(),
                                rows,
                                us_per_row,
                            });
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn write_csv<T: Serialize>(path: impl AsRef<Path>, rows: &[T]) -> std::io::Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}
