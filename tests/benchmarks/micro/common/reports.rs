/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::Path, time::Duration};

use resource::profile::{QueryProfile, StageProfile, SubstepProfile};
use serde::Serialize;
use tabled::Tabled;

use crate::templates::{MultiTxMultiQueryProfile, TxQueryProfile};

// ---------------------------------------------------------------------------
// MultiTxMultiQueryProfile report
// ---------------------------------------------------------------------------

// --- Per-transaction row ---

#[derive(Tabled, Serialize)]
pub struct TxTimingRow {
    pub txn_index: usize,
    pub driver_wall_us: f64,
    pub mean_query_us: f64,
    pub p50_query_us: f64,
    pub p95_query_us: f64,
    pub p99_query_us: f64,
    pub commit_us: f64,
    pub commit_types_validation_us: f64,
    pub commit_things_finalise_us: f64,
    pub commit_functions_finalise_us: f64,
    pub commit_snapshot_put_statuses_check_us: f64,
    pub commit_snapshot_commit_record_create_us: f64,
    pub commit_snapshot_durable_write_data_submit_us: f64,
    pub commit_snapshot_isolation_validate_us: f64,
    pub commit_snapshot_durable_write_data_confirm_us: f64,
    pub commit_snapshot_storage_write_us: f64,
    pub commit_snapshot_isolation_manager_notify_us: f64,
    pub commit_snapshot_durable_write_commit_status_submit_us: f64,
    pub commit_schema_update_statistics_durable_write_us: f64,
    pub commit_schema_update_caches_update_us: f64,
    pub commit_schema_update_statistics_update_us: f64,
}

// --- Summary stats across transactions ---

#[derive(Tabled, Serialize)]
pub struct TimingStats {
    pub metric: String,
    pub n: usize,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

impl TimingStats {
    fn compute(metric: impl Into<String>, mut values_us: Vec<f64>) -> Self {
        let metric = metric.into();
        let n = values_us.len();
        if n == 0 {
            return Self { metric, n: 0, mean_us: 0.0, p50_us: 0.0, p95_us: 0.0, p99_us: 0.0 };
        }
        let mean_us = values_us.iter().sum::<f64>() / n as f64;
        values_us.sort_by(f64::total_cmp);
        let pct = |p: f64| values_us[((p / 100.0) * (n - 1) as f64).round() as usize];
        Self { metric, n, mean_us, p50_us: pct(50.0), p95_us: pct(95.0), p99_us: pct(99.0) }
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
        let mut per_txn = Vec::with_capacity(profile.profiles.len());
        let mut wall_us_all = Vec::with_capacity(profile.profiles.len());
        let mut mean_query_us_all = Vec::with_capacity(profile.profiles.len());
        let mut commit_us_all = Vec::with_capacity(profile.profiles.len());

        for (i, tx) in profile.profiles.iter().enumerate() {
            let driver_wall_us = dur_us(tx.time_elapsed);
            let commit_us = dur_us(tx.tx_profile.commit_profile_ref().total());
            let phases = tx.tx_profile.commit_profile_ref().phases();

            let mut query_durations_us: Vec<f64> =
                tx.query_profiles.iter().map(|q| dur_us(q.total_duration())).collect();
            query_durations_us.sort_by(f64::total_cmp);
            let n_q = query_durations_us.len();
            let mean_query_us =
                if n_q > 0 { query_durations_us.iter().sum::<f64>() / n_q as f64 } else { 0.0 };
            let pct_q = |p: f64| -> f64 {
                if n_q == 0 {
                    return 0.0;
                }
                query_durations_us[((p / 100.0) * (n_q - 1) as f64).round() as usize]
            };

            let z = phases.as_ref();
            per_txn.push(TxTimingRow {
                txn_index: i,
                driver_wall_us,
                mean_query_us,
                p50_query_us: pct_q(50.0),
                p95_query_us: pct_q(95.0),
                p99_query_us: pct_q(99.0),
                commit_us,
                commit_types_validation_us: z.map_or(0.0, |p| dur_us(p.types_validation)),
                commit_things_finalise_us: z.map_or(0.0, |p| dur_us(p.things_finalise)),
                commit_functions_finalise_us: z.map_or(0.0, |p| dur_us(p.functions_finalise)),
                commit_snapshot_put_statuses_check_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_put_statuses_check)),
                commit_snapshot_commit_record_create_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_commit_record_create)),
                commit_snapshot_durable_write_data_submit_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_durable_write_data_submit)),
                commit_snapshot_isolation_validate_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_isolation_validate)),
                commit_snapshot_durable_write_data_confirm_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_durable_write_data_confirm)),
                commit_snapshot_storage_write_us: z.map_or(0.0, |p| dur_us(p.snapshot_storage_write)),
                commit_snapshot_isolation_manager_notify_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_isolation_manager_notify)),
                commit_snapshot_durable_write_commit_status_submit_us: z
                    .map_or(0.0, |p| dur_us(p.snapshot_durable_write_commit_status_submit)),
                commit_schema_update_statistics_durable_write_us: z
                    .map_or(0.0, |p| dur_us(p.schema_update_statistics_durable_write)),
                commit_schema_update_caches_update_us: z
                    .map_or(0.0, |p| dur_us(p.schema_update_caches_update)),
                commit_schema_update_statistics_update_us: z
                    .map_or(0.0, |p| dur_us(p.schema_update_statistics_update)),
            });

            wall_us_all.push(driver_wall_us);
            mean_query_us_all.push(mean_query_us);
            commit_us_all.push(commit_us);
        }

        let summary = vec![
            TimingStats::compute("driver_wall", wall_us_all),
            TimingStats::compute("mean_query", mean_query_us_all),
            TimingStats::compute("commit", commit_us_all),
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
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
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
    pub total_us: f64,
    pub batches: u64,
    pub rows: u64,
    pub us_per_row: f64,
}

pub struct TxQueryProfileReport {
    pub steps: Vec<QueryStepRow>,
    pub total_query_us: f64,
}

impl From<&TxQueryProfile> for TxQueryProfileReport {
    fn from(profile: &TxQueryProfile) -> Self {
        let total_query_us = dur_us(profile.query_profile.total_duration());
        let mut steps = Vec::new();
        collect_steps(&profile.query_profile, &mut steps);
        Self { steps, total_query_us }
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
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let folder = output_dir.join(format!("{}_{}", timestamp, name));
        std::fs::create_dir_all(&folder)?;
        write_csv(folder.join("query_steps.csv"), &self.steps)?;
        Ok(folder)
    }

    fn print_steps_table(&self) {
        println!("Total query time: {:.1} µs", self.total_query_us);
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
                    let total_us = step.total_nanos() as f64 / 1_000.0;
                    let batches = step.batches();
                    let rows = step.rows();
                    out.push(QueryStepRow {
                        stage_id,
                        step_description: step.description().unwrap_or("").to_owned(),
                        total_us,
                        batches,
                        rows,
                        us_per_row: if rows > 0 { total_us / rows as f64 } else { 0.0 },
                    });
                }
                SubstepProfile::QueryProfile { profile, description } => {
                    // inline function call — recurse with its stage_id as 0 placeholder
                    out.push(QueryStepRow {
                        stage_id,
                        step_description: format!("[fn] {description}"),
                        total_us: dur_us(profile.total_duration()),
                        batches: 0,
                        rows: 0,
                        us_per_row: 0.0,
                    });
                    collect_steps(profile, out);
                }
                SubstepProfile::PatternProfile(pattern) => {
                    for substep in pattern.substeps().read().unwrap().iter() {
                        if let SubstepProfile::StepProfile(step) = substep {
                            let total_us = step.total_nanos() as f64 / 1_000.0;
                            let batches = step.batches();
                            let rows = step.rows();
                            out.push(QueryStepRow {
                                stage_id,
                                step_description: step.description().unwrap_or("").to_owned(),
                                total_us,
                                batches,
                                rows,
                                us_per_row: if rows > 0 { total_us / rows as f64 } else { 0.0 },
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

fn dur_us(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1_000.0
}
