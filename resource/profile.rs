/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use itertools::Itertools;

#[derive(Debug)]
pub struct TransactionProfile {
    enabled: bool,
    commit_profile: CommitProfile,
}

impl TransactionProfile {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Display for TransactionProfile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Transaction profile[enabled={}]", self.enabled)?;
        write!(f, "{}", self.commit_profile)
    }
}

impl TransactionProfile {
    pub fn new(enabled: bool) -> Self {
        Self { enabled, commit_profile: CommitProfile::new(enabled) }
    }

    pub fn commit_profile(&mut self) -> &mut CommitProfile {
        &mut self.commit_profile
    }
}

#[derive(Debug)]
pub struct CommitProfile {
    data: Option<Box<CommitProfileData>>,
}

impl Display for CommitProfile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.data {
            None => writeln!(f, "  Commit[enabled=false]"),
            Some(data) => {
                writeln!(f, "  Commit[enabled=true, total micros={}]", data.total_nanos as f64 / 1000.0)?;
                writeln!(f, "    storage counters: {}", self.storage_counters())?;
                writeln!(f, "    types validation micros: {}", data.types_validation_nanos as f64 / 1000.0)?;
                writeln!(f, "    things finalise micros: {}", data.things_finalise_nanos as f64 / 1000.0)?;
                writeln!(f, "    functions finalise micros: {}", data.functions_finalise_nanos as f64 / 1000.0)?;
                writeln!(
                    f,
                    "    schema update statistics durable write micros: {}",
                    data.schema_update_statistics_durable_write_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot put statuses check micros: {}",
                    data.snapshot_put_statuses_check_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot commit record create micros: {}",
                    data.snapshot_commit_record_create_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot durable write data submit micros: {}",
                    data.snapshot_durable_write_data_submit_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot isolation validate micros: {}",
                    data.snapshot_isolation_validate_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot durable write data confirm micros: {}",
                    data.snapshot_durable_write_data_confirm_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot storage write micros: {}",
                    data.snapshot_storage_write_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot isolation manager notify micros: {}",
                    data.snapshot_isolation_manager_notify_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    snapshot durable write commit status submit micros: {}",
                    data.snapshot_durable_write_commit_status_submit_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    schema update caches update micros: {}",
                    data.schema_update_caches_update_nanos as f64 / 1000.0
                )?;
                writeln!(
                    f,
                    "    schema update statistics update micros: {}",
                    data.schema_update_statistics_update_nanos as f64 / 1000.0
                )
            }
        }
    }
}

impl CommitProfile {
    pub const DISABLED: Self = Self { data: None };

    pub fn new(enabled: bool) -> Self {
        match enabled {
            true => Self { data: Some(Box::new(CommitProfileData::new())) },
            false => Self { data: None },
        }
    }

    pub fn start(&mut self) {
        if let Some(data) = &mut self.data {
            data.start = Instant::now();
        }
    }
    pub fn types_validated(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => data.types_validation_nanos = Instant::now().duration_since(data.start).as_nanos(),
        }
    }

    pub fn things_finalised(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.things_finalise_nanos =
                    Instant::now().duration_since(data.start).as_nanos() - data.types_validation_nanos
            }
        }
    }

    pub fn functions_finalised(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.functions_finalise_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
            }
        }
    }

    pub fn schema_update_statistics_durably_written(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.schema_update_statistics_durable_write_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
            }
        }
    }

    pub fn snapshot_put_statuses_checked(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_put_statuses_check_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
            }
        }
    }

    pub fn snapshot_commit_record_created(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_commit_record_create_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
            }
        }
    }

    pub fn snapshot_durable_write_data_submitted(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_durable_write_data_submit_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
            }
        }
    }

    pub fn snapshot_isolation_validated(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_isolation_validate_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
            }
        }
    }

    pub fn snapshot_durable_write_data_confirmed(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_durable_write_data_confirm_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
                    - data.snapshot_isolation_validate_nanos
            }
        }
    }

    pub fn snapshot_storage_written(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_storage_write_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
                    - data.snapshot_isolation_validate_nanos
                    - data.snapshot_durable_write_data_confirm_nanos
            }
        }
    }

    pub fn snapshot_isolation_manager_notified(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_isolation_manager_notify_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
                    - data.snapshot_isolation_validate_nanos
                    - data.snapshot_durable_write_data_confirm_nanos
                    - data.snapshot_storage_write_nanos
            }
        }
    }

    pub fn snapshot_durable_write_commit_status_submitted(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.snapshot_durable_write_commit_status_submit_nanos =
                    Instant::now().duration_since(data.start).as_nanos()
                        - data.types_validation_nanos
                        - data.things_finalise_nanos
                        - data.functions_finalise_nanos
                        - data.schema_update_statistics_durable_write_nanos
                        - data.snapshot_put_statuses_check_nanos
                        - data.snapshot_commit_record_create_nanos
                        - data.snapshot_durable_write_data_submit_nanos
                        - data.snapshot_isolation_validate_nanos
                        - data.snapshot_durable_write_data_confirm_nanos
                        - data.snapshot_storage_write_nanos
                        - data.snapshot_isolation_manager_notify_nanos
            }
        }
    }

    pub fn schema_update_caches_updated(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.schema_update_caches_update_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
                    - data.snapshot_isolation_validate_nanos
                    - data.snapshot_durable_write_data_confirm_nanos
                    - data.snapshot_storage_write_nanos
                    - data.snapshot_isolation_manager_notify_nanos
                    - data.snapshot_durable_write_commit_status_submit_nanos
            }
        }
    }

    pub fn schema_update_statistics_keys_updated(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.schema_update_statistics_update_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.types_validation_nanos
                    - data.things_finalise_nanos
                    - data.functions_finalise_nanos
                    - data.schema_update_statistics_durable_write_nanos
                    - data.snapshot_put_statuses_check_nanos
                    - data.snapshot_commit_record_create_nanos
                    - data.snapshot_durable_write_data_submit_nanos
                    - data.snapshot_isolation_validate_nanos
                    - data.snapshot_durable_write_data_confirm_nanos
                    - data.snapshot_storage_write_nanos
                    - data.snapshot_isolation_manager_notify_nanos
                    - data.snapshot_durable_write_commit_status_submit_nanos
                    - data.schema_update_caches_update_nanos
            }
        }
    }
    pub fn end(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => data.total_nanos = Instant::now().duration_since(data.start).as_nanos(),
        }
    }

    pub fn storage_counters(&self) -> StorageCounters {
        match &self.data {
            None => StorageCounters::DISABLED,
            Some(data) => data.counters.clone(),
        }
    }
}

/// Record the time different stages of a commit.
/// This struct is simplified to expect that we execute exactly these steps in a fixed order with no other (significant) operations.
#[derive(Debug)]
struct CommitProfileData {
    counters: StorageCounters,
    start: Instant,
    types_validation_nanos: u128,
    things_finalise_nanos: u128,
    functions_finalise_nanos: u128,
    schema_update_statistics_durable_write_nanos: u128,
    snapshot_put_statuses_check_nanos: u128,
    snapshot_commit_record_create_nanos: u128,
    snapshot_durable_write_data_submit_nanos: u128,
    snapshot_isolation_validate_nanos: u128,
    snapshot_durable_write_data_confirm_nanos: u128,
    snapshot_storage_write_nanos: u128,
    snapshot_isolation_manager_notify_nanos: u128,
    snapshot_durable_write_commit_status_submit_nanos: u128,
    schema_update_caches_update_nanos: u128,
    schema_update_statistics_update_nanos: u128,
    total_nanos: u128,
}

impl CommitProfileData {
    fn new() -> Self {
        Self {
            counters: StorageCounters::new_enabled(),
            start: Instant::now(), // DUMMY
            types_validation_nanos: 0,
            things_finalise_nanos: 0,
            functions_finalise_nanos: 0,
            schema_update_statistics_durable_write_nanos: 0,
            snapshot_put_statuses_check_nanos: 0,
            snapshot_commit_record_create_nanos: 0,
            snapshot_durable_write_data_submit_nanos: 0,
            snapshot_isolation_validate_nanos: 0,
            snapshot_durable_write_data_confirm_nanos: 0,
            snapshot_storage_write_nanos: 0,
            snapshot_isolation_manager_notify_nanos: 0,
            snapshot_durable_write_commit_status_submit_nanos: 0,
            schema_update_caches_update_nanos: 0,
            schema_update_statistics_update_nanos: 0,
            total_nanos: 0,
        }
    }
}

#[derive(Debug)]
pub struct QueryProfile {
    compile_profile: CompileProfile,
    stage_profiles: RwLock<HashMap<i64, Arc<StageProfile>>>,
    enabled: bool,
}

impl QueryProfile {
    pub fn new(enabled: bool) -> Self {
        Self { compile_profile: CompileProfile::new(enabled), stage_profiles: RwLock::new(HashMap::new()), enabled }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn compilation_profile(&mut self) -> &mut CompileProfile {
        &mut self.compile_profile
    }

    pub fn profile_stage(&self, description_fn: impl Fn() -> String, id: i64) -> Arc<StageProfile> {
        if self.enabled {
            let profiles = self.stage_profiles.read().unwrap();
            if let Some(profile) = profiles.get(&id) {
                profile.clone()
            } else {
                drop(profiles);
                let profile = Arc::new(StageProfile::new(description_fn(), true));
                self.stage_profiles.write().unwrap().insert(id, profile.clone());
                profile
            }
        } else {
            Arc::new(StageProfile::new(String::new(), false))
        }
    }

    pub fn stage_profiles(&self) -> &RwLock<HashMap<i64, Arc<StageProfile>>> {
        &self.stage_profiles
    }
}

impl fmt::Display for QueryProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let compile_micros = self.compile_profile.total_micros();
        let stage_profiles = self.stage_profiles.read().unwrap();
        let total_micros = compile_micros
            + stage_profiles
                .iter()
                .map(|(_, stage_profile)| {
                    stage_profile
                        .step_profiles
                        .read()
                        .unwrap()
                        .iter()
                        .map(|step_profile| {
                            step_profile.data.as_ref().map(|data| data.nanos.load(Ordering::SeqCst)).unwrap_or(0)
                        })
                        .sum::<u64>()
                })
                .sum::<u64>() as f64
                / 1000.0;
        writeln!(f, "Query profile[measurements_enabled={}, total micros: {}]", self.enabled, total_micros)?;
        writeln!(f, "{}", self.compile_profile);
        for (id, pattern_profile) in stage_profiles.iter().sorted_by_key(|(id, _)| *id) {
            writeln!(f, "  -----")?;
            writeln!(f, "  Stage or Pattern [id={}] - {}", id, &pattern_profile.description)?;
            write!(f, "{}", pattern_profile)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CompileProfile {
    data: Option<CompileProfileData>,
}

impl CompileProfile {
    fn new(enabled: bool) -> Self {
        if enabled {
            Self {
                data: Some(CompileProfileData {
                    start: Instant::now(), // irrelevant
                    translation_nanos: 0,
                    validation_nanos: 0,
                    annotation_nanos: 0,
                    compilation_nanos: 0,
                }),
            }
        } else {
            Self { data: None }
        }
    }

    pub fn start(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => data.start = Instant::now(),
        }
    }

    pub fn translation_finished(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => data.translation_nanos = Instant::now().duration_since(data.start).as_nanos(),
        }
    }

    pub fn validation_finished(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.validation_nanos = Instant::now().duration_since(data.start).as_nanos() - data.translation_nanos
            }
        }
    }

    pub fn annotation_finished(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.annotation_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.translation_nanos
                    - data.translation_nanos
            }
        }
    }

    pub fn compilation_finished(&mut self) {
        match &mut self.data {
            None => {}
            Some(data) => {
                data.compilation_nanos = Instant::now().duration_since(data.start).as_nanos()
                    - data.translation_nanos
                    - data.translation_nanos
                    - data.annotation_nanos
            }
        }
    }

    fn total_micros(&self) -> f64 {
        match &self.data {
            None => 0.0,
            Some(data) => {
                (data.translation_nanos + data.validation_nanos + data.annotation_nanos + data.compilation_nanos) as f64
                    / 1000.0
            }
        }
    }
}

impl Display for CompileProfile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.data {
            None => writeln!(f, "  Compile[enabled=false]"),
            Some(data) => {
                writeln!(f, "  Compile[enabled=true, total micros={}", self.total_micros())?;
                writeln!(f, "    translation micros: {}", data.translation_nanos as f64 / 1000.0)?;
                writeln!(f, "    validation micros: {}", data.validation_nanos as f64 / 1000.0)?;
                writeln!(f, "    annotation micros: {}", data.annotation_nanos as f64 / 1000.0)?;
                writeln!(f, "    compilation micros: {}", data.compilation_nanos as f64 / 1000.0)
            }
        }
    }
}

/// Record the time different stages of a query compilation take.
/// This struct is simplified to expect that we execute exactly these steps in a fixed order with no other (significant) operations.
#[derive(Debug)]
struct CompileProfileData {
    start: Instant,
    translation_nanos: u128,
    validation_nanos: u128,
    annotation_nanos: u128,
    compilation_nanos: u128,
}

#[derive(Debug)]
pub struct StageProfile {
    description: String,
    step_profiles: RwLock<Vec<Arc<StepProfile>>>,
    enabled: bool,
}

impl StageProfile {
    fn new(description: String, enabled: bool) -> Self {
        Self { description, step_profiles: RwLock::new(Vec::new()), enabled }
    }

    pub fn extend_or_get(&self, index: usize, description_getter: impl Fn() -> String) -> Arc<StepProfile> {
        if self.enabled {
            let profiles = self.step_profiles.read().unwrap();
            if index < profiles.len() {
                profiles[index].clone()
            } else {
                debug_assert!(index == profiles.len(), "Can only extend step profiles sequentially");
                let profile = Arc::new(StepProfile::new_enabled(description_getter()));
                drop(profiles);
                let mut profiles_mut = self.step_profiles.write().unwrap();
                profiles_mut.push(profile.clone());
                profile
            }
        } else {
            Arc::new(StepProfile::new_disabled())
        }
    }
}

impl fmt::Display for StageProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, step_profile) in self.step_profiles.read().unwrap().iter().enumerate() {
            match step_profile.data.as_ref() {
                None => writeln!(f, "    {}.\n", i)?,
                Some(data) => writeln!(f, "    {}. {}\n", i, data)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct StepProfile {
    data: Option<StepProfileData>,
}

#[derive(Debug)]
struct StepProfileData {
    description: String,
    batches: AtomicU64,
    rows: AtomicU64,
    nanos: AtomicU64,
    storage: StorageCounters,
}

impl StepProfile {
    fn new_enabled(description: String) -> Self {
        Self {
            data: Some(StepProfileData {
                description,
                batches: AtomicU64::new(0),
                rows: AtomicU64::new(0),
                nanos: AtomicU64::new(0),
                storage: StorageCounters::new_enabled(),
            }),
        }
    }

    fn new_disabled() -> Self {
        Self { data: None }
    }

    pub fn start_measurement(&self) -> StepProfileMeasurement {
        if self.data.is_some() {
            StepProfileMeasurement::new(Some(Instant::now()))
        } else {
            StepProfileMeasurement::new(None)
        }
    }

    pub fn storage_counters(&self) -> StorageCounters {
        if let Some(data) = self.data.as_ref() {
            data.storage.clone()
        } else {
            StorageCounters::DISABLED
        }
    }
}

impl fmt::Display for StepProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rows = self.rows.load(Ordering::Relaxed);
        let micros = Duration::from_nanos(self.nanos.load(Ordering::Relaxed)).as_micros();
        let micros_per_row: f64 = micros as f64 / rows as f64;
        // TODO: print storage ops
        write!(
            f,
            "{}\n    ==> batches: {}, rows: {}, micros: {}, micros/row: {:.1} ({})",
            &self.description,
            self.batches.load(Ordering::Relaxed),
            rows,
            micros,
            micros_per_row,
            self.storage,
        )
    }
}

pub struct StepProfileMeasurement {
    // note: we don't store &StepProfile to make callers more flexible with immutable lifetime borrows
    start: Option<Instant>,
}

impl StepProfileMeasurement {
    fn new(start: Option<Instant>) -> Self {
        Self { start }
    }

    pub fn end(self, profile: &StepProfile, batches: u64, rows_produced: u64) {
        match self.start {
            None => {}
            Some(start) => {
                let end = Instant::now();
                let duration = end.duration_since(start).as_nanos() as u64;
                let profile_data = profile.data.as_ref().unwrap();
                profile_data.batches.fetch_add(batches, Ordering::Relaxed);
                profile_data.rows.fetch_add(rows_produced, Ordering::Relaxed);
                profile_data.nanos.fetch_add(duration, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageCounters {
    counters: Option<Arc<StorageCountersData>>,
}

impl StorageCounters {
    pub const DISABLED: Self = Self { counters: None };

    fn new_enabled() -> Self {
        Self { counters: Some(Arc::new(StorageCountersData::new())) }
    }

    pub fn increment_raw_advance(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.raw_advance.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_raw_advance(&self) -> Option<u64> {
        self.counters.as_ref().map(|counters| counters.raw_advance.load(Ordering::SeqCst))
    }

    pub fn increment_raw_seek(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.raw_seek.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_raw_seek(&self) -> Option<u64> {
        self.counters.as_ref().map(|counters| counters.raw_seek.load(Ordering::SeqCst))
    }

    pub fn increment_advance_mvcc_visible(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.advance_mvcc_visible.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn increment_advance_mvcc_invisible(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.advance_mvcc_invisible.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn increment_advance_mvcc_deleted(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.advance_mvcc_deleted.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Display for StorageCounters {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.counters.as_ref() {
            None => write!(f, "storage counters disabled"),
            Some(counters) => {
                write!(
                    f,
                    "raw seeks: {}, raw advances: {}, advances mvcc visible: {}, advances mvcc invisible: {}, advances deleted invisible: {}",
                    counters.raw_seek.load(Ordering::SeqCst),
                    counters.raw_advance.load(Ordering::SeqCst),
                    counters.advance_mvcc_visible.load(Ordering::SeqCst),
                    counters.advance_mvcc_invisible.load(Ordering::SeqCst),
                    counters.advance_mvcc_deleted.load(Ordering::SeqCst),
                )
            }
        }
    }
}

#[derive(Debug)]
struct StorageCountersData {
    raw_advance: AtomicU64,
    raw_seek: AtomicU64,
    advance_mvcc_visible: AtomicU64,
    advance_mvcc_invisible: AtomicU64,
    advance_mvcc_deleted: AtomicU64,
}

impl StorageCountersData {
    fn new() -> Self {
        Self {
            raw_advance: AtomicU64::new(0),
            raw_seek: AtomicU64::new(0),
            advance_mvcc_visible: AtomicU64::new(0),
            advance_mvcc_invisible: AtomicU64::new(0),
            advance_mvcc_deleted: AtomicU64::new(0),
        }
    }
}
