/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use itertools::Itertools;

#[derive(Debug)]
pub struct QueryProfile {
    stage_profiles: RwLock<HashMap<u64, Arc<StageProfile>>>,
    enabled: bool,
}

impl QueryProfile {
    pub fn new(enabled: bool) -> Self {
        Self { stage_profiles: RwLock::new(HashMap::new()), enabled }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn profile_stage(&self, description_fn: impl Fn() -> String, id: u64) -> Arc<StageProfile> {
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
}

impl fmt::Display for QueryProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Query profile[measurements_enabled={}]", self.enabled)?;
        let profiles = self.stage_profiles.read().unwrap();
        for (id, pattern_profile) in profiles.iter().sorted_by_key(|(id, _)| *id) {
            writeln!(f, "  -----")?;
            writeln!(f, "  Stage or Pattern [id={}] - {}", id, &pattern_profile.description)?;
            write!(f, "{}", pattern_profile)?;
        }
        Ok(())
    }
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

    pub(crate) fn extend_or_get(&self, index: usize, description_getter: impl Fn() -> String) -> Arc<StepProfile> {
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
}

impl StepProfile {
    fn new_enabled(description: String) -> Self {
        Self {
            data: Some(StepProfileData {
                description,
                batches: AtomicU64::new(0),
                rows: AtomicU64::new(0),
                nanos: AtomicU64::new(0),
            }),
        }
    }

    fn new_disabled() -> Self {
        Self { data: None }
    }

    pub(crate) fn start_measurement(&self) -> StepProfileMeasurement {
        if self.data.is_some() {
            StepProfileMeasurement::new(Some(Instant::now()))
        } else {
            StepProfileMeasurement::new(None)
        }
    }
}

impl fmt::Display for StepProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rows = self.rows.load(Ordering::Relaxed);
        let micros = Duration::from_nanos(self.nanos.load(Ordering::Relaxed)).as_micros();
        let micros_per_row: f64 = micros as f64 / rows as f64;
        write!(
            f,
            "{}\n    ==> batches: {}, rows: {}, micros: {}, micros/row: {:.1}",
            &self.description,
            self.batches.load(Ordering::Relaxed),
            rows,
            micros,
            micros_per_row,
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

    pub(crate) fn end(self, profile: &StepProfile, batches: u64, rows_produced: u64) {
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
