/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct QueryProfile {
    pattern_profiles: RwLock<HashMap<u64, Arc<PatternProfile>>>,
    enabled: bool,
}

impl QueryProfile {
    pub fn new(enabled: bool) -> Self {
        Self { pattern_profiles: RwLock::new(HashMap::new()), enabled }
    }
    
    pub fn profile_pattern(&self, id: u64) -> Arc<PatternProfile> {
        if self.enabled {
            let mut profiles = self.pattern_profiles.read().unwrap();
            if let Some(profile) = profiles.get(&id) {
                profile.clone()
            } else {
                drop(profiles);
                self.pattern_profiles.write().unwrap().insert(id, Arc::new(PatternProfile::new(true))).unwrap()
            }
        } else {
            Arc::new(PatternProfile::new(false))
        }
    }
}

#[derive(Debug)]
pub struct PatternProfile {
    step_profiles: Vec<Arc<StepProfile>>,
    enabled: bool,
}

impl PatternProfile {
    
    fn new(enabled: bool) -> Self {
        Self { step_profiles: Vec::new(), enabled }
    }
    
    fn add_step(&mut self, description_getter: fn() -> String) -> Arc<StepProfile> {
        let profile = if self.enabled {
            StepProfile::new_enabled(description_getter())
        } else {
            StepProfile::new_disabled()
        };
        self.step_profiles.push(Arc::new(profile));
        self.step_profiles.last().unwrap().clone()
    }
}

#[derive(Debug)]
pub struct StepProfile {
    data: Option<StepProfileData>
}

#[derive(Debug)]
struct StepProfileData {
    description: String,
    rows: AtomicU64,
    nanos: AtomicU64,
}

impl StepProfile {
    fn new_enabled(description: String) -> Self {
        Self {
            data: Some(StepProfileData {
                description,
                rows: AtomicU64::new(0),
                nanos: AtomicU64::new(0),
            })
        }
    }
    
    fn new_disabled() -> Self {
        Self { data: None }
    }

    fn start_measurement(&self) -> StepProfileMeasurement<'_> {
        if self.data.is_some() {
            StepProfileMeasurement::new(self, Some(Instant::now()))
        } else {
            StepProfileMeasurement::new(self, None)
        }
    }
}

pub struct StepProfileMeasurement<'a> {
    profile: &'a StepProfile,
    start: Option<Instant>,
}

impl<'a> StepProfileMeasurement<'a> {
    fn new(step_profile: &'a StepProfile, start: Option<Instant>) -> Self {
        Self {
            profile: step_profile,
            start,
        }
    }

    fn end(self, rows_produced: u64) {
        match self.start {
            None => {}
            Some(start) => {
                let end = Instant::now();
                let duration = end.duration_since(start).as_nanos() as u64;
                let profile_data = self.profile.data.as_ref().unwrap();
                profile_data.rows.fetch_add(rows_produced, Ordering::Relaxed);
                profile_data.nanos.fetch_add(duration, Ordering::Relaxed);
            }
        }
    }
}
