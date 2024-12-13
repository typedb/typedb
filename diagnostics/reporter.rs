/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cell::RefCell,
    collections::HashMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Local, Timelike};
use concurrency::IntervalRunner;
use logger::{debug, trace};
use reqwest::{
    blocking::{Client, Response},
    header::{HeaderValue, CONNECTION, CONTENT_TYPE},
};
use resource::constants::diagnostics::{
    DATABASE_METRICS_UPDATE_INTERVAL, DISABLED_REPORTING_FILE_NAME, REPORT_INTERVAL,
};

use crate::Diagnostics;

#[derive(Debug)]
pub struct Reporter {
    deployment_id: String,
    diagnostics: Arc<Diagnostics>,
    reporting_uri: &'static str,
    data_directory: PathBuf,
    is_enabled: bool,
    _reporting_job: Arc<Option<IntervalRunner>>,
}

impl Reporter {
    pub(crate) fn new(
        deployment_id: String,
        diagnostics: Arc<Diagnostics>,
        reporting_uri: &'static str,
        data_directory: PathBuf,
        is_enabled: bool,
    ) -> Self {
        Self { deployment_id, diagnostics, reporting_uri, data_directory, is_enabled, _reporting_job: Arc::new(None) }
    }

    pub fn may_start(&self) {
        if self.is_enabled {
            Self::delete_disabled_reporting_file_if_exists(&self.data_directory);
            self.schedule_reporting();
        } else {
            self.report_once_if_needed();
        }
    }

    fn schedule_reporting(&self) {
        let diagnostics = self.diagnostics.clone();
        let reporting_uri = self.reporting_uri;

        let reporting_job = IntervalRunner::new_with_initial_delay(
            move || {
                Self::report(diagnostics.clone(), reporting_uri);
            },
            REPORT_INTERVAL,
            self.calculate_initial_delay(),
        );
        let _ = Arc::get_mut(&mut Arc::clone(&self._reporting_job)).map(|opt| *opt = Some(reporting_job));
    }

    fn report_once_if_needed(&self) {
        let disabled_reporting_file = self.data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if !disabled_reporting_file.exists() {
            let diagnostics = self.diagnostics.clone();
            let reporting_uri = self.reporting_uri;
            let data_directory = self.data_directory.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_secs(3600)); // TODO: Make a constant
                if Self::report(diagnostics, reporting_uri) {
                    Self::save_disabled_reporting_file(&data_directory);
                }
            });
        }
    }

    fn calculate_initial_delay(&self) -> Duration {
        let report_interval_secs = REPORT_INTERVAL.as_secs();
        assert!(report_interval_secs == 3600, "Modify the algorithm if you change the interval!");

        let current_minute = Local::now().minute() as u64;

        let mut deployment_id_hasher = DefaultHasher::new();
        self.deployment_id.hash(&mut deployment_id_hasher);
        let scheduled_minute = deployment_id_hasher.finish() % report_interval_secs / 60;

        let delay_secs = if current_minute > scheduled_minute {
            report_interval_secs - (current_minute + scheduled_minute) * 60
        } else {
            (scheduled_minute - current_minute) * 60
        };

        Duration::from_secs(delay_secs)
    }

    fn report(diagnostics: Arc<Diagnostics>, reporting_uri: &'static str) -> bool {
        let diagnostics_json = diagnostics.to_reporting_json_against_snapshot().to_string();
        diagnostics.take_snapshot();

        let client = Client::new();
        let result = client
            .post(reporting_uri)
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "close")
            .body(diagnostics_json)
            .send();

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    true
                } else {
                    trace!("Failed to push diagnostics to {}: {}", reporting_uri, response.status());
                    false
                }
            }
            Err(e) => {
                trace!("Failed to push diagnostics to {}: {}", reporting_uri, e);
                false
            }
        }
    }

    fn save_disabled_reporting_file(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::write(&disabled_reporting_file, Local::now().to_string()) {
            debug!("Failed to save disabled reporting file: {}", e);
        }
    }

    fn delete_disabled_reporting_file_if_exists(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::remove_file(&disabled_reporting_file) {
            debug!("Failed to delete disabled reporting file: {}", e);
        }
    }
}
