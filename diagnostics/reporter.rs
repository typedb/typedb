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
    time::{Duration, Instant, UNIX_EPOCH},
};

use chrono::{DateTime, Timelike, Utc};
use concurrency::{IntervalRunner, TokioIntervalRunner};
use hyper::{
    client::HttpConnector,
    header::{HeaderValue, CONNECTION, CONTENT_TYPE},
    Body, Client, Method, Request,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use logger::{debug, trace};
use resource::constants::{
    common::SECONDS_IN_MINUTE,
    diagnostics::{
        DISABLED_REPORTING_FILE_NAME, POSTHOG_API_KEY, POSTHOG_BATCH_REPORTING_URI, REPORT_INITIAL_RETRY_DELAY,
        REPORT_INTERVAL, REPORT_MAX_RETRY_NUM, REPORT_ONCE_DELAY, REPORT_RETRY_DELAY_EXPONENTIAL_MULTIPLIER,
        SERVICE_REPORTING_URI,
    },
};

use crate::{hash_string_consistently, Diagnostics};

#[derive(Debug)]
pub struct Reporter {
    deployment_id: String,
    diagnostics: Arc<Diagnostics>,
    data_directory: PathBuf,
    is_enabled: bool,
    _reporting_job: Arc<Mutex<Option<TokioIntervalRunner>>>,
}

impl Reporter {
    pub(crate) fn new(
        deployment_id: String,
        diagnostics: Arc<Diagnostics>,
        data_directory: PathBuf,
        is_enabled: bool,
    ) -> Self {
        Self {
            deployment_id,
            diagnostics,
            data_directory,
            is_enabled,
            _reporting_job: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn may_start(&self) {
        if self.is_enabled {
            Self::delete_disabled_reporting_file_if_exists(&self.data_directory);
            self.schedule_reporting().await;
        } else {
            self.report_once_if_needed();
        }
    }

    async fn schedule_reporting(&self) {
        let diagnostics = self.diagnostics.clone();

        let reporting_job = TokioIntervalRunner::new_with_initial_delay(
            move || {
                let diagnostics = diagnostics.clone();
                async move {
                    Self::report(diagnostics).await;
                }
            },
            Duration::from_secs(15),
            Duration::from_secs(15),
            true,
        );
        *self._reporting_job.lock().expect("Expected reporting job exclusive lock acquisition") = Some(reporting_job);
    }

    fn report_once_if_needed(&self) {
        let disabled_reporting_file = self.data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if !disabled_reporting_file.exists() {
            let diagnostics = self.diagnostics.clone();
            let data_directory = self.data_directory.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(20)).await; // TODO: Return constant
                if Self::report(diagnostics).await {
                    Self::save_disabled_reporting_file(&data_directory);
                }
            });
        }
    }

    async fn report(diagnostics: Arc<Diagnostics>) -> bool {
        let service_task = Self::report_diagnostics_service(diagnostics.clone());
        let posthog_task = Self::report_posthog(diagnostics.clone());
        let (is_service_reported, is_posthog_reported) = tokio::join!(service_task, posthog_task);
        is_service_reported && is_posthog_reported
    }

    async fn report_diagnostics_service(diagnostics: Arc<Diagnostics>) -> bool {
        // let diagnostics_json = diagnostics.to_service_reporting_json_against_snapshot();
        // let is_reported = Self::send_request(
        //     diagnostics_json.to_string(),
        //     ReportingEndpoint::DiagnosticsService.get_uri(),
        // ).await;
        //
        // // The request is sent once, so it's fine to take a snapshot lossy with a small delay
        // if is_reported {
        //     println!("Service reporting is successful. Taking a snapshot...");
        //     trace!("Service reporting is successful. Taking a snapshot...");
        //     diagnostics.take_service_snapshot();
        // }
        // is_reported
        true // TODO: return the main code
    }

    async fn report_posthog(diagnostics: Arc<Diagnostics>) -> bool {
        let events_json = diagnostics.to_posthog_reporting_json_against_snapshot(POSTHOG_API_KEY);
        diagnostics.take_posthog_snapshot();
        println!("Posthog events: {events_json}");

        let is_reported =
            Self::send_request_with_retries(events_json.to_string(), ReportingEndpoint::PostHog.get_uri()).await;

        // The request is sent with retries, and we don't want to lose posthog data. The snapshot
        // is taken right after the json creation, but can be restored to preserve the not sent data
        // for the next reporting action
        if !is_reported {
            println!("PostHog reporting is not successful. Restoring the snapshot...");
            trace!("PostHog reporting is not successful. Restoring the snapshot...");
            diagnostics.restore_posthog_snapshot();
        }
        is_reported
    }

    async fn send_request_with_retries(request_body: String, uri: &'static str) -> bool {
        let mut retries_num = 0;
        let mut delay = REPORT_INITIAL_RETRY_DELAY;

        while retries_num < REPORT_MAX_RETRY_NUM {
            if Self::send_request(request_body.clone(), uri).await {
                return true;
            }

            println!("Retrying to send diagnostics data to {} after {:?}...", uri, delay);
            trace!("Retrying to send diagnostics data to {} after {:?}...", uri, delay);
            tokio::time::sleep(delay).await;
            retries_num += 1;
            delay *= REPORT_RETRY_DELAY_EXPONENTIAL_MULTIPLIER.pow(retries_num);
        }

        println!("Max reporting retries reached for {}.", uri);
        trace!("Max reporting retries reached for {}.", uri);
        false
    }

    async fn send_request(body: String, uri: &'static str) -> bool {
        let request = Request::post(uri)
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "close")
            .body(Body::from(body))
            .expect("Failed to construct the request");

        match Self::new_https_client().request(request).await {
            Ok(response) => {
                if response.status().is_success() {
                    println!("Successfully sent diagnostics data to {}", uri);
                    trace!("Successfully sent diagnostics data to {}", uri);
                    true
                } else {
                    println!("Failed to send diagnostics data to {}: {}", uri, response.status());
                    trace!("Failed to send diagnostics data to {}: {}", uri, response.status());
                    false
                }
            }
            Err(e) => {
                println!("Failed to send diagnostics data to {}: {}", uri, e);
                trace!("Failed to send diagnostics data to {}: {}", uri, e);
                false
            }
        }
    }

    fn new_https_client() -> Client<HttpsConnector<HttpConnector>> {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("No native root CA certificates found")
            .https_only()
            .enable_http1()
            .build();
        Client::builder().build::<_, Body>(https)
    }

    fn save_disabled_reporting_file(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::write(&disabled_reporting_file, Utc::now().to_string()) {
            debug!("Failed to save disabled reporting file: {}", e);
        }
    }

    fn delete_disabled_reporting_file_if_exists(data_directory: &PathBuf) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if fs::exists(&disabled_reporting_file).unwrap_or(false) {
            if let Err(e) = fs::remove_file(&disabled_reporting_file) {
                debug!("Failed to delete disabled reporting file: {}", e);
            }
        }
    }

    fn calculate_initial_delay(&self) -> Duration {
        let report_interval_secs = REPORT_INTERVAL.as_secs();
        assert!(report_interval_secs == 3600, "Modify the algorithm if you change the interval!");

        let current_minute = Utc::now().minute() as u64;
        let scheduled_minute =
            hash_string_consistently(&self.deployment_id) % (report_interval_secs / SECONDS_IN_MINUTE);

        let delay_secs = if current_minute > scheduled_minute {
            report_interval_secs - (current_minute + scheduled_minute) * SECONDS_IN_MINUTE
        } else {
            (scheduled_minute - current_minute) * SECONDS_IN_MINUTE
        };
        Duration::from_secs(delay_secs)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum ReportingEndpoint {
    DiagnosticsService,
    PostHog,
}

impl ReportingEndpoint {
    pub(crate) fn get_uri(&self) -> &'static str {
        match self {
            ReportingEndpoint::DiagnosticsService => SERVICE_REPORTING_URI,
            ReportingEndpoint::PostHog => POSTHOG_BATCH_REPORTING_URI,
        }
    }
}
