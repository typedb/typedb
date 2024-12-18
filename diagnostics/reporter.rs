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

use chrono::{DateTime, Timelike, Utc};
use concurrency::{IntervalRunner, TokioIntervalRunner};
use hyper::{
    header::{HeaderValue, CONNECTION, CONTENT_TYPE},
    Body, Client, Method, Request,
};
use hyper::client::HttpConnector;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use logger::{debug, trace};
use resource::constants::{
    common::SECONDS_IN_MINUTE,
    diagnostics::{DISABLED_REPORTING_FILE_NAME, REPORT_INTERVAL, REPORT_ONCE_DELAY},
};
use resource::constants::diagnostics::{POSTHOG_API_KEY, POSTHOG_BATCH_REPORTING_URI, SERVICE_REPORTING_URI};

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
            REPORT_INTERVAL,
            self.calculate_initial_delay(),
        );
        *self._reporting_job.lock().expect("Expected reporting job exclusive lock acquisition") = Some(reporting_job);
    }

    fn report_once_if_needed(&self) {
        let disabled_reporting_file = self.data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if !disabled_reporting_file.exists() {
            let diagnostics = self.diagnostics.clone();
            let data_directory = self.data_directory.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                if Self::report(diagnostics).await {
                    Self::save_disabled_reporting_file(&data_directory);
                }
            });
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

    async fn report(diagnostics: Arc<Diagnostics>) -> bool {
        println!("REPORT!"); // TODO: Remove
        let service_task = Self::report_diagnostics_service(diagnostics.clone(), SERVICE_REPORTING_URI);
        let posthog_task = Self::report_posthog(diagnostics.clone(), POSTHOG_BATCH_REPORTING_URI, POSTHOG_API_KEY);
        let (is_service_reported, is_posthog_reported) = tokio::join!(service_task, posthog_task);

        diagnostics.take_snapshot();

        // TODO: Redo
        if !is_service_reported {
            println!("Service diagnostics not reported, consider additional logic!");
        }
        if !is_posthog_reported {
            println!("Posthog diagnostics not reported, consider additional logic!");
        }
        is_posthog_reported && is_service_reported
    }

    async fn report_diagnostics_service(diagnostics: Arc<Diagnostics>, reporting_uri: &'static str) -> bool {
        let diagnostics_json = diagnostics.to_service_reporting_json_against_snapshot().to_string();

        // TODO: return
        // let request = hyper::Request::post(reporting_uri)
        //     .header(CONTENT_TYPE, "application/json")
        //     .header(CONNECTION, "close")
        //     .body(Body::from(diagnostics_json))
        //     .expect("Failed to construct the request");
        //
        // match Self::new_https_client().request(request).await {
        //     Ok(response) => {
        //         if response.status().is_success() {
        //             true
        //         } else {
        //             trace!("Failed to push diagnostics to {}: {}", reporting_uri, response.status());
        //             false
        //         }
        //     }
        //     Err(e) => {
        //         trace!("Failed to push diagnostics to {}: {}", reporting_uri, e);
        //         false
        //     }
        // }
        true
    }

    async fn report_posthog(diagnostics: Arc<Diagnostics>, reporting_uri: &'static str, api_key: &'static str) -> bool {
        let events_json = diagnostics.to_posthog_reporting_json_against_snapshot(api_key).to_string();
        println!("Posthog events: {events_json}");

        let request = Request::post(reporting_uri)
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "close")
            .body(Body::from(events_json.to_string()))
            .expect("Failed to construct the request");

        match Self::new_https_client().request(request).await {
            Ok(response) => {
                if response.status().is_success() {
                    println!("POSTHOG success!");
                    true
                } else {
                    println!("Failed to send a posthog batch to {}: {}", reporting_uri, response.status());
                    trace!("Failed to send a posthog batch to {}: {}", reporting_uri, response.status());
                    false
                }
            }
            Err(e) => {
                println!("Failed to send a posthog batch to {}: {}", reporting_uri, e);
                trace!("Failed to send a posthog batch to {}: {}", reporting_uri, e);
                false
            }
        }
    }

    pub async fn shutdown(&self) {
        if !self.is_enabled {
            return;
        }
        // TODO: implement final sending on shutdown
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
        if let Err(e) = fs::remove_file(&disabled_reporting_file) {
            debug!("Failed to delete disabled reporting file: {}", e);
        }
    }
}
