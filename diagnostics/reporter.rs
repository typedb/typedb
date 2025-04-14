/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use chrono::{Timelike, Utc};
use concurrency::TokioIntervalRunner;
use error::{typedb_error, TypeDBError};
use hyper::{
    client::HttpConnector,
    header::{CONNECTION, CONTENT_TYPE},
    http, Body, Client, Request,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use logger::{debug, trace};
use resource::constants::{
    common::{SECONDS_IN_HOUR, SECONDS_IN_MINUTE},
    diagnostics::{
        DISABLED_REPORTING_FILE_NAME, POSTHOG_API_KEY, POSTHOG_BATCH_REPORTING_URI, REPORT_INITIAL_RETRY_DELAY,
        REPORT_INTERVAL, REPORT_MAX_RETRY_NUM, REPORT_ONCE_DELAY, REPORT_RETRY_DELAY_EXPONENTIAL_MULTIPLIER,
    },
};

use crate::{hash_string_consistently, Diagnostics};

#[derive(Debug)]
pub struct Reporter {
    deployment_id: String,
    diagnostics: Arc<Diagnostics>,
    data_directory: PathBuf,
    is_posthog_enabled: Arc<AtomicBool>,
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
            is_posthog_enabled: Arc::new(AtomicBool::new(is_enabled)),
            _reporting_job: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn may_start(&self) {
        if self.is_posthog_enabled.load(Ordering::Relaxed) {
            Self::delete_disabled_reporting_file_if_exists(&self.data_directory);
            self.schedule_reporting().await;
        } else {
            self.report_once_if_needed();
        }
    }

    async fn schedule_reporting(&self) {
        let is_posthog_enabled = self.is_posthog_enabled.clone();
        let diagnostics = self.diagnostics.clone();

        let reporting_job = TokioIntervalRunner::new_with_initial_delay(
            move || {
                let is_posthog_enabled = is_posthog_enabled.clone();
                let diagnostics = diagnostics.clone();
                async move {
                    Self::report(is_posthog_enabled, false, diagnostics).await;
                }
            },
            REPORT_INTERVAL,
            self.calculate_initial_delay(),
            true,
        );
        *self._reporting_job.lock().expect("Expected reporting job exclusive lock acquisition") = Some(reporting_job);
    }

    fn report_once_if_needed(&self) {
        let disabled_reporting_file = self.data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if !disabled_reporting_file.exists() {
            let is_posthog_enabled = self.is_posthog_enabled.clone();
            let diagnostics = self.diagnostics.clone();
            let data_directory = self.data_directory.clone();

            tokio::spawn(async move {
                tokio::time::sleep(REPORT_ONCE_DELAY).await;
                if Self::report(is_posthog_enabled, true, diagnostics).await {
                    Self::save_disabled_reporting_file(&data_directory);
                }
            });
        }
    }

    async fn report(
        is_posthog_enabled: Arc<AtomicBool>,
        ignore_disabling: bool,
        diagnostics: Arc<Diagnostics>,
    ) -> bool {
        let posthog_result = Self::report_posthog(is_posthog_enabled, ignore_disabling, diagnostics).await;
        posthog_result
    }

    async fn report_posthog(
        is_enabled: Arc<AtomicBool>,
        ignore_disabling: bool,
        diagnostics: Arc<Diagnostics>,
    ) -> bool {
        if !ignore_disabling && !is_enabled.load(Ordering::Relaxed) {
            return false;
        }

        let events_json = diagnostics.to_posthog_reporting_json_against_snapshot(POSTHOG_API_KEY);
        diagnostics.take_snapshot();

        let uri = ReportingEndpoint::Posthog.get_uri();
        let is_reported = match Self::send_request_with_retries(events_json.to_string(), uri).await {
            Ok(is_reported) => is_reported,
            Err(error) => {
                trace!("Posthog reporting got an error. Disabling Posthog reporting...");
                is_enabled.store(false, Ordering::Relaxed);
                Self::report_inner_error(error);
                false
            }
        };

        // The request is sent with retries, and we don't want to lose posthog data. The snapshot
        // is taken right after the json creation, but can be restored to preserve the not sent data
        // for the next reporting action
        if !is_reported {
            trace!("Posthog reporting is not successful. Restoring the snapshot...");
            diagnostics.restore_posthog_snapshot();
        }
        is_reported
    }

    async fn send_request_with_retries(
        request_body: String,
        uri: &'static str,
    ) -> Result<bool, DiagnosticsReporterError> {
        let mut retries_num = 0;
        let mut delay = REPORT_INITIAL_RETRY_DELAY;

        while retries_num < REPORT_MAX_RETRY_NUM {
            if Self::send_request(request_body.clone(), uri).await? {
                return Ok(true);
            }

            trace!("Retrying to send diagnostics data to {} after {:?}...", uri, delay);
            tokio::time::sleep(delay).await;
            retries_num += 1;
            delay *= REPORT_RETRY_DELAY_EXPONENTIAL_MULTIPLIER.pow(retries_num);
        }

        trace!("Max reporting retries reached for {}.", uri);
        Ok(false)
    }

    async fn send_request(body: String, uri: &'static str) -> Result<bool, DiagnosticsReporterError> {
        let request = Request::post(uri)
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "close")
            .body(Body::from(body))
            .map_err(|source| DiagnosticsReporterError::HttpRequestBuilding { source: Arc::new(source) })?;

        match Self::new_https_client()?.request(request).await {
            Ok(response) => {
                if response.status().is_success() {
                    trace!("Successfully sent diagnostics data to {}", uri);
                    Ok(true)
                } else {
                    trace!("Failed to send diagnostics data to {}: {}", uri, response.status());
                    Ok(false)
                }
            }
            Err(e) => {
                trace!("Failed to send diagnostics data to {}: {}", uri, e);
                Ok(false)
            }
        }
    }

    fn new_https_client() -> Result<Client<HttpsConnector<HttpConnector>>, DiagnosticsReporterError> {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|source| DiagnosticsReporterError::HttpsClientBuilding { source: Arc::new(source) })?
            .https_only()
            .enable_http1()
            .build();
        Ok(Client::builder().build::<_, Body>(https))
    }

    fn save_disabled_reporting_file(data_directory: &Path) {
        let disabled_reporting_file = data_directory.join(DISABLED_REPORTING_FILE_NAME);
        if let Err(e) = fs::write(&disabled_reporting_file, Utc::now().to_string()) {
            debug!("Failed to save disabled reporting file: {}", e);
        }
    }

    fn delete_disabled_reporting_file_if_exists(data_directory: &Path) {
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
            report_interval_secs - current_minute * SECONDS_IN_MINUTE + scheduled_minute * SECONDS_IN_MINUTE
        } else {
            (scheduled_minute - current_minute) * SECONDS_IN_MINUTE
        };
        Duration::from_secs(delay_secs)
    }

    fn report_inner_error(error: DiagnosticsReporterError) {
        match error {
            DiagnosticsReporterError::HttpsClientBuilding { .. } => {
                Self::report_inner_error_message_warning(<dyn TypeDBError>::to_string(&error).as_str())
            }
            DiagnosticsReporterError::HttpRequestBuilding { .. } => {
                Self::report_inner_error_message_critical(<dyn TypeDBError>::to_string(&error).as_str())
            }
        }
    }

    fn report_inner_error_message_critical(message: &str) {
        sentry::capture_message(message, sentry::Level::Error);
    }

    fn report_inner_error_message_warning(message: &str) {
        sentry::capture_message(message, sentry::Level::Warning);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum ReportingEndpoint {
    Posthog,
}

impl ReportingEndpoint {
    pub(crate) fn get_uri(&self) -> &'static str {
        match self {
            ReportingEndpoint::Posthog => POSTHOG_BATCH_REPORTING_URI,
        }
    }
}

typedb_error!(
    pub DiagnosticsReporterError(component = "DiagnosticsReporter", prefix = "DIR") {
        HttpsClientBuilding(1, "Error while building a diagnostics reporting https client.", source: Arc<std::io::Error>),
        HttpRequestBuilding(2, "Error while building a diagnostics reporting http request.", source: Arc<http::Error>),
    }
);
