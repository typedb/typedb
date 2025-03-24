/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::PathBuf, str::FromStr, time::Duration};

use clap::{ArgAction, Parser};
use resource::constants::server::{
    DEFAULT_ADDRESS, DEFAULT_AUTHENTICATION_TOKEN_TTL_SECONDS, DEFAULT_HTTP_ADDRESS, MONITORING_DEFAULT_PORT, VERSION,
};

use crate::parameters::config::{AuthenticationConfig, Config, DiagnosticsConfig, EncryptionConfig};

/// TypeDB CE usage
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
#[clap(version = VERSION)]
pub struct CLIArgs {
    /// Server host and port (e.g., 0.0.0.0:1729)
    #[arg(long = "server.address", default_value_t = DEFAULT_ADDRESS.to_string())]
    pub server_address: String,

    /// Enable/disable HTTP endpoint
    #[arg(long = "server.http.enable", default_value_t = true, action=ArgAction::Set)]
    pub server_http_enable: bool,

    /// HTTP endpoint host and port (e.g., 0.0.0.0:8000)
    #[arg(long = "server.http.address", default_value_t = DEFAULT_HTTP_ADDRESS.to_string())]
    pub server_http_address: String,

    /// The amount of seconds generated authentication tokens will remain valid, specified in seconds.
    /// Use smaller values for better security and bigger values for better authentication performance and convenience
    /// (min: 1 second, max: 1 year).
    #[arg(
        long = "server.authentication.token_ttl_seconds",
        default_value_t = DEFAULT_AUTHENTICATION_TOKEN_TTL_SECONDS,
    )]
    pub server_authentication_token_ttl_seconds: u64,

    /// Enable/disable in-flight encryption. Specify to enable, or leave out to disable
    #[arg(long = "server.encryption.enabled")]
    pub server_encryption_enabled: bool,

    /// Encryption certificate in PEM format. Must be supplied if encryption is enabled
    #[arg(long = "server.encryption.cert", value_name = "FILE")]
    pub server_encryption_cert: Option<String>,

    /// Encryption certificate key. Must be supplied if encryption is enabled
    #[arg(long = "server.encryption.cert-key", value_name = "FILE")]
    pub server_encryption_cert_key: Option<String>,

    /// Encryption CA in PEM format.
    #[arg(long = "server.encryption.root-ca", value_name = "FILE")]
    pub server_encryption_root_ca: Option<String>,

    /// Path to the data directory
    #[arg(long = "storage.data", value_name = "DIR")]
    pub storage_data: Option<String>,

    /// Enable usage metrics reporting
    #[arg(long = "diagnostics.reporting.metrics", default_value_t = true, action=ArgAction::Set)]
    pub diagnostics_reporting_metrics: bool, // used to be `statistics` in 2.x

    /// Enable critical error reporting
    #[arg(long = "diagnostics.reporting.errors", default_value_t = true, action=ArgAction::Set)]
    pub diagnostics_reporting_errors: bool,

    /// Enable a diagnostics monitoring HTTP endpoint
    #[arg(long = "diagnostics.monitoring.enable", default_value_t = true, action=ArgAction::Set)]
    pub diagnostics_monitoring_enable: bool,

    /// Port on which to expose the diagnostics monitoring endpoint
    #[arg(long = "diagnostics.monitoring.port", default_value_t = MONITORING_DEFAULT_PORT)]
    pub diagnostics_monitoring_port: u16,

    /// Enable development mode for testing setups. Note that running TypeDB in development mode
    /// may result in error reporting limitations (obstructing maintenance and support), additional
    /// logging, restricted functionalities, and reduced performance
    #[arg(long = "development-mode.enabled", hide = true)]
    pub development_mode_enabled: bool,
}

impl CLIArgs {
    pub fn to_config(&self) -> Config {
        let authentication_config = AuthenticationConfig {
            token_expiration_seconds: Duration::from_secs(self.server_authentication_token_ttl_seconds),
        };
        let encryption_config = EncryptionConfig {
            enabled: self.server_encryption_enabled,
            cert: self.server_encryption_cert.clone().map(|path| PathBuf::from_str(path.as_str()).unwrap()),
            cert_key: self.server_encryption_cert_key.clone().map(|path| PathBuf::from_str(path.as_str()).unwrap()),
            root_ca: self.server_encryption_root_ca.clone().map(|path| PathBuf::from_str(path.as_str()).unwrap()),
        };
        let diagnostics_config = DiagnosticsConfig {
            is_reporting_error_enabled: self.diagnostics_reporting_errors,
            is_reporting_metric_enabled: self.diagnostics_reporting_metrics,
            is_monitoring_enabled: self.diagnostics_monitoring_enable,
            monitoring_port: self.diagnostics_monitoring_port,
        };

        let mut config = Config::new(self.server_address.clone())
            .authentication(authentication_config)
            .encryption(encryption_config)
            .diagnostics(diagnostics_config)
            .development_mode(self.development_mode_enabled);
        if self.server_http_enable {
            config = config.server_http_address(&self.server_http_address);
        }
        if let Some(data_directory) = self.storage_data.as_ref() {
            config =
                config.data_directory(&PathBuf::from_str(data_directory.as_str()).expect("Expected data directory"));
        }
        config.build()
    }
}
