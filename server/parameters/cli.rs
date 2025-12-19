/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

use clap::Parser;
use resource::constants::server::DISTRIBUTION_INFO;

/// TypeDB CE usage
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
#[clap(version = DISTRIBUTION_INFO.version)]
pub struct CLIArgs {
    /// Path to config file
    #[arg(long = "config")]
    pub config_file_override: Option<String>,

    /// Server serving host and port (e.g., 0.0.0.0:1729)
    #[arg(long = "server.address")]
    pub server_address: Option<String>,

    /// Server connection host and port (e.g., 127.0.0.1:1729)
    /// This address overrides the serving address in the server info shared through APIs and other outputs
    /// It is a reference address, which means that its resolved IP address is checked for correctness,
    /// but the form specified here (even if it's an alias) will not be changed.
    #[arg(long = "server.connection-address")]
    pub server_connection_address: Option<String>,

    /// Enable/disable HTTP endpoint
    #[arg(long = "server.http.enabled")]
    pub server_http_enabled: Option<bool>,

    /// HTTP endpoint host and port (e.g., 0.0.0.0:8000)
    #[arg(long = "server.http.address")]
    pub server_http_address: Option<String>,

    /// The amount of seconds generated authentication tokens will remain valid, specified in seconds.
    /// Use smaller values for better security and bigger values for better authentication performance and convenience
    /// (min: 1 second, max: 1 year)
    #[arg(long = "server.authentication.token-expiration-seconds")]
    pub server_authentication_token_expiration_seconds: Option<u64>,

    /// Enable/disable in-flight encryption. Specify to enable, or leave out to disable
    #[arg(long = "server.encryption.enabled", action=clap::ArgAction::Set)]
    pub server_encryption_enabled: Option<bool>,

    /// Encryption certificate in PEM format. Must be supplied if encryption is enabled
    #[arg(long = "server.encryption.certificate", value_name = "FILE")]
    pub server_encryption_certificate: Option<String>,

    /// Encryption certificate key. Must be supplied if encryption is enabled
    #[arg(long = "server.encryption.certificate-key", value_name = "FILE")]
    pub server_encryption_certificate_key: Option<String>,

    /// Encryption CA in PEM format
    #[arg(long = "server.encryption.ca-certificate", value_name = "FILE")]
    pub server_encryption_ca_certificate: Option<String>,

    /// Path to the data directory
    #[arg(long = "storage.data-directory", value_name = "DIR")]
    pub storage_data_directory: Option<String>,

    /// Path to the log directory
    #[arg(long = "logging.directory")]
    pub logging_directory: Option<String>,

    /// Enable usage metrics reporting
    #[arg(long = "diagnostics.reporting.metrics")]
    pub diagnostics_reporting_metrics: Option<bool>, // used to be `statistics` in 2.x

    /// Enable critical error reporting
    #[arg(long = "diagnostics.reporting.errors")]
    pub diagnostics_reporting_errors: Option<bool>,

    /// Enable a diagnostics monitoring HTTP endpoint
    #[arg(long = "diagnostics.monitoring.enabled")]
    pub diagnostics_monitoring_enabled: Option<bool>,

    /// Port on which to expose the diagnostics monitoring endpoint
    #[arg(long = "diagnostics.monitoring.port")]
    pub diagnostics_monitoring_port: Option<u16>,

    /// Enable development mode for testing setups. Note that running TypeDB in development mode
    /// may result in error reporting limitations (obstructing maintenance and support), additional
    /// logging, restricted functionalities, and reduced performance
    #[arg(long = "development-mode.enabled", hide = true)]
    pub development_mode_enabled: Option<bool>,
}

impl CLIArgs {
    pub fn resolve_path_from_pwd(path: &PathBuf) -> PathBuf {
        std::env::current_dir().expect("Could not read working directory").join(path)
    }
}
