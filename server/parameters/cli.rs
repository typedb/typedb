/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::net::SocketAddr;
use clap::{ArgAction, Parser};
use resource::constants::server::MONITORING_DEFAULT_PORT;

/// TypeDB Core usage
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLIArgs {

    /// Server host and port, eg., 0.0.0.0:1729
    #[arg(long = "server.address")]
    pub server_address: Option<String>,

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
