/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

use resource::constants::server::{DEFAULT_AUTHENTICATION_TOKEN_TTL, MONITORING_DEFAULT_PORT};
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use crate::parameters::{cli::CLIArgs, ConfigError};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    pub logging: LoggingConfig,
    #[serde(rename = "development-mode.enabled", default)]
    pub development_mode: DevelopmentModeConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub address: String,
    pub http: HttpEndpointConfig,
    pub authentication: AuthenticationConfig,
    pub encryption: EncryptionConfig,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HttpEndpointConfig {
    pub enabled: bool,
    pub address: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AuthenticationConfig {
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "token-expiration-seconds")]
    pub token_expiration: Duration,
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self { token_expiration: DEFAULT_AUTHENTICATION_TOKEN_TTL }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub certificate: Option<PathBuf>,
    pub certificate_key: Option<PathBuf>,
    pub ca_certificate: Option<PathBuf>,
}

impl EncryptionConfig {
    pub fn disabled() -> Self {
        Self { enabled: false, certificate: None, certificate_key: None, ca_certificate: None }
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct StorageConfig {
    pub data_directory: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DiagnosticsConfig {
    pub reporting: Reporting,
    pub monitoring: Monitoring,
}

impl DiagnosticsConfig {
    pub fn enabled() -> Self {
        Self {
            reporting: Reporting { report_errors: true, report_metrics: true },
            monitoring: Monitoring { enabled: true, port: MONITORING_DEFAULT_PORT },
        }
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self::enabled()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Reporting {
    #[serde(rename = "errors")]
    pub report_errors: bool,
    #[serde(rename = "metrics")]
    pub report_metrics: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Monitoring {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoggingConfig {
    pub directory: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DevelopmentModeConfig {
    pub enabled: bool,
}

impl Default for DevelopmentModeConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}

macro_rules! override_config {
    ($($target:expr => $field:expr;)*) => {
        $( if let Some(value) = $field {
            $target = value;
        }
        )*
    }
}

#[derive(Debug)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    #[cfg(feature = "published")]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = false;
    #[cfg(not(feature = "published"))]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = true;

    pub fn from_file(path: PathBuf) -> Result<Self, ConfigError> {
        let mut config = String::new();
        let resolved_path = Self::resolve_path_from_executable(&path);
        File::open(resolved_path.clone())
            .map_err(|source| ConfigError::ErrorReadingConfigFile { source, path: resolved_path.clone() })?
            .read_to_string(&mut config)
            .map_err(|source| ConfigError::ErrorReadingConfigFile { source, path })?;
        serde_yaml2::from_str::<Config>(config.as_str())
            .map_err(|source| ConfigError::ErrorParsingYaml { source })
            .map(|config| Self { config })
    }

    pub fn override_with_cliargs(&mut self, cliargs: CLIArgs) {
        let CLIArgs {
            config_file_override: _,
            server_address,
            server_http_enabled,
            server_http_address,
            server_authentication_token_ttl_seconds,
            server_encryption_enabled,
            server_encryption_certificate,
            server_encryption_certificate_key,
            server_encryption_ca_certificate,
            storage_data_directory,
            logging_directory,
            diagnostics_reporting_metrics,
            diagnostics_reporting_errors,
            diagnostics_monitoring_enabled,
            diagnostics_monitoring_port,
            development_mode_enabled,
        } = cliargs;
        let Self { config } = self;
        override_config! {
            config.server.address => server_address;
            config.server.http.enabled => server_http_enabled;
            config.server.http.address => server_http_address;
            config.server.authentication.token_expiration => server_authentication_token_ttl_seconds.map(|secs| Duration::new(secs, 0));

            config.server.encryption.enabled => server_encryption_enabled;
            config.server.encryption.certificate => server_encryption_certificate.map(|cert| Some(cert.into()));
            config.server.encryption.certificate_key => server_encryption_certificate_key.map(|cert| Some(cert.into()));
            config.server.encryption.ca_certificate => server_encryption_ca_certificate.map(|cert| Some(cert.into()));

            config.storage.data_directory => storage_data_directory.map(|p| CLIArgs::resolve_path_from_pwd(&p.into()));
            config.logging.directory => logging_directory.map(|p| CLIArgs::resolve_path_from_pwd(&p.into()));

            config.diagnostics.reporting.report_metrics => diagnostics_reporting_metrics;
            config.diagnostics.reporting.report_errors => diagnostics_reporting_errors;
            config.diagnostics.monitoring.enabled => diagnostics_monitoring_enabled;
            config.diagnostics.monitoring.port => diagnostics_monitoring_port;

            config.development_mode.enabled => development_mode_enabled;
        }
    }

    pub fn build(self) -> Result<Config, ConfigError> {
        let Self { mut config } = self;
        let encryption = &config.server.encryption;
        if encryption.enabled && encryption.certificate.is_none() {
            return Err(ConfigError::ValidationError {
                message: "Server encryption was enabled, but certificate was not configured.",
            });
        }
        if encryption.enabled && encryption.certificate_key.is_none() {
            return Err(ConfigError::ValidationError {
                message: "Server encryption was enabled, but certificate key was not configured.",
            });
        }
        // finalise:
        config.storage.data_directory = Self::resolve_path_from_executable(&config.storage.data_directory);
        config.logging.directory = Self::resolve_path_from_executable(&config.logging.directory);
        config.development_mode.enabled = config.development_mode.enabled | Self::IS_DEVELOPMENT_MODE_FORCED;
        Ok(config)
    }

    pub fn resolve_path_from_executable(path: &PathBuf) -> PathBuf {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().expect("Expected parent directory of: {path}").to_path_buf())
            .unwrap_or(std::env::current_dir().expect("Expected access to the current directory"));
        // if path is absolute, join will just return path
        typedb_dir_or_current.join(path)
    }

    // Overrides
    pub fn server_address(mut self, address: impl Into<String>) -> Self {
        self.config.server.address = address.into();
        self
    }

    pub fn server_http_enabled(mut self, enabled: bool) -> Self {
        self.config.server.http.enabled = enabled;
        self
    }

    pub fn server_http_address(mut self, address: impl Into<String>) -> Self {
        self.config.server.http.address = address.into();
        self
    }

    pub fn authentication(mut self, config: AuthenticationConfig) -> Self {
        self.config.server.authentication = config;
        self
    }

    pub fn encryption(mut self, config: EncryptionConfig) -> Self {
        self.config.server.encryption = config;
        self
    }

    pub fn diagnostics(mut self, config: DiagnosticsConfig) -> Self {
        self.config.diagnostics = config;
        self
    }

    pub fn data_directory(mut self, path: impl AsRef<Path>) -> Self {
        self.config.storage.data_directory = path.as_ref().to_path_buf();
        self
    }

    pub fn development_mode(mut self, is_enabled: bool) -> Self {
        self.config.development_mode.enabled = is_enabled;
        self
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;

    use assert as assert_true;
    use clap::Parser;

    use crate::parameters::{
        cli::CLIArgs,
        config::{Config, ConfigBuilder},
        ConfigError,
    };

    fn config_path() -> PathBuf {
        #[cfg(feature = "bazel")]
        return std::env::current_dir().unwrap().join("server/config.yml");
        #[cfg(not(feature = "bazel"))]
        return std::env::current_dir().unwrap().join("config.yml");
    }

    fn load_and_parse(yaml: PathBuf, args: Vec<&str>) -> Result<Config, ConfigError> {
        let mut args_with_binary_infront = Vec::with_capacity(args.len() + 1);
        args_with_binary_infront.push("dummy");
        args_with_binary_infront.extend(args);
        let mut config = ConfigBuilder::from_file(yaml)?;
        let cli_args: CLIArgs = CLIArgs::parse_from(args_with_binary_infront);
        config.override_with_cliargs(cli_args);
        config.build()
    }

    #[test]
    fn server_toml_parser_properly() {
        assert_true!(load_and_parse(config_path(), vec![]).is_ok());
    }

    #[test]
    fn fields_can_be_overridden() {
        let set_to = "10.9.8.7:1234";
        let result = load_and_parse(config_path(), vec!["--server.address", set_to]).unwrap();
        assert_eq!(result.server.address.as_str(), set_to);
    }

    #[test]
    fn enabling_encryption_without_setting_cert_and_key_is_flagged() {
        {
            // Check pre-conditions
            let config = load_and_parse(config_path(), vec![]).unwrap();
            assert_true!(
                config.server.encryption.enabled == false
                    && config.server.encryption.certificate_key.is_none()
                    && config.server.encryption.certificate.is_none()
            ); // Test is bad if this fails
        }

        {
            let args = vec!["--server.encryption.enabled", "true"];
            assert_true!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
        }

        {
            let args = vec![
                "--server.encryption.enabled",
                "true",
                "--server.encryption.certificate-key",
                "somekey",
                "--server.encryption.certificate",
                "somecert.pem",
            ];
            assert_true!(load_and_parse(config_path(), args).is_ok());
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.certificate", "somecert.pem"];
            assert_true!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.certificate-key", "somekey"];
            assert_true!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
        }
    }
}
