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

use resource::constants::server::{
    DEFAULT_ADDRESS, DEFAULT_AUTHENTICATION_TOKEN_TTL, DEFAULT_DATA_DIR, DEFAULT_LOG_DIR, MONITORING_DEFAULT_PORT,
};
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use crate::parameters::ConfigError;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub server: ServerConfig,
    pub(crate) storage: StorageConfig,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    pub logging: LoggingConfig,
    #[serde(rename = "development-mode.enabled", default)]
    pub development_mode: DevelopmentModeConfig,
}

impl Config {
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
        serde_yaml2::from_str::<Config>(config.as_str()).map_err(|source| ConfigError::ErrorParsingYaml { source })
    }

    pub(crate) fn validate_and_finalise(&mut self) -> Result<(), ConfigError> {
        let encryption = &self.server.encryption;
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
        self.storage.data_directory = Self::resolve_path_from_executable(&self.storage.data_directory);
        self.logging.directory = Self::resolve_path_from_executable(&self.logging.directory);
        self.development_mode.enabled = self.development_mode.enabled | Self::IS_DEVELOPMENT_MODE_FORCED;
        Ok(())
    }

    pub fn resolve_path_from_executable(path: &PathBuf) -> PathBuf {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().expect("Expected parent directory of: {path}").to_path_buf())
            .unwrap_or(std::env::current_dir().expect("Expected access to the current directory"));
        // if path is absolute, join will just return path
        typedb_dir_or_current.join(path)
    }

    fn is_development_mode_default() -> bool {
        false
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub(crate) address: String,
    pub(crate) http_enabled: bool,
    pub(crate) http_address: String,
    pub(crate) authentication: AuthenticationConfig,
    pub(crate) encryption: EncryptionConfig,
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
    pub(crate) data_directory: PathBuf,
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

#[derive(Debug, Default)]
pub struct ConfigBuilderForTests {
    server_address: Option<String>,
    server_http_address: Option<String>,
    authentication: Option<AuthenticationConfig>,
    encryption: Option<EncryptionConfig>,
    diagnostics: Option<DiagnosticsConfig>,
    data_directory: Option<PathBuf>,
    log_directory: Option<PathBuf>,
    is_development_mode: Option<bool>,
}

impl ConfigBuilderForTests {
    pub fn server_address(mut self, address: impl Into<String>) -> Self {
        self.server_address = Some(address.into());
        self
    }

    pub fn server_http_address(mut self, address: impl Into<String>) -> Self {
        self.server_http_address = Some(address.into());
        self
    }

    pub fn authentication(mut self, config: AuthenticationConfig) -> Self {
        self.authentication = Some(config);
        self
    }

    pub fn encryption(mut self, config: EncryptionConfig) -> Self {
        self.encryption = Some(config);
        self
    }

    pub fn diagnostics(mut self, config: DiagnosticsConfig) -> Self {
        self.diagnostics = Some(config);
        self
    }

    pub fn data_directory(mut self, path: impl AsRef<Path>) -> Self {
        self.data_directory = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn development_mode(mut self, is_enabled: bool) -> Self {
        self.is_development_mode = Some(is_enabled);
        self
    }

    pub fn build(self) -> Result<Config, ConfigError> {
        let data_directory = self.data_directory.unwrap_or(DEFAULT_DATA_DIR.into()).into();
        let log_directory = self.log_directory.unwrap_or(DEFAULT_LOG_DIR.into());
        let development_mode = DevelopmentModeConfig {
            enabled: Config::IS_DEVELOPMENT_MODE_FORCED || self.is_development_mode.unwrap_or(false),
        };
        let mut config = Config {
            server: ServerConfig {
                address: self.server_address.unwrap_or_else(|| DEFAULT_ADDRESS.to_string()),
                http_address: self.server_http_address.clone().unwrap_or("".to_owned()),
                http_enabled: self.server_http_address.is_some(),
                authentication: self.authentication.unwrap_or_else(AuthenticationConfig::default),
                encryption: self.encryption.unwrap_or_else(EncryptionConfig::default),
            },
            storage: StorageConfig { data_directory: data_directory },
            diagnostics: DiagnosticsConfig::default(),
            logging: LoggingConfig { directory: log_directory },
            development_mode,
        };
        config.validate_and_finalise()?;
        Ok(config)
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::parameters::{cli::CLIArgs, config::Config, ConfigError};

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
        let mut config = Config::from_file(yaml)?;
        let cli_args: CLIArgs = CLIArgs::parse_from(args_with_binary_infront);
        cli_args.override_config(&mut config)?;
        Ok(config)
    }

    #[test]
    fn server_toml_parser_properly() {
        assert!(load_and_parse(config_path(), vec![]).is_ok());
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
            assert!(
                config.server.encryption.enabled == false
                    && config.server.encryption.certificate_key.is_none()
                    && config.server.encryption.certificate.is_none()
            ); // Test is bad if this fails
        }

        {
            let args = vec!["--server.encryption.enabled", "true"];
            assert!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
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
            assert!(load_and_parse(config_path(), args).is_ok());
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.certificate", "somecert.pem"];
            assert!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.certificate-key", "somekey"];
            assert!(matches!(load_and_parse(config_path(), args), Err(ConfigError::ValidationError { .. })));
        }
    }
}
