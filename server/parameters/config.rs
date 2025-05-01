/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use resource::constants::server::{DEFAULT_ADDRESS, DEFAULT_AUTHENTICATION_TOKEN_TTL, DEFAULT_DATA_DIR, DEFAULT_LOG_DIR, MONITORING_DEFAULT_PORT};
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use crate::parameters::ConfigError;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub(crate) storage: StorageConfig,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    pub logging: LoggingConfig,
    #[serde(default="Config::is_development_mode_default")]
    pub is_development_mode: bool,
}

impl Config {
    #[cfg(feature = "published")]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = false;
    #[cfg(not(feature = "published"))]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = true;

    pub fn new(server_address: impl Into<String>) -> ConfigBuilder {
        ConfigBuilder::default().server_address(server_address)
    }

    pub fn from_file(path: PathBuf) -> Result<Self, ConfigError> {
        let mut config = String::new();
        // Could fail
        File::open(path.clone())
            .map_err(|source| ConfigError::ErrorReadingConfigFile { source, path })?
            .read_to_string(&mut config)
            .unwrap();
        serde_yaml::from_str::<Config>(config.as_str()).map_err(|source| ConfigError::ErrorParsingYaml { source })
    }

    pub(crate) fn validate_and_finalise(&mut self) -> Result<(), ConfigError> {
        let encryption = &self.server.encryption;
        if encryption.enabled && encryption.cert.is_none() {
            return Err(ConfigError::ValidationError {
                message: "Server encryption was enabled, but certificate was not configured.",
            });
        }
        if encryption.enabled && encryption.cert_key.is_none() {
            return Err(ConfigError::ValidationError {
                message: "Server encryption was enabled, but certificate key was not configured.",
            });
        }
        // finalise:
        self.storage.data = resolve_path_from_executable(&self.storage.data);
        self.logging.directory = resolve_path_from_executable(&self.logging.directory);
        Ok(())
    }

    fn is_development_mode_default() -> bool {
        false
    }
}

#[derive(Debug, Default)]
pub struct ConfigBuilder {
    server_address: Option<String>,
    server_http_address: Option<String>,
    authentication: Option<AuthenticationConfig>,
    encryption: Option<EncryptionConfig>,
    diagnostics: Option<DiagnosticsConfig>,
    data_directory: Option<PathBuf>,
    log_directory: Option<PathBuf>,
    is_development_mode: Option<bool>,
}

impl ConfigBuilder {
    fn server_address(mut self, address: impl Into<String>) -> Self {
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
        let mut config = Config {
            server: ServerConfig {
                address: self.server_address.unwrap_or_else(|| DEFAULT_ADDRESS.to_string()),
                http_address: self.server_http_address.clone().unwrap_or("".to_owned()),
                http_enabled: self.server_http_address.is_some(),
                authentication: self.authentication.unwrap_or_else(AuthenticationConfig::default),
                encryption: self.encryption.unwrap_or_else(EncryptionConfig::default),
            },
            storage: StorageConfig { data: data_directory },
            diagnostics: DiagnosticsConfig::default(),
            logging: LoggingConfig { directory: log_directory },
            is_development_mode: Config::IS_DEVELOPMENT_MODE_FORCED || self.is_development_mode.unwrap_or(false),
        };
        config.validate_and_finalise()?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub(crate) address: String,
    pub(crate) http_enabled: bool,
    pub(crate) http_address: String,
    pub(crate) authentication: AuthenticationConfig,
    pub(crate) encryption: EncryptionConfig,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct AuthenticationConfig {
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "token_expiration_seconds")]
    pub token_expiration: Duration,
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self { token_expiration: DEFAULT_AUTHENTICATION_TOKEN_TTL }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub cert: Option<PathBuf>,
    pub cert_key: Option<PathBuf>,
    pub root_ca: Option<PathBuf>,
}

impl EncryptionConfig {
    pub fn disabled() -> Self {
        Self { enabled: false, cert: None, cert_key: None, root_ca: None }
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct DiagnosticsConfig {
    pub is_reporting_error_enabled: bool,
    pub is_reporting_metric_enabled: bool,
    pub is_monitoring_enabled: bool,
    pub monitoring_port: u16,
}

impl DiagnosticsConfig {
    pub fn enabled() -> Self {
        Self {
            is_reporting_error_enabled: true,
            is_reporting_metric_enabled: true,
            is_monitoring_enabled: true,
            monitoring_port: MONITORING_DEFAULT_PORT,
        }
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self::enabled()
    }
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub directory: PathBuf,
}

fn resolve_path_from_executable(path: &PathBuf) -> PathBuf {
    let typedb_dir_or_current = std::env::current_exe()
        .map(|path| path.parent().expect("Expected parent directory of: {path}").to_path_buf())
        .unwrap_or(std::env::current_dir().expect("Expected access to the current directory"));
    // if path is absolute, join will just return path
    typedb_dir_or_current.join(path)
}

#[cfg(test)]
pub mod tests {
    use clap::Parser;

    use crate::parameters::{cli::CLIArgs, config::Config, ConfigError};

    const CONFIG_YAML_PATH: &str = "./typedb.yml";

    fn load_and_parse(toml: &str, args: Vec<&str>) -> Result<Config, ConfigError> {
        let mut args_with_binary_infront = Vec::with_capacity(args.len() + 1);
        args_with_binary_infront.push("dummy");
        args_with_binary_infront.extend(args);
        let mut config = Config::from_file(toml.into())?;
        let cli_args: CLIArgs = CLIArgs::parse_from(args_with_binary_infront);
        cli_args.override_config(&mut config)?;
        Ok(config)
    }

    #[test]
    fn server_toml_parser_properly() {
        assert!(load_and_parse(CONFIG_YAML_PATH, vec![]).is_ok());
    }

    #[test]
    fn fields_can_be_overridden() {
        let set_to = "10.9.8.7:1234";
        let result = load_and_parse(CONFIG_YAML_PATH, vec!["--server.address", set_to]).unwrap();
        assert_eq!(result.server.address.as_str(), set_to);
    }

    #[test]
    fn enabling_encryption_without_setting_cert_and_key_is_flagged() {
        {
            // Check pre-conditions
            let config = load_and_parse("./typedb.yml", vec![]).unwrap();
            assert!(
                config.server.encryption.enabled == false
                    && config.server.encryption.cert_key.is_none()
                    && config.server.encryption.cert.is_none()
            ); // Test is bad if this fails
        }

        {
            let args = vec!["--server.encryption.enabled", "true"];
            assert!(matches!(load_and_parse(CONFIG_YAML_PATH, args), Err(ConfigError::ValidationError { .. })));
        }

        {
            let args = vec![
                "--server.encryption.enabled",
                "true",
                "--server.encryption.cert-key",
                "somekey",
                "--server.encryption.cert",
                "somecert.pem",
            ];
            assert!(load_and_parse(CONFIG_YAML_PATH, args).is_ok());
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.cert", "somecert.pem"];
            assert!(matches!(load_and_parse(CONFIG_YAML_PATH, args), Err(ConfigError::ValidationError { .. })));
        }

        {
            let args = vec!["--server.encryption.enabled", "true", "--server.encryption.cert-key", "somekey"];
            assert!(matches!(load_and_parse(CONFIG_YAML_PATH, args), Err(ConfigError::ValidationError { .. })));
        }
    }
}
