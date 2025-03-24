/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use resource::constants::server::{
    DEFAULT_ADDRESS, DEFAULT_AUTHENTICATION_TOKEN_TTL, DEFAULT_DATA_DIR, MONITORING_DEFAULT_PORT,
};

#[derive(Debug)]
pub struct Config {
    pub server: ServerConfig,
    pub(crate) storage: StorageConfig,
    pub diagnostics: DiagnosticsConfig,
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
}

#[derive(Debug, Default)]
pub struct ConfigBuilder {
    server_address: Option<String>,
    server_http_address: Option<String>,
    authentication: Option<AuthenticationConfig>,
    encryption: Option<EncryptionConfig>,
    diagnostics: Option<DiagnosticsConfig>,
    data_directory: Option<PathBuf>,
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

    pub fn build(self) -> Config {
        Config {
            server: ServerConfig {
                address: self.server_address.unwrap_or_else(|| DEFAULT_ADDRESS.to_string()),
                http_address: self.server_http_address,
                authentication: self.authentication.unwrap_or_else(AuthenticationConfig::default),
                encryption: self.encryption.unwrap_or_else(EncryptionConfig::default),
            },
            storage: StorageConfig { data: self.data_directory.unwrap_or_else(StorageConfig::default_directory) },
            diagnostics: DiagnosticsConfig::default(),
            is_development_mode: Config::IS_DEVELOPMENT_MODE_FORCED || self.is_development_mode.unwrap_or(false),
        }
    }
}

#[derive(Debug)]
pub struct ServerConfig {
    pub(crate) address: String,
    pub(crate) http_address: Option<String>,
    pub(crate) authentication: AuthenticationConfig,
    pub(crate) encryption: EncryptionConfig,
}

#[derive(Debug)]
pub struct AuthenticationConfig {
    pub token_expiration_seconds: Duration,
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self { token_expiration_seconds: DEFAULT_AUTHENTICATION_TOKEN_TTL }
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}

impl StorageConfig {
    fn default_directory() -> PathBuf {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().expect("Expected parent directory of: {path}").to_path_buf())
            .unwrap_or(std::env::current_dir().expect("Expected access to the current directory"));
        typedb_dir_or_current.join(
            PathBuf::from_str(DEFAULT_DATA_DIR).expect("Expected default data directory to exist: {DEFAULT_DATA_DIR}"),
        )
    }
}

#[derive(Debug)]
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
