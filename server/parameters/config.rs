/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use resource::constants::server::{DEFAULT_ADDRESS, DEFAULT_DATA_DIR, MONITORING_DEFAULT_PORT};
use tokio::net::lookup_host;

#[derive(Debug)]
pub struct Config {
    pub(crate) server: ServerConfig,
    pub(crate) storage: StorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().unwrap().to_path_buf())
            .unwrap_or(std::env::current_dir().unwrap());
        Self {
            server: ServerConfig {
                address: DEFAULT_ADDRESS.to_string(),
                encryption: EncryptionConfig::default(),
                diagnostics: DiagnosticsConfig::default(),
                is_development_mode: ServerConfig::IS_DEVELOPMENT_MODE_FORCED,
            },
            storage: StorageConfig { data: typedb_dir_or_current.join(PathBuf::from_str(DEFAULT_DATA_DIR).unwrap()) },
        }
    }

    pub fn new_with_encryption_config(encryption_config: EncryptionConfig, is_development_mode: bool) -> Self {
        Self::customised(None, Some(encryption_config), None, None, is_development_mode)
    }

    pub fn new_with_diagnostics_config(diagnostics_config: DiagnosticsConfig, is_development_mode: bool) -> Self {
        Self::customised(None, None, Some(diagnostics_config), None, is_development_mode)
    }

    pub fn new_with_data_directory(data_directory: &Path, is_development_mode: bool) -> Self {
        Self::customised(None, None, None, Some(data_directory.to_path_buf()), is_development_mode)
    }

    pub fn customised(
        server_address: Option<String>,
        encryption_config: Option<EncryptionConfig>,
        diagnostics_config: Option<DiagnosticsConfig>,
        data_directory: Option<PathBuf>,
        is_development_mode: bool,
    ) -> Self {
        let server_address = server_address.unwrap_or_else(|| DEFAULT_ADDRESS.to_string());
        let encryption_config = encryption_config.unwrap_or_else(|| EncryptionConfig::default());
        let diagnostics_config = diagnostics_config.unwrap_or_else(|| DiagnosticsConfig::default());
        let data_directory = data_directory.map(|dir| dir.to_path_buf()).unwrap_or_else(|| {
            let typedb_dir_or_current = std::env::current_exe()
                .map(|path| path.parent().unwrap().to_path_buf())
                .unwrap_or(std::env::current_dir().unwrap());
            typedb_dir_or_current.join(PathBuf::from_str(DEFAULT_DATA_DIR).unwrap())
        });
        let is_development_mode = ServerConfig::IS_DEVELOPMENT_MODE_FORCED || is_development_mode;
        Self {
            server: ServerConfig {
                address: server_address,
                encryption: encryption_config,
                diagnostics: diagnostics_config,
                is_development_mode,
            },
            storage: StorageConfig { data: data_directory.to_owned() },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServerConfig {
    pub(crate) address: String,
    pub(crate) encryption: EncryptionConfig,
    pub(crate) diagnostics: DiagnosticsConfig,

    pub(crate) is_development_mode: bool,
}

impl ServerConfig {
    #[cfg(feature = "published")]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = false;
    #[cfg(not(feature = "published"))]
    pub const IS_DEVELOPMENT_MODE_FORCED: bool = true;
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct DiagnosticsConfig {
    pub is_monitoring_enabled: bool,
    pub monitoring_port: u16,
    pub is_reporting_enabled: bool,
}

impl DiagnosticsConfig {
    pub fn enabled() -> Self {
        Self {
            is_monitoring_enabled: true,
            monitoring_port: MONITORING_DEFAULT_PORT,
            is_reporting_enabled: true,
        }
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self::enabled()
    }
}
