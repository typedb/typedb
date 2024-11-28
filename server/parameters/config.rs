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
use resource::constants::server::DEFAULT_ADDRESS;

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
                address: SocketAddr::from_str(DEFAULT_ADDRESS).unwrap(),
                encryption: EncryptionConfig::disabled()
            },
            storage: StorageConfig { data: typedb_dir_or_current.join(PathBuf::from_str("server/data").unwrap()) },
        }
    }

    pub fn new_with_encryption_config(encryption_config: EncryptionConfig) -> Self {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().unwrap().to_path_buf())
            .unwrap_or(std::env::current_dir().unwrap());
        Self {
            server: ServerConfig {
                address: SocketAddr::from_str("0.0.0.0:1729").unwrap(),
                encryption: encryption_config
            },
            storage: StorageConfig { data: typedb_dir_or_current.join(PathBuf::from_str("server/data").unwrap()) },
        }
    }

    pub fn new_with_data_directory(data_directory: &Path) -> Self {
        Self {
            server: ServerConfig {
                address: SocketAddr::from_str("0.0.0.0:1729").unwrap(),
                encryption: EncryptionConfig::disabled()
            },
            storage: StorageConfig { data: data_directory.to_owned() },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServerConfig {
    pub(crate) address: SocketAddr,
    pub(crate) encryption: EncryptionConfig,
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
        Self::new(false, None, None, None)
    }

    pub fn new(enabled: bool,
        cert: Option<PathBuf>,
        cert_key: Option<PathBuf>,
        root_ca: Option<PathBuf>
    ) -> Self {
        Self { enabled, cert, cert_key, root_ca }
    }
}

#[derive(Debug)]
pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}
