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
                address: SocketAddr::from_str("0.0.0.0:1729").unwrap(),
                encryption: EncryptionConfig::disabled()
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
pub(crate) struct EncryptionConfig {
    pub(crate) enabled: bool,
    pub(crate) cert: Option<PathBuf>,
    pub(crate) cert_key: Option<PathBuf>,
    pub(crate) root_ca: Option<PathBuf>,
}

impl EncryptionConfig {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            cert: None,
            cert_key: None,
            root_ca: None,
        }
    }

    pub(crate) fn enabled(cert: PathBuf, cert_key: PathBuf) -> Self {
        Self {
            enabled: true,
            cert: Some(cert),
            cert_key: Some(cert_key),
            root_ca: None,
        }
    }

    pub(crate) fn enabled_with_custom_ca(cert: PathBuf, cert_key: PathBuf, root_ca: PathBuf) -> Self {
        Self {
            enabled: true,
            cert: Some(cert),
            cert_key: Some(cert_key),
            root_ca: Some(root_ca),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}
