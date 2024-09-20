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

impl Config {
    pub fn new() -> Self {
        let typedb_dir_or_current = std::env::current_exe()
            .map(|path| path.parent().unwrap().to_path_buf())
            .unwrap_or(std::env::current_dir().unwrap());
        Self {
            server: ServerConfig { address: SocketAddr::from_str("127.0.0.1:1729").unwrap() },
            storage: StorageConfig { data: typedb_dir_or_current.join(PathBuf::from_str("server/data").unwrap()) },
        }
    }

    pub fn new_with_data_directory(data_directory: &Path) -> Self {
        Self {
            server: ServerConfig { address: SocketAddr::from_str("127.0.0.1:1729").unwrap() },
            storage: StorageConfig { data: data_directory.to_owned() },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServerConfig {
    pub(crate) address: SocketAddr,
}

#[derive(Debug)]
pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}
