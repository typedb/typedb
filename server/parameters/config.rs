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

pub struct Config {
    pub(crate) server: ServerConfig,
    pub(crate) storage: StorageConfig,
}

impl Config {
    pub fn new() -> Self {
        Self {
            server: ServerConfig { address: SocketAddr::from_str("127.0.0.1:1729").unwrap() },
            storage: StorageConfig { data: "runtimedata/server/data".into() },
        }
    }

    pub fn new_with_data_directory(data_directory: &Path) -> Self {
        Self {
            server: ServerConfig { address: SocketAddr::from_str("127.0.0.1:1729").unwrap() },
            storage: StorageConfig { data: data_directory.into() },
        }
    }
}

pub(crate) struct ServerConfig {
    pub(crate) address: SocketAddr,
}

pub(crate) struct StorageConfig {
    pub(crate) data: PathBuf,
}
