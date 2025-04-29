/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::PathBuf;

pub mod cli;
pub mod config;

#[derive(Debug)]
pub enum ConfigError {
    ErrorReadingConfigFile { source: std::io::Error, path: PathBuf },

    ErrorParsingYaml { source: serde_yaml::Error },
    ValidationError { message: &'static str },
}
