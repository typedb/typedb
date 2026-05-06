/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::PathBuf,
};

use options::ParseByteSizeError;

pub mod cli;
pub mod config;

#[derive(Debug)]
pub enum ConfigError {
    ErrorReadingConfigFile { source: std::io::Error, path: PathBuf },
    ErrorParsingYaml { source: serde::de::value::Error },
    ValidationError { message: &'static str },
    InvalidByteSize { flag: &'static str, source: ParseByteSizeError },
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ErrorReadingConfigFile { path, source } => {
                write!(f, "Error reading config file '{}': {}", path.display(), source)
            }
            ConfigError::ErrorParsingYaml { source } => write!(f, "Error parsing YAML config: {}", source),
            ConfigError::ValidationError { message } => f.write_str(message),
            ConfigError::InvalidByteSize { flag, source } => write!(f, "Invalid value for `{}`: {}", flag, source),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConfigError::ErrorReadingConfigFile { source, .. } => Some(source),
            ConfigError::ErrorParsingYaml { source } => Some(source),
            ConfigError::ValidationError { .. } => None,
            ConfigError::InvalidByteSize { source, .. } => Some(source),
        }
    }
}
