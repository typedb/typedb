/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::PathBuf, sync::Arc};

use error::typedb_error;
use options::ParseByteSizeError;

pub mod cli;
pub mod config;

typedb_error! {
    pub ConfigError(component = "Config", prefix = "CFG") {
        ErrorReadingConfigFile(1, "Error reading config file '{path:?}'.", path: PathBuf, source: Arc<std::io::Error>),
        ErrorParsingYaml(2, "Error parsing YAML config.", source: Arc<serde::de::value::Error>),
        ValidationError(3, "{message}", message: &'static str),
        InvalidByteSize(4, "Invalid value for `{flag}`.", flag: &'static str, source: ParseByteSizeError),
    }
}
