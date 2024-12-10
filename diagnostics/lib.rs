/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{collections::HashMap, path::PathBuf};

use crate::metrics::{ActionMetrics, ErrorMetrics, LoadMetrics, ServerMetrics, ServerProperties};

pub mod diagnostics_manager;
pub mod metrics;
mod version;

type DatabaseName = String;

#[derive(Debug)]
pub struct Diagnostics {
    server_properties: ServerProperties,
    server_metrics: ServerMetrics,
    load_metrics: HashMap<DatabaseName, LoadMetrics>,
    action_metrics: HashMap<DatabaseName, ActionMetrics>,
    error_metrics: HashMap<DatabaseName, ErrorMetrics>,
}

impl Diagnostics {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        reporting_enabled: bool,
    ) -> Diagnostics {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: HashMap::new(),
            action_metrics: HashMap::new(),
            error_metrics: HashMap::new(),
        }
    }
}
