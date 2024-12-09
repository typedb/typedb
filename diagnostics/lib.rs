/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::collections::HashMap;

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
