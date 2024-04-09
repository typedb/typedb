/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use tracing::{self, dispatcher::DefaultGuard};
pub use tracing::{error, info, trace};
use tracing_subscriber::prelude::*;

pub mod result;

pub fn initialise_logging() -> DefaultGuard {
    let default_layer = tracing_subscriber::fmt::layer();
    let subscriber = tracing_subscriber::registry().with(default_layer);
    tracing::subscriber::set_default(subscriber)
}
