/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
