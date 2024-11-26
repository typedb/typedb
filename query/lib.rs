/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

mod definable_resolution;
mod definable_status;
mod define;
pub mod error;
pub mod query_cache;
pub mod query_manager;
mod redefine;
mod undefine;
