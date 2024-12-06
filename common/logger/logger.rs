/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unexpected_cfgs)]

use tracing::{self, dispatcher::DefaultGuard, metadata::LevelFilter, Level};
pub use tracing::{error, info, trace};
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

pub mod result;

pub fn initialise_logging_global() {
    let filter = EnvFilter::from_default_env()
        .add_directive(LevelFilter::INFO.into())
        // .add_directive("database=trace".parse().unwrap())
        // .add_directive("server=trace".parse().unwrap())
        // .add_directive("storage=trace".parse().unwrap())
        // .add_directive("executor=trace".parse().unwrap())
        // .add_directive("query=trace".parse().unwrap())
        // useful for debugging what tonic is doing:
        // .add_directive("tonic=trace".parse().unwrap());
    ;

    let subscriber = SubscriberBuilder::default().with_max_level(Level::TRACE).with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap()
}

pub fn initialise_logging() -> DefaultGuard {
    let subscriber = SubscriberBuilder::default().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_default(subscriber)
}
