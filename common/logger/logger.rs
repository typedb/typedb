/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unexpected_cfgs)]

use std::{fs, io::stdout, path::PathBuf};

use tracing::{self, dispatcher::DefaultGuard, metadata::LevelFilter, Level};
pub use tracing::{debug, error, info, trace};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::{writer::Tee, SubscriberBuilder},
    EnvFilter,
};

use crate::tracing_panic::log_panic;

pub mod result;
mod tracing_panic;

pub fn initialise_logging_global(logdir: &PathBuf) {
    debug_assert!(logdir.is_absolute());
    let filter = EnvFilter::from_default_env()
        .add_directive(LevelFilter::INFO.into())
        // .add_directive("database=trace".parse().unwrap())
        // .add_directive("server=trace".parse().unwrap())
        // .add_directive("storage=trace".parse().unwrap())
        // .add_directive("executor=trace".parse().unwrap())
        // .add_directive("query=trace".parse().unwrap())
        // .add_directive("compiler=trace".parse().unwrap())
        // useful for debugging what tonic is doing:
        // .add_directive("tonic=trace".parse().unwrap());
    ;

    fs::create_dir_all(logdir.clone()).expect("Failed to create log dir");
    let file_appender = RollingFileAppender::new(Rotation::HOURLY, logdir, "typedb.log");
    let subscriber = SubscriberBuilder::default()
        .with_max_level(Level::TRACE)
        .with_env_filter(filter)
        .with_writer(Tee::new(stdout, file_appender))
        .with_ansi(false) // Disable ANSI colors in file output
        .with_thread_ids(true)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .finish();

    let result = tracing::subscriber::set_global_default(subscriber).unwrap();
    let old_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        log_panic(panic_info);
        old_panic_hook(panic_info)
    }));
    result
}

pub fn initialise_logging() -> DefaultGuard {
    let subscriber = SubscriberBuilder::default().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_default(subscriber)
}
