/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::path::PathBuf;

use clap::Parser;
use logger::initialise_logging_global;
use resource::constants::server::{DEFAULT_CONFIG_PATH, SENTRY_REPORTING_URI, SERVER_INFO};
use sentry::ClientInitGuard as SentryGuard;
use server::{
    parameters::{
        cli::CLIArgs,
        config::{Config, ConfigBuilder},
    },
    ServerBuilder,
};
use tokio::runtime::Runtime;

fn main() {
    initialise_abort_on_panic();
    let cli_args: CLIArgs = CLIArgs::parse();
    let config_file = match cli_args.config_file_override.as_ref() {
        None => ConfigBuilder::resolve_path_from_executable(&PathBuf::from(DEFAULT_CONFIG_PATH)),
        Some(path) => CLIArgs::resolve_path_from_pwd(&path.into()),
    };
    let mut config_builder = ConfigBuilder::from_file(config_file.into()).expect("Error reading from config file");
    config_builder.override_with_cliargs(cli_args);
    let config = config_builder.build().expect("Error validating config file overridden with cli args");
    initialise_logging_global(&config.logging.directory);
    // This guard sends Sentry reports when it's dropped. It has to be alive while the server is running.
    // DO NOT delete!
    let _guard = may_initialise_error_reporting(&config);
    create_tokio_runtime().block_on(async {
        let server = ServerBuilder::default().server_info(SERVER_INFO).build(config).await.unwrap();
        match server.serve().await {
            Ok(_) => println!("Exited."),
            Err(err) => println!("Exited with error: {:?}", err),
        }
    });
}

fn initialise_abort_on_panic() {
    std::panic::set_hook({
        let default_panic = std::panic::take_hook();
        Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        })
    });
}

fn may_initialise_error_reporting(config: &Config) -> Option<SentryGuard> {
    if config.diagnostics.reporting.report_errors && !config.development_mode.enabled {
        let options = (
            SENTRY_REPORTING_URI,
            sentry::ClientOptions { release: Some(SERVER_INFO.version.into()), ..Default::default() },
        );
        Some(sentry::init(options))
    } else {
        None
    }
}

fn create_tokio_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Expected a main tokio runtime")
}
