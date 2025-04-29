/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use clap::Parser;
use logger::initialise_logging_global;
use resource::constants::server::{ASCII_LOGO, DEFAULT_CONFIG_PATH, DISTRIBUTION, SENTRY_REPORTING_URI, VERSION};
use server::{
    parameters::{cli::CLIArgs, config::Config},
    server::Server,
};
use tokio::runtime::Runtime;

fn main() {
    initialise_abort_on_panic();
    initialise_logging_global();
    let cli_args: CLIArgs = CLIArgs::parse();
    let config_file = cli_args.config_file_override.as_ref().map(|x| x.as_str()).unwrap_or(DEFAULT_CONFIG_PATH);
    let mut config = Config::from_file(config_file.into()).unwrap();
    cli_args.override_config(&mut config).unwrap();
    may_initialise_error_reporting(&config);
    create_tokio_runtime().block_on(async {
        let server = Server::new(config, ASCII_LOGO, DISTRIBUTION, VERSION, None).await.unwrap();
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

fn may_initialise_error_reporting(config: &Config) {
    if config.diagnostics.is_reporting_error_enabled && !config.is_development_mode {
        let opts =
            (SENTRY_REPORTING_URI, sentry::ClientOptions { release: Some(VERSION.into()), ..Default::default() });
        let _ = sentry::init(opts);
    }
}

fn create_tokio_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Expected a main tokio runtime")
}
