/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use clap::Parser;
use logger::initialise_logging_global;
use resource::constants::server::{DISTRIBUTION, SENTRY_REPORTING_URI, VERSION};
use sentry::ClientInitGuard as SentryGuard;
use server::parameters::{
    cli::CLIArgs,
    config::ServerConfig,
};

fn main() {
    setup_abort_on_panic();
    initialise_logging_global();

    let args = CLIArgs::parse();
    let config = args.to_config();

    let deployment_id = None;
    let is_error_reporting_enabled = args.diagnostics_reporting_errors;
    let _error_reporting_guard = setup_error_reporting(config.server_config(), is_error_reporting_enabled);

    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Expected a main tokio runtime").block_on(
        async {
            let open_result = server::server::Server::open(config, DISTRIBUTION, VERSION, deployment_id).await;
            let result = open_result.unwrap().serve().await;
            match result {
                Ok(_) => println!("Exited."),
                Err(err) => println!("Exited with error: {:?}", err),
            }
        },
    );
}

fn setup_abort_on_panic() {
    std::panic::set_hook({
        let default_panic = std::panic::take_hook();
        Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        })
    });
}

fn setup_error_reporting(server_config: &ServerConfig, is_error_reporting_enabled: bool) -> Option<SentryGuard> {
    if is_error_reporting_enabled && !server_config.is_development_mode {
        Some(sentry::init((
            SENTRY_REPORTING_URI,
            sentry::ClientOptions { release: Some(VERSION.into()), ..Default::default() },
        )))
    } else {
        None
    }
}
