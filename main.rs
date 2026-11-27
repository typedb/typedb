/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::path::Path;

use clap::Parser;
use diagnostics::error_reporting;
use logger::{initialise_logging_global, safe_eprintln};
use resource::constants::server::{DEFAULT_CONFIG_PATH, DISTRIBUTION_INFO};
use server::{
    ServerBuilder,
    parameters::{
        cli::CLIArgs,
        config::{Config, ConfigBuilder},
    },
};
use tokio::runtime::Runtime;

const ERROR_REPORTING_PRODUCT: &str = "server";

fn main() {
    initialise_abort_on_panic();
    let cli_args: CLIArgs = CLIArgs::parse();
    let config_file = match cli_args.config_file_override.as_ref() {
        None => ConfigBuilder::resolve_path_from_executable(Path::new(DEFAULT_CONFIG_PATH)),
        Some(path) => CLIArgs::resolve_path_from_pwd(Path::new(path)),
    };
    let mut config_builder = ConfigBuilder::from_file(config_file).unwrap_or_else(|err| {
        safe_eprintln!("Error reading from config file: {:?}", err);
        std::process::exit(1);
    });
    config_builder.override_with_cliargs(cli_args).unwrap_or_else(|err| {
        safe_eprintln!("Error applying CLI overrides: {:?}", err);
        std::process::exit(1);
    });
    let config = config_builder.build().unwrap_or_else(|err| {
        safe_eprintln!("Error validating config file overridden with cli args: {:?}", err);
        std::process::exit(1);
    });
    initialise_logging_global(&config.logging.directory);

    #[cfg(not(debug_assertions))]
    if std::env::var(fail_point::FAIL_POINT_ENV).is_ok() {
        tracing::warn!(
            "{} is set, but `debug_assertions` are not enabled; fail points will not trigger",
            fail_point::FAIL_POINT_ENV
        );
    }

    ServerApplication::new(config).run();
}

struct ServerApplication {
    runtime: Runtime,
    config: Config,
}

impl ServerApplication {
    fn new(config: Config) -> Self {
        may_initialise_error_reporting(&config);
        Self { config, runtime: create_tokio_runtime() }
    }

    fn run(self) {
        self.runtime.block_on(async {
            let server =
                ServerBuilder::default().distribution_info(DISTRIBUTION_INFO).build(self.config).await.unwrap();
            match server.serve().await {
                Ok(_) => tracing::info!("Exited."),
                Err(err) => tracing::error!("Exited with error: {:?}", err),
            }
        })
    }
}

fn initialise_abort_on_panic() {
    std::panic::set_hook({
        let default_panic = std::panic::take_hook();
        Box::new(move |info| {
            default_panic(info);
            error_reporting::report_panic(info);
            std::process::exit(1);
        })
    });
}

fn may_initialise_error_reporting(config: &Config) {
    if config.diagnostics.reporting.report_errors && !config.development_mode.enabled {
        error_reporting::init(ERROR_REPORTING_PRODUCT, DISTRIBUTION_INFO.version);
    }
}

fn create_tokio_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Expected a main tokio runtime")
}
