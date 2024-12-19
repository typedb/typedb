/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{path::PathBuf, str::FromStr};

use clap::Parser;
use logger::initialise_logging_global;
use resource::constants::server::ASCII_LOGO;
use server::parameters::{
    cli::CLIArgs,
    config::{Config, DiagnosticsConfig, EncryptionConfig},
};

const DISTRIBUTION: &str = "TypeDB CE";

#[tokio::main]
async fn main() {
    setup_abort_on_panic();
    let cli_args = server::parameters::cli::CLIArgs::parse();

    print_ascii_logo(); // very important
    initialise_logging_global();

    let deployment_id = None;
    let config = get_configuration(cli_args);

    let open_result = server::typedb::Server::open(config, DISTRIBUTION, deployment_id).await;

    let result = open_result.unwrap().serve().await;
    match result {
        Ok(_) => println!("Exited."),
        Err(err) => println!("Exited with error: {:?}", err),
    }
}

fn get_configuration(cli_args: CLIArgs) -> Config {
    let encryption_config = EncryptionConfig {
        enabled: cli_args.server_encryption_enabled,
        cert: cli_args.server_encryption_cert.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
        cert_key: cli_args.server_encryption_cert_key.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
        root_ca: cli_args.server_encryption_root_ca.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
    };
    let diagnostics_config = DiagnosticsConfig {
        is_monitoring_enabled: cli_args.diagnostics_monitoring_enable,
        monitoring_port: cli_args.diagnostics_monitoring_port,
        is_service_reporting_enabled: cli_args.diagnostics_reporting_metrics,
    };
    let data_dir = cli_args.storage_data.map(|dir| PathBuf::from_str(dir.as_str()).unwrap());
    Config::customised(cli_args.server_address, Some(encryption_config), Some(diagnostics_config), data_dir, cli_args.development_mode_enabled)
}

fn print_ascii_logo() {
    println!("{ASCII_LOGO}");
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
