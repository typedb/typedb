/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::path::PathBuf;
use std::str::FromStr;
use logger::initialise_logging_global;
use resource::constants::server::ASCII_LOGO;
use server::parameters::config::{Config, EncryptionConfig};
use clap::Parser;
use server::parameters::cli::CLIArgs;

#[tokio::main]
async fn main() {
    setup_abort_on_panic();
    let cli_args = server::parameters::cli::CLIArgs::parse();

    print_ascii_logo(); // very important
    initialise_logging_global();

    let config = get_configuration(cli_args);

    let open_result = server::typedb::Server::open(config);

    let result = open_result.unwrap().serve().await;
    match result {
        Ok(_) => println!("Exited."),
        Err(err) => println!("Exited with error: {:?}", err),
    }
}

fn get_configuration(cli_args: CLIArgs) -> Config {
    let encryption_config = EncryptionConfig::new(
        cli_args.server_encryption_enabled,
        cli_args.server_encryption_cert.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
        cli_args.server_encryption_cert_key.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
        cli_args.server_encryption_root_ca.map(|path| PathBuf::from_str(path.as_str()).unwrap()),
    );
    Config::new_with_encryption_config(encryption_config)
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
