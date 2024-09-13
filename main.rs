/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use logger::initialise_logging;
use resource::constants::server::ASCII_LOGO;
use server::parameters::config::Config;

#[tokio::main]
async fn main() {
    print_ascii_logo(); // very important
    let _guard = initialise_logging();

    let config = get_configuration();

    server::typedb::Server::open(config).unwrap().serve().await.unwrap()
}

fn get_configuration() -> Config {
    Config::new()
}

fn print_ascii_logo() {
    println!("{ASCII_LOGO}");
}
