/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use logger::initialise_logging_global;
use resource::constants::server::ASCII_LOGO;
use server::parameters::config::Config;

#[tokio::main]
async fn main() {
    setup_abort_on_panic();

    print_ascii_logo(); // very important
    initialise_logging_global();

    let config = get_configuration();

    let open_result = server::typedb::Server::open(config);

    let result = open_result.unwrap().serve().await;
    match result {
        Ok(_) => println!("Exited."),
        Err(err) => println!("Exited with error: {:?}", err),
    }
}

fn get_configuration() -> Config {
    Config::new()
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
