/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use logger::initialise_logging;
use resource::constants::server::ASCII_LOGO;

#[tokio::main]
async fn main() {
    print_ascii_logo(); // very important

    let _guard = initialise_logging();

    server::typedb::Server::open("runtimedata/server/data").unwrap()
        .serve()
        .await
        .unwrap()
}

fn print_ascii_logo() {
    println!("{ASCII_LOGO}");
}
