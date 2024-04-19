/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use logger::initialise_logging;
use server::typedb;
use resource::constants::server::ASCII_LOGO;

fn main() {
    print_ascii_logo(); // very important

    let _guard = initialise_logging();

    typedb::Server::open("runtimedata/server/data").unwrap().serve();
}

fn print_ascii_logo() {
    println!("{ASCII_LOGO}");
}

