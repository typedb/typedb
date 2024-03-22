/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use logger::initialise_logging;
use server::typedb;
use resource::constants::server::ASCII_LOGO;

fn main() {
    print_ascii_logo(); // very important

    let _guard = initialise_logging();

    typedb::Server::recover("runtimedata/server/data").unwrap().serve();
}

fn print_ascii_logo() {
    println!("{ASCII_LOGO}");
}

