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
#![feature(test)]

use std::process::Termination;
use test::{Bencher, black_box};
use encoding::thing::id_generator::ThingIIDGenerator;

#[bench]
fn id_generation(&mut bencher: Bencher) -> impl Termination {
    let thing_iid_generator = ThingIIDGenerator::new();
    dbg!("TEST");
    ().into()
}

// #[bench]
// fn id_generation_and_serialisation() {
//
// }
