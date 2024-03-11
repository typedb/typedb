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
#![deny(rust_2018_idioms)]

use durability::DurabilityService;
use itertools::Itertools;

mod common;

use self::common::{open_wal, TestRecord};

fn main() {
    let wal = open_wal(std::env::args().nth(1).unwrap());
    let message = std::env::args().nth(2).unwrap().bytes().collect_vec();
    for i in 0.. {
        let record = TestRecord { bytes: message.iter().copied().chain(format!(" {i}").bytes()).collect_vec() };
        wal.sequenced_write(&record).unwrap();
    }
}
