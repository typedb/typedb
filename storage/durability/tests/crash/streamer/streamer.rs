/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use durability::DurabilityService;
use durability_test_common::{open_wal, TestRecord};
use itertools::Itertools;

fn main() {
    let wal = open_wal(std::env::args().nth(1).unwrap());
    let message = std::env::args().nth(2).unwrap().bytes().collect_vec();
    for i in 1.. {
        let record = TestRecord { bytes: message.iter().copied().chain(format!(" {i}").bytes()).collect_vec() };
        wal.sequenced_write(&record).unwrap();
    }
}
