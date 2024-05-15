/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use std::str;

use durability::{DurabilitySequenceNumber, DurabilityService, RawRecord};
use durability_test_common::{load_wal, TestRecord};

fn main() {
    let wal = load_wal(std::env::args().nth(1).unwrap());
    for RawRecord { sequence_number, record_type, bytes } in wal.iter_any_from(DurabilitySequenceNumber::MIN).unwrap().map(|r| r.unwrap())
    {
        assert_eq!(record_type, TestRecord::RECORD_TYPE);
        let number = sequence_number.number();
        println!(r#"{} "{}""#, number, str::from_utf8(&bytes).unwrap());
    }
}
