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

use std::path::Path;

use durability::{wal::WAL, DurabilityRecord, DurabilityRecordType, DurabilityService};
use itertools::Itertools;

#[derive(Debug, PartialEq, Eq, Clone)]
struct TestRecord {
    bytes: Vec<u8>,
}

impl DurabilityRecord for TestRecord {
    const RECORD_TYPE: DurabilityRecordType = 0;
    const RECORD_NAME: &'static str = "TEST";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        writer.write_all(&self.bytes)?;
        Ok(())
    }

    fn deserialize_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Self { bytes })
    }
}

fn open_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::open(directory).unwrap();
    wal.register_record_type::<TestRecord>();
    wal
}

fn main() {
    let wal = open_wal(std::env::args().nth(1).unwrap());
    let message = std::env::args().nth(2).unwrap().bytes().collect_vec();
    for i in 0.. {
        let record = TestRecord { bytes: message.iter().copied().chain(format!(" {i}").bytes()).collect_vec() };
        wal.sequenced_write(&record).unwrap();
    }
}
