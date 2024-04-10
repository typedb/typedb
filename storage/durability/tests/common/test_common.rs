/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;

use durability::{wal::WAL, DurabilityRecord, DurabilityRecordType, DurabilityService, SequencedDurabilityRecord};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TestRecord {
    pub bytes: Vec<u8>,
}

impl DurabilityRecord for TestRecord {
    const RECORD_TYPE: DurabilityRecordType = 0;
    const RECORD_NAME: &'static str = "TEST";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        writer.write_all(&self.bytes)?;
        Ok(())
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Self { bytes })
    }
}

impl SequencedDurabilityRecord for TestRecord { }

pub fn open_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::recover(directory).unwrap();
    wal.register_record_type::<TestRecord>();
    wal
}
