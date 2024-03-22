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

use std::path::Path;

use durability::{wal::WAL, DurabilityRecord, DurabilityRecordType, DurabilityService};

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

    fn deserialize_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Self { bytes })
    }
}

pub fn open_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::recover(directory).unwrap();
    wal.register_record_type::<TestRecord>();
    wal
}
