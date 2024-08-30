/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;

use durability::{wal::WAL, DurabilityRecordType, DurabilityService};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TestRecord {
    pub bytes: Vec<u8>,
}

impl TestRecord {
    pub const RECORD_TYPE: DurabilityRecordType = 0;
    const RECORD_NAME: &'static str = "TEST";

    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub fn create_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::create(directory).unwrap();
    wal.register_record_type(TestRecord::RECORD_TYPE, TestRecord::RECORD_NAME);
    wal
}

pub fn load_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::load(directory).unwrap();
    wal.register_record_type(TestRecord::RECORD_TYPE, TestRecord::RECORD_NAME);
    wal
}
