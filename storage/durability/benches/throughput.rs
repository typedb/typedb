/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use durability::{wal::WAL, DurabilityRecord, DurabilityRecordType, DurabilityService, SequencedDurabilityRecord};
use itertools::Itertools;
use tempdir::TempDir;

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

fn throughput_small(c: &mut Criterion) {
    let directory = TempDir::new("wal-test").unwrap();
    let wal = open_wal(&directory);

    let message = TestRecord { bytes: b"hello world".to_vec() };
    let serialized_len = {
        let mut buf = Vec::new();
        message.serialise_into(&mut buf).unwrap();
        buf.len()
    };

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes(serialized_len as u64));
    group.bench_function("write_small", |b| b.iter(|| wal.sequenced_write(&message).unwrap()));
    group.finish();
}

fn throughput_large(c: &mut Criterion) {
    let directory = TempDir::new("wal-test").unwrap();
    let wal = open_wal(&directory);

    let message = TestRecord { bytes: std::iter::repeat(*b"hello world").take(100).flatten().collect_vec() };
    let serialized_len = {
        let mut buf = Vec::new();
        message.serialise_into(&mut buf).unwrap();
        buf.len()
    };

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes(serialized_len as u64));
    group.bench_function("write_large", |b| b.iter(|| wal.sequenced_write(&message).unwrap()));
    group.finish();
}

criterion_group!(benches, throughput_small, throughput_large);
criterion_main!(benches);
