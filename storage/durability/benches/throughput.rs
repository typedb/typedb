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

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use durability::{wal::WAL, DurabilityRecord, DurabilityRecordType, DurabilityService};
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

    fn deserialize_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Self { bytes })
    }
}

pub fn open_wal(directory: impl AsRef<Path>) -> WAL {
    let mut wal = WAL::open(directory).unwrap();
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
