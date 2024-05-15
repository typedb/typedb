/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use durability::{wal::WAL, DurabilityRecordType, DurabilityService};
use itertools::Itertools;
use tempdir::TempDir;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TestRecord {
    pub bytes: Vec<u8>,
}

impl TestRecord {
    pub const RECORD_TYPE: DurabilityRecordType = 0;
    const RECORD_NAME: &'static str = "TEST";

    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes: bytes }
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

fn throughput_small(c: &mut Criterion) {
    let directory = TempDir::new("wal-test").unwrap();
    let wal = create_wal(&directory);

    let message = TestRecord { bytes: b"hello world".to_vec() };
    let serialized_len = message.bytes().len();

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes(serialized_len as u64));
    group.bench_function("write_small", |b| {
        b.iter(|| wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap())
    });
    group.finish();
}

fn throughput_large(c: &mut Criterion) {
    let directory = TempDir::new("wal-test").unwrap();
    let wal = create_wal(&directory);

    let message = TestRecord { bytes: std::iter::repeat(*b"hello world").take(100).flatten().collect_vec() };
    let serialized_len = message.bytes().len();

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes(serialized_len as u64));
    group.bench_function("write_large", |b| {
        b.iter(|| wal.sequenced_write(TestRecord::RECORD_TYPE, message.bytes()).unwrap())
    });
    group.finish();
}

criterion_group!(benches, throughput_small, throughput_large);
criterion_main!(benches);
