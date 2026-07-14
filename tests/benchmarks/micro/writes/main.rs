/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs::File, os::raw::c_int, path::Path};

use database::transaction::{CommitIntent, DataCommitError};
use criterion::{Criterion, criterion_group, criterion_main, profiler::Profiler};
use pprof::ProfilerGuard;
use database::transaction::TransactionWrite;
use lib_benchmark::{Config, Context};
use options::TransactionOptions;
use storage::durability_client::WALClient;

const SCHEMA: &str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

fn bench_insert_person_only(c: &mut Criterion) {
    let query = "insert $_ isa person;".to_owned();
    let context = Context::init(Config::default());
    let db = context.create_database("insert_person_only").unwrap();

    lib_benchmark::create_schema(db.clone(), SCHEMA);
    c.bench_function("bench_insert_person_only", |b| b.iter(|| {
        let tx = TransactionWrite::open(db.clone(), TransactionOptions::default()).unwrap();
        let (tx, query_result) = lib_benchmark::execute_write_query_in(tx, query.clone(), None);
        query_result.unwrap();
        lib_benchmark::commit_write_tx(tx);
    }));
}

fn criterion_benchmark(c: &mut Criterion) {
    println!("In criterion benchmark");
    bench_insert_person_only(c);
}

// --- Code to generate flamegraphs copied from https://www.jibbow.com/posts/criterion-flamegraphs/ ---
// This causes a SIGBUS on (mac) arm64 if the frequency is set too high.

pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        FlamegraphProfiler { frequency, active_profiler: None }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(&flamegraph_path).expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            profiler.report().build().unwrap().flamegraph(flamegraph_file).expect("Error writing flamegraph");
            println!("Wrote flamegraph to {}", flamegraph_path.display());
        }
    }
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(100))
}

criterion_group!(
    name = benches;
    config = profiled();
    targets = criterion_benchmark
);

criterion_main!(benches);
