/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
    typedb_workload::benchmark::sanity_check,
};

// mod match_reads;
mod insert_entities;
mod insert_relations;
mod run_configs;
mod simple_inserts;

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    runner.new_group("sanity_check").run_benchmark(sanity_check());
    simple_inserts::run_all(&mut runner);
    insert_entities::run_all(&mut runner);
    insert_relations::run_all(&mut runner);
    runner.summary();
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
