/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use clap::Parser;
use database::Database;
use lib_benchmark::{
    benchmark::{PreloadDataFn, PrepareRunFn, sanity_check},
    datagen::RandomDataGen,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup, SimpleRunner},
};
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

mod heavy_inserts;
// mod match_reads;
mod parallel_heavy_inserts;
mod simple_inserts;

// Initial data
pub fn no_initial_data() -> Option<PreloadDataFn> {
    None
}

// prepare_run
pub fn no_given_rows() -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(|_: Arc<Database<WALClient>>| None)
}

pub fn n_empty_given_rows(n: usize) -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = Vec::new();
        let mut rows = Vec::with_capacity(n);
        rows.resize(n, Vec::new());
        Some(GivenRowsSimple { variables, rows })
    })
}

pub fn given_rows_with(
    n_rows: usize,
    variables: Vec<String>,
    gen_row: fn(&mut RandomDataGen) -> Vec<GivenRowEntry>,
) -> PrepareRunFn<Option<GivenRowsSimple>> {
    Box::new(move |_: Arc<Database<WALClient>>| {
        let variables = variables.clone();
        let mut rows = Vec::with_capacity(n_rows);
        let mut rng = RandomDataGen::new();
        let gen_row = move || gen_row(&mut rng);
        rows.resize_with(n_rows, gen_row);
        Some(GivenRowsSimple { variables, rows })
    })
}

fn run_benchmarks(mut runner: impl BenchmarkRunner) {
    runner.new_group("sanity_check").run_benchmark(sanity_check());
    simple_inserts::run_all(&mut runner);
    heavy_inserts::run_all(&mut runner);
    parallel_heavy_inserts::run_all(&mut runner);

    runner.summary();
}

fn main() {
    run_benchmarks(SimpleRunner::parse());
}
