/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use database::{Database, transaction::TransactionWrite};
use lib_benchmark::{
    QueryAnswer,
    benchmark::{BenchmarkedFn, PreloadDataFn, PrepareRunFn, TypeDBMicroBenchmark},
    commit,
    datagen::RandomDataGen,
    execute_write_query_in,
    profiling::TxQueryProfile,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    utils::{CountResults, unpack_result},
};
use options::TransactionOptions;
use query::given_rows::{GivenRowEntry, GivenRowsSimple};
use storage::durability_client::WALClient;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    // I'm mainly keeping this file around as an alternate to using TypeDBWorkloadBenchmark
    let mut group = runner.new_group("simple_inserts");
    group.run_benchmark(entities_one());
    group.run_benchmark(entities_thousand());
    group.run_benchmark(ownerships_thousand_names_short());
    group.run_benchmark(ownerships_thousand_names_long());
}

pub(crate) const SCHEMA: &'static str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

const N_ROWS: usize = 100_000;

fn _query_in_write_tx(query: &str) -> BenchmarkedFn<Option<GivenRowsSimple>, TxQueryProfile> {
    let query_owned = query.to_owned();
    Box::new(move |database: Arc<Database<WALClient>>, given_rows: Option<GivenRowsSimple>| {
        let tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
        let (query_result, tx) =
            unpack_result(execute_write_query_in::<_, CountResults>(tx, query_owned.as_str(), given_rows, true));
        let QueryAnswer { profile: query_profile, answer: _ } = query_result.unwrap();
        let tx_profile = commit(tx).unwrap();
        TxQueryProfile { tx_profile: Some(tx_profile), query_profile }
    })
}

fn entities_one() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_one",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: no_given_rows(),
        benchmark_fn: _query_in_write_tx("insert $x isa person;"),
    }
}

fn entities_thousand() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_thousand",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: n_empty_given_rows(N_ROWS),
        benchmark_fn: _query_in_write_tx("given ; insert $x isa person;"),
    }
}

fn ownerships_thousand_names_short() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_thousand_short_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: given_rows_with(N_ROWS, vec!["name".to_owned()], |rng| vec![rng.entry_string(5)]),
        benchmark_fn: _query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}

fn ownerships_thousand_names_long() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_thousand_long_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: given_rows_with(N_ROWS, vec!["name".to_owned()], |rng| vec![rng.entry_string(50)]),
        benchmark_fn: _query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}

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
