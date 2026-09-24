/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;
use database::Database;
use database::transaction::TransactionWrite;
use lib_benchmark::{commit, execute_write_query_in, runner::{BenchmarkRunner, BenchmarkRunnerGroup},  QueryAnswer};
use lib_benchmark::benchmark::{BenchmarkedFn, TypeDBMicroBenchmark};
use lib_benchmark::profiling::TxQueryProfile;
use lib_benchmark::utils::{unpack_result, CountResults};
use options::TransactionOptions;
use query::given_rows::GivenRowsSimple;
use storage::durability_client::WALClient;
use crate::{given_rows_with, n_empty_given_rows, no_given_rows, no_initial_data};

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
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
