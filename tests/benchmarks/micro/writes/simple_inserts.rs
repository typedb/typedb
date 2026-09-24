/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use lib_benchmark::{
    benchmark::{
        TypeDBMicroBenchmark, given_rows_with, n_empty_given_rows, no_given_rows, no_initial_data, query_in_write_tx,
    },
    profiling::TxQueryProfile,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
};
use lib_benchmark::benchmark::{QueryDescriptor, RunDescriptor, WorkLoad};
use query::given_rows::GivenRowsSimple;

pub type TransactionInsertBenchmark = TypeDBMicroBenchmark<Option<GivenRowsSimple>, TxQueryProfile>;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("simple_inserts");
    group.run_benchmark(entities_one());
    group.run_benchmark(entities_many());
    group.run_benchmark(ownerships_many_names_short());
    group.run_benchmark(ownerships_many_names_long());
}

pub(crate) const SCHEMA: &'static str = r#"
define
    attribute name, value string;
    entity person, owns name;
"#;

const N_ROWS: usize = 100_000;

fn simple_insert_workload(query_descriptor: QueryDescriptor) ->  {

    let run_descriptor = RunDescriptor {
        parallelism: 1,
        total_txns: 1,
        n_queries_per_tx: 1,
        n_rows_per_query: N_ROWS,
    };
    let workload = WorkLoad { query_descriptor, run_descriptor };
}

fn entities_one() -> TransactionInsertBenchmark {
    let query_descriptor = QueryDescriptor {
        query: "insert $x isa person;",
        variables: vec![],
        produce_row: None,
    };
    let run_descriptor = RunDescriptor {
        parallelism: 1,
        total_txns: 1,
        n_queries_per_tx: 1,
        n_rows_per_query: N_ROWS,
    };
    let workload = WorkLoad { query_descriptor, run_descriptor };
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_one",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: workload.for_warmup(),
        prepare_run_fn: workload.prepare_fn(),
        benchmark_fn: workload.runner(),
    }
}

fn entities_many() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__entities_many",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: n_empty_given_rows(N_ROWS),
        benchmark_fn: query_in_write_tx("given ; insert $x isa person;"),
    }
}

fn ownerships_many_names_short() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_many_short_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: given_rows_with(N_ROWS, vec!["name".to_owned()], |rng| vec![rng.entry_string(5)]),
        benchmark_fn: query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}

fn ownerships_many_names_long() -> TransactionInsertBenchmark {
    TransactionInsertBenchmark {
        name: "simple_inserts__ownerships_many_long_names",
        schema: SCHEMA,
        preload_data_fn: no_initial_data(),
        warmup_fn: None,
        prepare_run_fn: given_rows_with(N_ROWS, vec!["name".to_owned()], |rng| vec![rng.entry_string(50)]),
        benchmark_fn: query_in_write_tx("given $name: string; insert $x isa person, has name == $name;"),
    }
}
