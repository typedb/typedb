/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::vertex::TypeID;
use lib_benchmark::{
    benchmark::{QueryDescriptor, RunDescriptor, TypeDBWorkloadBenchmark, WorkloadInstance},
    datagen::RandomDataGen,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
};
use query::given_rows::GivenRowEntry;

use crate::{no_initial_data, simple_inserts::SCHEMA as SIMPLE_SCHEMA};

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("parallel_inserts");
    group.run_benchmark(parallel_many_small_tx());
    group.run_benchmark(parallel_many_average_tx());
    group.run_benchmark(parallel_many_large_tx());
    group.run_benchmark(parallel_binary_relation());
}

fn parametrised_parallel_entity_insert(name: &'static str, run_descriptor: RunDescriptor) -> TypeDBWorkloadBenchmark {
    let query_descriptor =
        QueryDescriptor { query: "given; insert $x isa person;", variables: vec![], produce_row: Some(|_| vec![]) };
    TypeDBWorkloadBenchmark::new(name, SIMPLE_SCHEMA, no_initial_data(), query_descriptor, run_descriptor)
}

fn parallel_many_small_tx() -> TypeDBWorkloadBenchmark {
    let run_descriptor =
        RunDescriptor { parallelism: 8, total_txns: 100_000, n_queries_per_tx: 1, n_rows_per_query: 1 };
    parametrised_parallel_entity_insert("parallel_many_small_tx", run_descriptor)
}

fn parallel_many_average_tx() -> TypeDBWorkloadBenchmark {
    let run_descriptor =
        RunDescriptor { parallelism: 8, total_txns: 1_000, n_queries_per_tx: 1, n_rows_per_query: 100 };
    parametrised_parallel_entity_insert("parallel_many_average_tx", run_descriptor)
}

fn parallel_many_large_tx() -> TypeDBWorkloadBenchmark {
    let run_descriptor =
        RunDescriptor { parallelism: 8, total_txns: 1_000, n_queries_per_tx: 1, n_rows_per_query: 10_000 };
    parametrised_parallel_entity_insert("parallel_many_large_tx", run_descriptor)
}

fn parametrised_parallel_binary_relation(name: &'static str, run_descriptor: RunDescriptor) -> TypeDBWorkloadBenchmark {
    const N_ENTITIES: usize = 100_000;
    fn produce_row(rng: &mut RandomDataGen) -> Vec<GivenRowEntry> {
        vec![
            rng.entry_entity_raw_in(TypeID::new(0), 0, (N_ENTITIES - 1) as u64),
            rng.entry_entity_raw_in(TypeID::new(1), 0, (N_ENTITIES - 1) as u64),
        ]
    }
    let schema = r#"
    define
        relation r1, relates e1, relates e2;
        entity e1, plays r1:e1;
        entity e2, plays r1:e2;
    "#;

    let preload_data_fn = WorkloadInstance::make_preload_data_fn(
        "given; insert $_ isa e1; $_ isa e2;",
        vec![],
        |_| vec![],
        N_ENTITIES,
        10_000,
    );

    // Query
    let query = r#"
        given $e1:e1, $e2: e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#;
    let variables = vec!["e1".to_owned(), "e2".to_owned()];
    let query_descriptor = QueryDescriptor { query, variables, produce_row: Some(produce_row) };

    TypeDBWorkloadBenchmark::new(name, schema, Some(preload_data_fn), query_descriptor, run_descriptor)
}

fn parallel_binary_relation() -> TypeDBWorkloadBenchmark {
    let run_descriptor =
        RunDescriptor { parallelism: 16, total_txns: 1_000, n_queries_per_tx: 1, n_rows_per_query: 1_000 };
    parametrised_parallel_binary_relation("parallel_binary_relation", run_descriptor)
}
