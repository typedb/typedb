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

use crate::{
    run_configs::{
        PARALLEL_MANY_LARGE, PARALLEL_MANY_MEDIUM, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL,
    },
    simple_inserts::{SCHEMA as SIMPLE_SCHEMA, no_initial_data},
};

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("serial_inserts");
    group.run_benchmark(serial_entities_few_large());
    group.run_benchmark(serial_entities_few_large_10q());
    group.run_benchmark(serial_entities_many_small());

    let mut group = runner.new_group("parallel_inserts");
    group.run_benchmark(parallel_entities_many_small());
    group.run_benchmark(parallel_entities_many_large());
    group.run_benchmark(parallel_relations_many_medium());
}

fn parametrised_entity_insert(name: &'static str, run_descriptor: RunDescriptor) -> TypeDBWorkloadBenchmark {
    let query_descriptor =
        QueryDescriptor { query: "given; insert $x isa person;", variables: vec![], produce_row: Some(|_| vec![]) };
    TypeDBWorkloadBenchmark::new(name, SIMPLE_SCHEMA, no_initial_data(), query_descriptor, run_descriptor)
}

// Serial

fn serial_entities_few_large() -> TypeDBWorkloadBenchmark {
    parametrised_entity_insert("serial_entities_few_large", SERIAL_FEW_LARGE)
}

fn serial_entities_few_large_10q() -> TypeDBWorkloadBenchmark {
    // FLAGGED: 10 queries/tx has no constant in run_configs
    parametrised_entity_insert(
        "serial_entities_few_large_10q",
        RunDescriptor { n_queries_per_tx: 10, ..SERIAL_FEW_LARGE },
    )
}

fn serial_entities_many_small() -> TypeDBWorkloadBenchmark {
    parametrised_entity_insert("serial_entities_many_small", SERIAL_MANY_SMALL)
}

// Parallel
fn parallel_entities_many_small() -> TypeDBWorkloadBenchmark {
    parametrised_entity_insert("parallel_entities_many_small", PARALLEL_MANY_SMALL)
}

fn parallel_entities_many_large() -> TypeDBWorkloadBenchmark {
    parametrised_entity_insert("parallel_entities_many_large", PARALLEL_MANY_LARGE)
}

fn parallel_relations_many_medium() -> TypeDBWorkloadBenchmark {
    parametrised_parallel_binary_relation("parallel_relations_many_medium", PARALLEL_MANY_MEDIUM)
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

    let query = r#"
        given $e1:e1, $e2: e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#;
    let variables = vec!["e1".to_owned(), "e2".to_owned()];
    let query_descriptor = QueryDescriptor { query, variables, produce_row: Some(produce_row) };

    TypeDBWorkloadBenchmark::new(name, schema, Some(preload_data_fn), query_descriptor, run_descriptor)
}
