/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::{
    benchmark::{QueryDescriptor, RunDescriptor, TypeDBQueryWorkloadBenchmark},
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
};

use crate::{
    run_configs::{PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL, standardised_name},
    simple_inserts::{SCHEMA, no_initial_data},
};

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("insert_entities");
    group.run_benchmark(serial_entities_few_large());
    group.run_benchmark(serial_entities_few_large_q10());
    group.run_benchmark(serial_entities_many_small());
    group.run_benchmark(parallel_entities_many_small());
    group.run_benchmark(parallel_entities_many_large());
}

fn parametrised_entity_insert(
    suggested_name: &'static str,
    run_descriptor: RunDescriptor,
) -> TypeDBQueryWorkloadBenchmark {
    let query = "given; insert $x isa person;".to_owned();
    let name = "entities".to_owned();
    let query_descriptor = QueryDescriptor { name, query, variables: vec![], produce_row: Some(|_| vec![]) };
    let name = standardised_name(&query_descriptor, &run_descriptor);
    assert_eq!(name, suggested_name);
    TypeDBQueryWorkloadBenchmark::new(name, SCHEMA, no_initial_data(), query_descriptor, run_descriptor)
}

fn serial_entities_few_large() -> TypeDBQueryWorkloadBenchmark {
    parametrised_entity_insert("serial_entities_few_large", SERIAL_FEW_LARGE)
}

fn serial_entities_few_large_q10() -> TypeDBQueryWorkloadBenchmark {
    // FLAGGED: 10 queries/tx has no constant in run_configs
    parametrised_entity_insert(
        "serial_entities_few_large_queries[10]",
        RunDescriptor { n_queries_per_tx: 10, ..SERIAL_FEW_LARGE },
    )
}

fn serial_entities_many_small() -> TypeDBQueryWorkloadBenchmark {
    parametrised_entity_insert("serial_entities_many_small", SERIAL_MANY_SMALL)
}

fn parallel_entities_many_small() -> TypeDBQueryWorkloadBenchmark {
    parametrised_entity_insert("parallel_entities_many_small", PARALLEL_MANY_SMALL)
}

fn parallel_entities_many_large() -> TypeDBQueryWorkloadBenchmark {
    parametrised_entity_insert("parallel_entities_many_large", PARALLEL_MANY_LARGE)
}
