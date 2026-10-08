/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload::{QueryDescriptor, RunDescriptor, benchmark::TypeDBQueryWorkloadBenchmark},
};

use crate::{
    run_configs::{self, PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL},
    simple_inserts::{SCHEMA, no_initial_data},
};

const STANDARD_RUNS: [RunDescriptor; 4] =
    [SERIAL_MANY_SMALL, SERIAL_FEW_LARGE, PARALLEL_MANY_SMALL, PARALLEL_MANY_LARGE];

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("insert_entities");
    for run_descriptor in STANDARD_RUNS {
        group.run_benchmark(parametrised_workload(entity_insert(), run_descriptor));
    }
}

fn parametrised_workload(
    query_descriptor: QueryDescriptor,
    run_descriptor: RunDescriptor,
) -> TypeDBQueryWorkloadBenchmark {
    let name = run_configs::standardised_name(&query_descriptor, &run_descriptor);
    TypeDBQueryWorkloadBenchmark::new(name, SCHEMA, no_initial_data(), query_descriptor, run_descriptor)
}

fn entity_insert() -> QueryDescriptor {
    QueryDescriptor {
        name: "entities".to_owned(),
        query: "given; insert $x isa person;".to_owned(),
        variables: vec![],
        produce_row: Some(|_| vec![]),
    }
}
