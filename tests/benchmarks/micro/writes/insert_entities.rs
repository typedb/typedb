/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload,
    typedb_workload::{
        QueryDescriptor, RunDescriptor, TxType,
        benchmark::TypeDBQueryWorkloadBenchmark,
        run_configs::{PARALLEL_MANY_LARGE, PARALLEL_MANY_SMALL, SERIAL_FEW_LARGE, SERIAL_MANY_SMALL},
    },
};

use crate::{NO_INITIAL_DATA, SELECTED_RUN_DESCRIPTORS};

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("insert_entities");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        group.run_benchmark(parametrised_workload(entity_insert(), run_descriptor));
    }
}

fn parametrised_workload(
    query_descriptor: QueryDescriptor,
    run_descriptor: RunDescriptor,
) -> TypeDBQueryWorkloadBenchmark {
    pub(crate) const SCHEMA: &'static str = "define entity person";

    let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
    TypeDBQueryWorkloadBenchmark::new(name, SCHEMA, NO_INITIAL_DATA, query_descriptor, run_descriptor)
}

fn entity_insert() -> QueryDescriptor {
    QueryDescriptor {
        name: "entities".to_owned(),
        query: "given; insert $x isa person;".to_owned(),
        variables: vec![],
        produce_row: Some(|_| vec![]),
        tx_type: TxType::Write,
    }
}
