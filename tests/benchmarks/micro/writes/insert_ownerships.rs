/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload,
    typedb_workload::{benchmark::TypeDBQueryWorkloadBenchmark, QueryDescriptor, RunDescriptor},
};
use lib_benchmark::id_maker::{IDMaker, IntegerIDMaker, LongStringIDMaker, ShortStringIDMaker};
use crate::{
    NO_INITIAL_DATA, SELECTED_RUN_DESCRIPTORS,
};

const MAX_ID_INTEGER: usize = 100_000_000;
pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("insert_entities");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(
            insert_ownership::<IntegerIDMaker>("int"),
            run_descriptor,
        ));
        group.run_benchmark(parametrised_workload::<ShortStringIDMaker>(
            insert_ownership::<ShortStringIDMaker>("short_str"),
            run_descriptor,
        ));
        group.run_benchmark(parametrised_workload::<LongStringIDMaker>(
            insert_ownership::<LongStringIDMaker>("long_str"),
            run_descriptor,
        ));
    }
}

fn parametrised_workload<MakeID: IDMaker>(
    query_descriptor: QueryDescriptor,
    run_descriptor: RunDescriptor,
) -> TypeDBQueryWorkloadBenchmark {
    let id_type = MakeID::ID_TYPE;
    let schema = format!(
        r#"
    define
        attribute id, value {id_type};
        entity person, owns id;
    "#
    );

    let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
    TypeDBQueryWorkloadBenchmark::new(name, schema, NO_INITIAL_DATA, query_descriptor, run_descriptor)
}

fn insert_ownership<MakeID: IDMaker>(id_name: &str) -> QueryDescriptor {
    let id_type = MakeID::ID_TYPE;
    QueryDescriptor {
        name: format!("ownerships_{id_name}_id"),
        query: format!("given $id_val: {id_type}; insert $x isa person, has id == $id_val;"),
        variables: vec!["id_val".to_owned()],
        produce_row: Some(|rng| vec![MakeID::make_id(rng.integer_in(0, (MAX_ID_INTEGER - 1) as i64))]),
    }
}
