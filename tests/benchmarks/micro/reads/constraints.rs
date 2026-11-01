/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use lib_benchmark::runner::BenchmarkRunner;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("constraints");
}

// fn parametrised_workload<MakeID: IDMaker>(
//     query_descriptor: QueryDescriptor,
//     run_descriptor: RunDescriptor,
// ) -> TypeDBQueryWorkloadBenchmark {
//     let id_type = MakeID::ID_TYPE;
//     let schema = format!(
//         r#"
//     define
//         attribute id, value {id_type};
//         entity person, owns id;
//     "#
//     );
//
//     let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
//     TypeDBQueryWorkloadBenchmark::new(name, schema, NO_INITIAL_DATA, query_descriptor, run_descriptor)
// }
