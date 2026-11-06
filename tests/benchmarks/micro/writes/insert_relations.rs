/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use encoding::graph::type_::vertex::TypeID;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload,
    typedb_workload::{
        QueryDescriptor, RunDescriptor, TxType,
        benchmark::{PreloadDataFn, TypeDBQueryWorkloadBenchmark},
        datagen::{IDMaker, IntegerIDMaker, LongStringIDMaker, RandomDataGen, ShortStringIDMaker},
        run_configs::CI_MINIMAL,
    },
};
use query::given_rows::GivenRowEntry;

use crate::{SELECTED_RUN_DESCRIPTORS, assert_query_row_count_is};

const N_ENTITIES: usize = 100_000;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    run_ci_all(runner);

    run_by_insert(runner);
    run_by_concept(runner);
    run_by_id(runner);
}

fn run_ci_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("ci_tests_relations");
    group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_concept(), CI_MINIMAL));
    group.run_benchmark(parametrised_workload::<IntegerIDMaker>(
        relation_by_id::<IntegerIDMaker>("integer"),
        CI_MINIMAL,
    ));
    group.run_benchmark(parametrised_workload::<ShortStringIDMaker>(
        relation_by_id::<ShortStringIDMaker>("short_string"),
        CI_MINIMAL,
    ));
    group.run_benchmark(parametrised_workload::<LongStringIDMaker>(
        relation_by_id::<LongStringIDMaker>("long_string"),
        CI_MINIMAL,
    ));
}

pub fn run_by_concept(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("relations_by_concept");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_concept(), run_descriptor));
    }
}

pub fn run_by_id(runner: &mut impl BenchmarkRunner) {
    let mut ci_group = runner.new_group("relations_by_id");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        ci_group.run_benchmark(parametrised_workload::<IntegerIDMaker>(
            relation_by_id::<IntegerIDMaker>("integer"),
            run_descriptor,
        ));
        ci_group.run_benchmark(parametrised_workload::<ShortStringIDMaker>(
            relation_by_id::<ShortStringIDMaker>("short_string"),
            run_descriptor,
        ));
        ci_group.run_benchmark(parametrised_workload::<LongStringIDMaker>(
            relation_by_id::<LongStringIDMaker>("long_string"),
            run_descriptor,
        ));
        ci_group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_insert(), run_descriptor));
    }
}

pub fn run_by_insert(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("relations_by_insert");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_insert(), run_descriptor));
    }
}

fn produce_empty_row(_: &mut RandomDataGen) -> Vec<GivenRowEntry> {
    vec![]
}

fn produce_row_by_concept(rng: &mut RandomDataGen) -> Vec<GivenRowEntry> {
    vec![
        rng.entry_entity_raw_in(TypeID::new(0), 0, (N_ENTITIES - 1) as u64),
        rng.entry_entity_raw_in(TypeID::new(1), 0, (N_ENTITIES - 1) as u64),
    ]
}

fn produce_row_by_id<MID: IDMaker>(rng: &mut RandomDataGen) -> Vec<GivenRowEntry> {
    vec![
        MID::make_id(rng.integer_in(0, (N_ENTITIES - 1) as i64)),
        MID::make_id(rng.integer_in(0, (N_ENTITIES - 1) as i64)),
    ]
}

fn preload_entities_with_id<MakeID: IDMaker>() -> PreloadDataFn {
    fn preload_row<MID: IDMaker>(i: usize, _: &mut RandomDataGen) -> Vec<GivenRowEntry> {
        vec![MID::make_id(i as i64), MID::make_id(i as i64)]
    }
    let id_type = MakeID::ID_TYPE;
    TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
        format!(
            r#"
            given $id1: {id_type}, $id2: {id_type};
            insert $_ isa e1, has id == $id1; $_ isa e2, has id == $id2;
        "#
        ),
        vec!["id1".to_owned(), "id2".to_owned()],
        preload_row::<MakeID>,
        N_ENTITIES,
        10_000,
    )
}

// Workload definitions
fn parametrised_workload<MakeID: IDMaker>(
    query_descriptor: QueryDescriptor,
    run_descriptor: RunDescriptor,
) -> TypeDBQueryWorkloadBenchmark {
    let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
    let schema = MakeID::schema();
    let preload_data_fn = Some(preload_entities_with_id::<MakeID>());
    let dbg_assert = Some(assert_query_row_count_is(&run_descriptor));
    TypeDBQueryWorkloadBenchmark::new(name, schema, preload_data_fn, query_descriptor, run_descriptor, dbg_assert)
}

fn relation_by_concept() -> QueryDescriptor {
    let name = "relations_by_concept".to_owned();
    let query = r#"
        given $e1:e1, $e2: e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    .to_owned();
    let variables = vec!["e1".to_owned(), "e2".to_owned()];
    let tx_type = TxType::Write;
    QueryDescriptor { name, query, variables, produce_row: Some(produce_row_by_concept), tx_type }
}

fn relation_by_insert() -> QueryDescriptor {
    let name = "relations_by_insert".to_owned();
    let query = r#"
        given;
        insert $e1 isa e1; $e2 isa e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    .to_owned();
    let tx_type = TxType::Write;
    QueryDescriptor { name, query, variables: vec![], produce_row: Some(produce_empty_row), tx_type }
}

fn relation_by_id<MakeID: IDMaker>(id_name: &str) -> QueryDescriptor {
    let id_type = MakeID::ID_TYPE;
    let name = format!("relations_by_{id_name}_id");
    let query = format!(
        r#"
        given $id1: {id_type}, $id2: {id_type};
        match $e1 isa e1, has id == $id1; $e2 isa e2, has id == $id2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    );
    let variables = vec!["id1".to_owned(), "id2".to_owned()];
    let tx_type = TxType::Write;
    QueryDescriptor { name, query, variables, produce_row: Some(produce_row_by_id::<MakeID>), tx_type }
}
