/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::borrow::Cow;

use encoding::{graph::type_::vertex::TypeID, value::value::Value};
use lib_benchmark::{
    benchmark::{
        PreloadDataFn, QueryDescriptor, RunDescriptor, TypeDBQueryWorkloadBenchmark, TypeDBWorkloadBenchmark,
        WorkloadInstance,
    },
    datagen::RandomDataGen,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
};
use query::given_rows::GivenRowEntry;

use crate::{
    run_configs,
    run_configs::{PARALLEL_MANY_LARGE, PARALLEL_MANY_MEDIUM, SERIAL_FEW_LARGE, SERIAL_MANY_MEDIUM},
};

const N_ENTITIES: usize = 100_000;

const STANDARD_RUNS: [RunDescriptor; 4] =
    [SERIAL_MANY_MEDIUM, SERIAL_FEW_LARGE, PARALLEL_MANY_MEDIUM, PARALLEL_MANY_LARGE];

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    run_by_insert(runner);
    run_by_concept(runner);
    run_by_id(runner);
}

pub fn run_by_concept(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("relations_by_concept");
    for run_descriptor in STANDARD_RUNS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_concept(), run_descriptor));
    }
}

pub fn run_by_id(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("relations_by_id");
    for run_descriptor in STANDARD_RUNS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(
            relation_by_id::<IntegerIDMaker>("integer"),
            run_descriptor.clone(),
        ));
        group.run_benchmark(parametrised_workload::<ShortStringIDMaker>(
            relation_by_id::<ShortStringIDMaker>("short_string"),
            run_descriptor.clone(),
        ));
        group.run_benchmark(parametrised_workload::<LongStringIDMaker>(
            relation_by_id::<LongStringIDMaker>("long_string"),
            run_descriptor.clone(),
        ));
    }
}

pub fn run_by_insert(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("relations_by_insert");
    for run_descriptor in STANDARD_RUNS {
        group.run_benchmark(parametrised_workload::<IntegerIDMaker>(relation_by_insert(), run_descriptor));
    }
}

// Helpers
trait IDMaker {
    const ID_TYPE: &'static str;
    fn make_id(id: i64) -> GivenRowEntry;

    fn schema() -> String {
        let id_type = Self::ID_TYPE;
        format!(
            r#"
            define
                attribute id, value {id_type};
                relation r1, relates e1, relates e2;
                entity e1, plays r1:e1, owns id;
                entity e2, plays r1:e2, owns id;
            "#
        )
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
    WorkloadInstance::make_preload_data_fn(
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
    let name = run_configs::standardised_name(&query_descriptor, &run_descriptor);
    let schema = MakeID::schema();
    let preload_data_fn = Some(preload_entities_with_id::<MakeID>());
    TypeDBQueryWorkloadBenchmark::new(name, schema, preload_data_fn, query_descriptor, run_descriptor)
}

fn relation_by_concept() -> QueryDescriptor {
    let name = "relations_by_concept".to_owned();
    let query = r#"
        given $e1:e1, $e2: e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    .to_owned();
    let variables = vec!["e1".to_owned(), "e2".to_owned()];
    QueryDescriptor { name, query, variables, produce_row: Some(produce_row_by_concept) }
}

fn relation_by_insert() -> QueryDescriptor {
    let name = "relations_by_insert".to_owned();
    let query = r#"
        given;
        insert $e1 isa e1; $e2 isa e2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    .to_owned();
    QueryDescriptor { name, query, variables: vec![], produce_row: Some(produce_empty_row) }
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
    QueryDescriptor { name, query, variables, produce_row: Some(produce_row_by_id::<MakeID>) }
}

struct LongStringIDMaker {}
impl IDMaker for LongStringIDMaker {
    const ID_TYPE: &'static str = "string";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("id_longer_than_16_bytes__{id}"))))
    }
}

struct ShortStringIDMaker {}
impl IDMaker for ShortStringIDMaker {
    const ID_TYPE: &'static str = "string";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("smol_{id}"))))
    }
}

struct IntegerIDMaker {}
impl IDMaker for IntegerIDMaker {
    const ID_TYPE: &'static str = "integer";
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(id))
    }
}
