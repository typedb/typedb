/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use encoding::value::value::Value;
use lib_benchmark::{
    benchmark::{QueryDescriptor, RunDescriptor, TypeDBWorkloadReport, WorkloadInstance},
    datagen::RandomDataGen,
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
};
use query::given_rows::GivenRowEntry;

use crate::run_configs::SERIAL_FEW_LARGE;

pub fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("match_inserts");

    group.run_benchmark(serial_relations_by_short_string_id_few_large());
    group.run_benchmark(serial_relations_by_long_string_id_few_large());
    group.run_benchmark(serial_relations_by_integer_id_few_large());
}

trait IDMaker {
    fn make_id(id: i64) -> GivenRowEntry;
}

fn parametrised_binary_relation_by_id<MakeID: IDMaker>(
    name: &'static str,
    run_descriptor: RunDescriptor,
    id_type: &'static str,
) -> TypeDBWorkloadReport {
    fn preload_row<MID: IDMaker>(i: usize, _: &mut RandomDataGen) -> Vec<GivenRowEntry> {
        vec![MID::make_id(i as i64), MID::make_id(i as i64)]
    }

    const N_ENTITIES: usize = 100_000;
    let schema = format!(
        r#"
    define
        attribute id, value {id_type};
        relation r1, relates e1, relates e2;
        entity e1, plays r1:e1, owns id;
        entity e2, plays r1:e2, owns id;
    "#
    );

    let preload_data_fn = WorkloadInstance::make_preload_data_fn(
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
    );

    let query = format!(
        r#"
        given $id1: {id_type}, $id2: {id_type};
        match $e1 isa e1, has id == $id1; $e2 isa e2, has id == $id2;
        insert $r isa r1, links (e1: $e1, e2: $e2);
       "#
    );
    let variables = vec!["id1".to_owned(), "id2".to_owned()];
    fn produce_row<MID: IDMaker>(rng: &mut RandomDataGen) -> Vec<GivenRowEntry> {
        vec![
            MID::make_id(rng.integer_in(0, (N_ENTITIES - 1) as i64)),
            MID::make_id(rng.integer_in(0, (N_ENTITIES - 1) as i64)),
        ]
    }
    let query_descriptor = QueryDescriptor { query, variables, produce_row: Some(produce_row::<MakeID>) };

    TypeDBWorkloadReport::new(name, schema, Some(preload_data_fn), query_descriptor, run_descriptor)
}

fn serial_relations_by_short_string_id_few_large() -> TypeDBWorkloadReport {
    parametrised_binary_relation_by_id::<LongStringIDMaker>(
        "serial_relations_by_short_string_id_few_large",
        SERIAL_FEW_LARGE,
        "string",
    )
}

fn serial_relations_by_long_string_id_few_large() -> TypeDBWorkloadReport {
    parametrised_binary_relation_by_id::<LongStringIDMaker>(
        "serial_relations_by_long_string_id_few_large",
        SERIAL_FEW_LARGE,
        "string",
    )
}

fn serial_relations_by_integer_id_few_large() -> TypeDBWorkloadReport {
    parametrised_binary_relation_by_id::<IntegerIDMaker>(
        "serial_relations_by_integer_id_few_large",
        SERIAL_FEW_LARGE,
        "integer",
    )
}

struct LongStringIDMaker {}
impl IDMaker for LongStringIDMaker {
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("id_longer_than_16_bytes__{id}"))))
    }
}

struct ShortStringIDMaker {}
impl IDMaker for ShortStringIDMaker {
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(format!("smol_{id}"))))
    }
}

struct IntegerIDMaker {}
impl IDMaker for IntegerIDMaker {
    fn make_id(id: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(id))
    }
}
