/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::marker::PhantomData;

use encoding::graph::type_::vertex::TypeID;
use lib_benchmark::{
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload,
    typedb_workload::{
        QueryDescriptor, RunDescriptor, TxType,
        benchmark::{DbgAssertFn, PreloadDataFn, TypeDBQueryWorkloadBenchmark},
        datagen::{IDMaker, IntegerIDMaker, RandomDataGen},
        run_configs::CI_MINIMAL,
    },
};
use query::given_rows::GivenRowEntry;

use crate::{
    PARALLEL_RUN_LARGE, SCALE_LARGE, SCALE_MEDIUM, SCALE_SMALL, SERIAL_RUN_LARGE, assert_query_row_count_is,
    assert_query_row_count_within,
};

const MANY_RATIO_SMALL: usize = 100;
const MANY_RATIO_LARGE: usize = 10_000;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    run_for_ci(runner);

    let mut group = runner.new_group("constraints");
    for run_descriptor in [SERIAL_RUN_LARGE, PARALLEL_RUN_LARGE] {
        run_one_to_one_for::<{ SCALE_SMALL }>(&mut group, run_descriptor);
        run_one_to_one_for::<{ SCALE_MEDIUM }>(&mut group, run_descriptor);
        run_one_to_one_for::<{ SCALE_LARGE }>(&mut group, run_descriptor);

        run_many_to_one_for::<{ SCALE_SMALL }, { MANY_RATIO_SMALL }>(&mut group, run_descriptor);
        run_many_to_one_for::<{ SCALE_MEDIUM }, { MANY_RATIO_SMALL }>(&mut group, run_descriptor);
        run_many_to_one_for::<{ SCALE_LARGE }, { MANY_RATIO_SMALL }>(&mut group, run_descriptor);

        run_many_to_one_for::<{ SCALE_SMALL }, { MANY_RATIO_LARGE }>(&mut group, run_descriptor);
        run_many_to_one_for::<{ SCALE_MEDIUM }, { MANY_RATIO_LARGE }>(&mut group, run_descriptor);
        run_many_to_one_for::<{ SCALE_LARGE }, { MANY_RATIO_LARGE }>(&mut group, run_descriptor);
    }
}

fn run_for_ci(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("ci_tests_constraints");
    run_one_to_one_for::<{ SCALE_SMALL }>(&mut group, CI_MINIMAL);
    run_many_to_one_for::<{ SCALE_SMALL }, { MANY_RATIO_SMALL }>(&mut group, CI_MINIMAL);
}

fn run_one_to_one_for<const SCALE_FACTOR: usize>(group: &mut impl BenchmarkRunnerGroup, run_descriptor: RunDescriptor) {
    group.run_benchmark(ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::parametrised_ownership(
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::ownership_lookups(format!("integer_1to1_s{SCALE_FACTOR}")),
        run_descriptor,
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::preload_ownerships_one_to_one(),
    ));
    group.run_benchmark(ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::parametrised_relations(
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::relation_lookups(format!("1to1_s{SCALE_FACTOR}")),
        run_descriptor,
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::preload_relations_one_per_role1(),
    ));
}

fn run_many_to_one_for<const SCALE_FACTOR: usize, const MANY_RATIO: usize>(
    group: &mut impl BenchmarkRunnerGroup,
    run_descriptor: RunDescriptor,
) {
    let scale_many_name = format!("s{SCALE_FACTOR}_m{MANY_RATIO}");
    group.run_benchmark(ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::parametrised_ownership(
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::ownership_lookups(format!("integer_Nto1_{scale_many_name}")),
        run_descriptor,
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::preload_ownerships_many_owners_per_attribute(MANY_RATIO),
    ));
    group.run_benchmark(ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::parametrised_relations(
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::relation_lookups(format!("Nto1_{scale_many_name}")),
        run_descriptor,
        ScaleHelper::<SCALE_FACTOR, IntegerIDMaker>::preload_relations_many_relations_per_entity(MANY_RATIO),
    ));
}

struct ScaleHelper<const SCALE_FACTOR: usize, MakeID: IDMaker = IntegerIDMaker>(PhantomData<MakeID>);
impl<const SCALE_FACTOR: usize, MakeID: IDMaker> ScaleHelper<SCALE_FACTOR, MakeID> {
    fn create_workload(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
        preload: PreloadDataFn,
        dbg_assert: DbgAssertFn,
    ) -> TypeDBQueryWorkloadBenchmark {
        let id_type = MakeID::ID_TYPE;
        let schema = format!(
            r#"
        define
            attribute id @independent, value {id_type};
            relation rel, relates role1, relates role2;
            entity e1, owns id,
                plays rel:role1, plays rel:role2;
        "#
        );
        let preload = Some(preload);
        let dbg_assert = Some(dbg_assert);
        let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
        TypeDBQueryWorkloadBenchmark::new(name, schema, preload, query_descriptor, run_descriptor, dbg_assert)
    }

    fn parametrised_ownership(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
        preload: PreloadDataFn,
    ) -> TypeDBQueryWorkloadBenchmark {
        let dbg_assert = assert_query_row_count_is(&run_descriptor);
        Self::create_workload(query_descriptor, run_descriptor, preload, dbg_assert)
    }

    fn parametrised_relations(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
        preload: PreloadDataFn,
    ) -> TypeDBQueryWorkloadBenchmark {
        let expected_rows =
            run_descriptor.n_rows_per_query as f64 * (run_descriptor.total_rows() as f64 / SCALE_FACTOR as f64);
        let dbg_assert = assert_query_row_count_within(expected_rows, 10.0);
        Self::create_workload(query_descriptor, run_descriptor, preload, dbg_assert)
    }

    // Ownerships
    fn ownership_lookups(name: String) -> QueryDescriptor {
        let id_type = MakeID::ID_TYPE;
        QueryDescriptor {
            name: format!("ownership_lookups_{name}_id"),
            query: format!("given $id_val: {id_type}; match $x isa e1, has id == $id_val;"),
            variables: vec!["id_val".to_owned()],
            produce_row: Some(|rng| vec![MakeID::make_id(rng.integer_in(0, (SCALE_FACTOR - 1) as i64))]),
            tx_type: TxType::Read,
        }
    }

    fn preload_ownerships_one_to_one() -> PreloadDataFn {
        fn preload_row<MID: IDMaker>(i: usize, _: &mut RandomDataGen) -> Vec<GivenRowEntry> {
            vec![MID::make_id(i as i64)]
        }
        let id_type = MakeID::ID_TYPE;
        TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            format!(
                r#"
                given $id: {id_type};
                insert $_ isa e1, has id == $id;
            "#
            ),
            vec!["id".to_owned()],
            preload_row::<MakeID>,
            SCALE_FACTOR,
            10_000,
        )
    }

    fn preload_ownerships_many_owners_per_attribute(many_ratio: usize) -> PreloadDataFn {
        fn produce_i_as_id<MID: IDMaker>(i: usize, _: &mut RandomDataGen) -> Vec<GivenRowEntry> {
            vec![MID::make_id(i as i64)]
        }
        fn pick_random_id<const SCALE_FACTOR: usize, MID: IDMaker>(
            _: usize,
            rng: &mut RandomDataGen,
        ) -> Vec<GivenRowEntry> {
            vec![MID::make_id(rng.integer_in(0, (SCALE_FACTOR - 1) as i64))]
        }
        let id_type = MakeID::ID_TYPE;
        let load_attributes = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            format!(
                r#"
                given $id: {id_type};
                insert $_ isa id == $id;
            "#
            ),
            vec!["id".to_owned()],
            produce_i_as_id::<MakeID>,
            SCALE_FACTOR,
            10_000,
        );
        let load_owners = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            format!(
                r#"
                given $id: {id_type};
                insert $_ isa e1, has id == $id;
            "#
            ),
            vec!["id".to_owned()],
            pick_random_id::<SCALE_FACTOR, MakeID>,
            SCALE_FACTOR * many_ratio,
            10_000,
        );
        Box::new(move |db| {
            (*load_attributes)(db.clone());
            (*load_owners)(db.clone());
        })
    }

    // Relations
    fn relation_lookups(name: String) -> QueryDescriptor {
        QueryDescriptor {
            name: format!("relation_lookups_{name}"),
            query: "given $e1: e1; match $_ isa rel, links (role1: $e1, role2: $e2);".to_owned(),
            variables: vec!["e1".to_owned()],
            produce_row: Some(|rng| vec![rng.entry_entity_raw_in(TypeID::new(0), 0, (SCALE_FACTOR - 1) as u64)]),
            tx_type: TxType::Read,
        }
    }

    fn preload_relations_one_per_role1() -> PreloadDataFn {
        fn produce_i_and_random_entity<const SCALE_FACTOR: usize>(
            i: usize,
            datagen: &mut RandomDataGen,
        ) -> Vec<GivenRowEntry> {
            vec![
                datagen.entry_entity_raw_with_id(TypeID::new(0), i as u64),
                datagen.entry_entity_raw_in(TypeID::new(0), 0, (SCALE_FACTOR - 1) as u64),
            ]
        }

        let load_entities = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            "given; insert $_ isa e1;".to_owned(),
            vec![],
            |_, _| vec![],
            SCALE_FACTOR,
            10_000,
        );

        let load_relations = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            r#"
                given $e1: e1, $e2: e1;
                insert $_ isa rel, links (role1: $e1, role2: $e2);
            "#
            .to_owned(),
            vec!["e1".to_owned(), "e2".to_owned()],
            produce_i_and_random_entity::<SCALE_FACTOR>,
            SCALE_FACTOR,
            10_000,
        );

        Box::new(move |db| {
            (*load_entities)(db.clone());
            (*load_relations)(db.clone());
        })
    }

    fn preload_relations_many_relations_per_entity(many_ratio: usize) -> PreloadDataFn {
        fn produce_two_random_entities<const SCALE_FACTOR: usize>(
            _i: usize,
            datagen: &mut RandomDataGen,
        ) -> Vec<GivenRowEntry> {
            vec![
                datagen.entry_entity_raw_in(TypeID::new(0), 0, (SCALE_FACTOR - 1) as u64),
                datagen.entry_entity_raw_in(TypeID::new(0), 0, (SCALE_FACTOR - 1) as u64),
            ]
        }

        let load_entities = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            "given; insert $_ isa e1;".to_owned(),
            vec![],
            |_, _| vec![],
            SCALE_FACTOR,
            10_000,
        );

        let load_relations = TypeDBQueryWorkloadBenchmark::make_preload_data_fn(
            r#"
                given $e1: e1, $e2: e1;
                insert $_ isa rel, links (role1: $e1, role2: $e2);
            "#
            .to_owned(),
            vec!["e1".to_owned(), "e2".to_owned()],
            produce_two_random_entities::<SCALE_FACTOR>,
            SCALE_FACTOR * many_ratio,
            10_000,
        );

        Box::new(move |db| {
            (*load_entities)(db.clone());
            (*load_relations)(db.clone());
        })
    }
}
