/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::marker::PhantomData;

use encoding::graph::type_::vertex::TypeID;
use lib_benchmark::{
    id_maker::{IDMaker, IntegerIDMaker},
    runner::{BenchmarkRunner, BenchmarkRunnerGroup},
    typedb_workload,
    typedb_workload::{
        QueryDescriptor, RunDescriptor, TxType,
        benchmark::{DbgAssertFn, PreloadDataFn, TypeDBQueryWorkloadBenchmark, WorkloadInstance},
        datagen::RandomDataGen,
    },
};
use query::given_rows::GivenRowEntry;

use crate::{SELECTED_RUN_DESCRIPTORS, assert_query_row_count_is, assert_query_row_count_within};

const SCALE_SWITCH: usize = 50_000;
struct ScaleHelper<const N_ENTITIES: usize, MakeID: IDMaker = IntegerIDMaker>(PhantomData<MakeID>);
const SMALL_SCALE: usize = 100_000;
const LARGE_SCALE: usize = 1_000_000;

pub(crate) fn run_all(runner: &mut impl BenchmarkRunner) {
    let mut group = runner.new_group("constraints");
    for &run_descriptor in SELECTED_RUN_DESCRIPTORS {
        if run_descriptor.total_rows() < SCALE_SWITCH {
            run_all_for::<SMALL_SCALE>(&mut group, run_descriptor);
        } else {
            run_all_for::<LARGE_SCALE>(&mut group, run_descriptor);
        }
    }
}

fn run_all_for<const PRE_LOAD_SIZE: usize>(group: &mut impl BenchmarkRunnerGroup, run_descriptor: RunDescriptor) {
    group.run_benchmark(ScaleHelper::<PRE_LOAD_SIZE, IntegerIDMaker>::parametrised_ownership(
        ScaleHelper::<PRE_LOAD_SIZE, IntegerIDMaker>::ownership_lookups("integer"),
        run_descriptor,
    ));

    group.run_benchmark(ScaleHelper::<PRE_LOAD_SIZE, IntegerIDMaker>::parametrised_relations(
        ScaleHelper::<PRE_LOAD_SIZE, IntegerIDMaker>::relation_lookups(),
        run_descriptor,
    ));
}

impl<const PRELOAD_SIZE: usize, MakeID: IDMaker> ScaleHelper<PRELOAD_SIZE, MakeID> {
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
            attribute id, value {id_type};
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

    fn ownership_lookups(id_name: &str) -> QueryDescriptor {
        let id_type = MakeID::ID_TYPE;
        QueryDescriptor {
            name: format!("ownership_lookups_{id_name}_id"),
            query: format!("given $id_val: {id_type}; match $x isa e1, has id == $id_val;"),
            variables: vec!["id_val".to_owned()],
            produce_row: Some(|rng| vec![MakeID::make_id(rng.integer_in(0, (PRELOAD_SIZE - 1) as i64))]),
            tx_type: TxType::Read,
        }
    }

    fn parametrised_ownership(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
    ) -> TypeDBQueryWorkloadBenchmark {
        let preload = Self::preload_ownerships_one_to_one();
        let dbg_assert = assert_query_row_count_is(&run_descriptor);
        Self::create_workload(query_descriptor, run_descriptor, preload, dbg_assert)
    }

    fn preload_ownerships_one_to_one() -> PreloadDataFn {
        fn preload_row<MID: IDMaker>(i: usize, _: &mut RandomDataGen) -> Vec<GivenRowEntry> {
            vec![MID::make_id(i as i64)]
        }
        let id_type = MakeID::ID_TYPE;
        WorkloadInstance::make_preload_data_fn(
            format!(
                r#"
                given $id: {id_type};
                insert $_ isa e1, has id == $id;
            "#
            ),
            vec!["id".to_owned()],
            preload_row::<MakeID>,
            PRELOAD_SIZE,
            10_000,
        )
    }

    fn parametrised_relations(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
    ) -> TypeDBQueryWorkloadBenchmark {
        let preload = Self::preload_relations_one_per_role1(&run_descriptor);
        let expected_rows =
            run_descriptor.n_rows_per_query as f64 * (run_descriptor.total_rows() as f64 / PRELOAD_SIZE as f64);
        let dbg_assert = assert_query_row_count_within(expected_rows, 10.0);
        Self::create_workload(query_descriptor, run_descriptor, preload, dbg_assert)
    }

    fn preload_relations_one_per_role1(descriptor: &RunDescriptor) -> PreloadDataFn {
        fn produce_i_and_random_entity<const N_ENTITIES: usize>(
            i: usize,
            datagen: &mut RandomDataGen,
        ) -> Vec<GivenRowEntry> {
            vec![
                datagen.entry_entity_raw_with_id(TypeID::new(0), i as u64),
                datagen.entry_entity_raw_in(TypeID::new(0), 0, (N_ENTITIES - 1) as u64),
            ]
        }
        let total_relations = descriptor.total_rows();
        assert!(total_relations < (PRELOAD_SIZE * PRELOAD_SIZE) / 1000);

        let load_entities = WorkloadInstance::make_preload_data_fn(
            "given; insert $_ isa e1;".to_owned(),
            vec![],
            |_, _| vec![],
            PRELOAD_SIZE,
            10_000,
        );

        let load_relations = WorkloadInstance::make_preload_data_fn(
            r#"
                given $e1: e1, $e2: e1;
                insert $_ isa rel, links (role1: $e1, role2: $e2);
            "#
            .to_owned(),
            vec!["e1".to_owned(), "e2".to_owned()],
            produce_i_and_random_entity::<PRELOAD_SIZE>,
            PRELOAD_SIZE,
            10_000,
        );

        Box::new(move |db| {
            (*load_entities)(db.clone());
            (*load_relations)(db.clone());
        })
    }

    fn relation_lookups() -> QueryDescriptor {
        QueryDescriptor {
            name: "relation_lookups".to_owned(),
            query: "given $e1: e1; match $_ isa rel, links (role1: $e1, role2: $e2);".to_owned(),
            variables: vec!["e1".to_owned()],
            produce_row: Some(|rng| vec![rng.entry_entity_raw_in(TypeID::new(0), 0, (PRELOAD_SIZE - 1) as u64)]),
            tx_type: TxType::Read,
        }
    }
}
