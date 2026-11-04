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
        benchmark::{PreloadDataFn, TypeDBQueryWorkloadBenchmark, WorkloadInstance},
        datagen::RandomDataGen,
    },
};
use query::given_rows::GivenRowEntry;

use crate::{PRELOAD_SCALING_FACTOR, SELECTED_RUN_DESCRIPTORS};

const SCALE_SWITCH: usize = 1_000_000;
struct ScaleHelper<const N_ENTITIES: usize, MakeID: IDMaker = IntegerIDMaker>(PhantomData<MakeID>);
const SMALL_SCALE: usize = 100_000;
const LARGE_SCALE: usize = 10_000_000;

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

fn run_all_for<const N_ENTITIES: usize>(group: &mut impl BenchmarkRunnerGroup, run_descriptor: RunDescriptor) {
    group.run_benchmark(ScaleHelper::<N_ENTITIES, IntegerIDMaker>::parametrised_ownership(
        ScaleHelper::<N_ENTITIES, IntegerIDMaker>::ownership_lookups("integer"),
        run_descriptor,
    ));

    group.run_benchmark(ScaleHelper::<N_ENTITIES, IntegerIDMaker>::parametrised_relations(
        ScaleHelper::<N_ENTITIES, IntegerIDMaker>::relation_lookups(),
        run_descriptor,
    ));
}

impl<const N_ENTITIES: usize, MakeID: IDMaker> ScaleHelper<N_ENTITIES, MakeID> {
    fn parametrised_ownership(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
    ) -> TypeDBQueryWorkloadBenchmark {
        let id_type = MakeID::ID_TYPE;
        let schema = format!(
            r#"
        define
            attribute id, value {id_type};
            entity e1, owns id;
        "#
        );
        let preload_fn = Some(Self::preload_ownerships(&run_descriptor));
        let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
        TypeDBQueryWorkloadBenchmark::new(name, schema, preload_fn, query_descriptor, run_descriptor)
    }

    fn preload_ownerships(descriptor: &RunDescriptor) -> PreloadDataFn {
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
            descriptor.total_rows() * PRELOAD_SCALING_FACTOR,
            10_000,
        )
    }

    fn ownership_lookups(id_name: &str) -> QueryDescriptor {
        let id_type = MakeID::ID_TYPE;
        QueryDescriptor {
            name: format!("ownership_lookups_{id_name}_id"),
            query: format!("given $id_val: {id_type}; match $x isa e1, has id == $id_val;"),
            variables: vec!["id_val".to_owned()],
            produce_row: Some(|rng| vec![MakeID::make_id(rng.integer_in(0, (N_ENTITIES - 1) as i64))]),
            tx_type: TxType::Read,
        }
    }

    fn parametrised_relations(
        query_descriptor: QueryDescriptor,
        run_descriptor: RunDescriptor,
    ) -> TypeDBQueryWorkloadBenchmark {
        let schema = r#"
        define
            relation rel, relates role1, relates role2;
            entity e1, plays rel:role1, plays rel:role2;
        "#
        .to_owned();
        let preload_fn = Some(Self::preload_relations(&run_descriptor));
        let name = typedb_workload::run_configs::standardised_name(&query_descriptor, &run_descriptor);
        TypeDBQueryWorkloadBenchmark::new(name, schema, preload_fn, query_descriptor, run_descriptor)
    }

    fn preload_relations(descriptor: &RunDescriptor) -> PreloadDataFn {
        fn produce_two_random_entities<const N_ENTITIES: usize>(
            i: usize,
            datagen: &mut RandomDataGen,
        ) -> Vec<GivenRowEntry> {
            vec![
                datagen.entry_entity_raw_with_id(TypeID::new(0), i as u64),
                datagen.entry_entity_raw_in(TypeID::new(0), 0, N_ENTITIES as u64),
            ]
        }
        let total_relations = descriptor.total_rows() * PRELOAD_SCALING_FACTOR;
        assert!(total_relations < (N_ENTITIES * N_ENTITIES) / 1000);

        let load_entities = WorkloadInstance::make_preload_data_fn(
            "given; insert $_ isa e1;".to_owned(),
            vec![],
            |_, _| vec![],
            N_ENTITIES,
            10_000,
        );

        let load_relations = WorkloadInstance::make_preload_data_fn(
            r#"
                given $e1: e1, $e2: e1;
                insert $_ isa rel, links (role1: $e1, role2: $e2);
            "#
            .to_owned(),
            vec!["e1".to_owned(), "e2".to_owned()],
            produce_two_random_entities::<N_ENTITIES>,
            total_relations,
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
            produce_row: Some(|rng| vec![rng.entry_entity_raw_in(TypeID::new(0), 0, N_ENTITIES as u64)]),
            tx_type: TxType::Read,
        }
    }
}
