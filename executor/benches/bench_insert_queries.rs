/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    collections::HashMap,
    ffi::c_int,
    fs::File,
    path::Path,
    sync::{Arc, OnceLock},
};

use criterion::{Criterion, criterion_group, criterion_main, profiler::Profiler, SamplingMode};
use pprof::ProfilerGuard;

use answer::variable_value::VariableValue;
use compiler::insert::insert::build_insert_plan;
use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use compiler::match_::inference::type_inference::infer_types;
use compiler::VariablePosition;
use concept::{
    thing::{thing_manager::ThingManager},
    type_::{Ordering, OwnerAPI, PlayerAPI, type_manager::TypeManager},
};
use encoding::value::{label::Label, value_type::ValueType};
use executor::batch::Row;
use executor::write::insert::InsertExecutor;
use executor::write::WriteError;
use ir::translation::TranslationContext;
use storage::{
    durability_client::WALClient,
    MVCCStorage,
    snapshot::{CommittableSnapshot, WritableSnapshot, WriteSnapshot},
};
use test_utils::init_logging;

use crate::common::{load_managers, setup_storage};

mod common;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_MEMBER_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();

fn setup_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let person_type = type_manager.create_entity_type(&mut snapshot, PERSON_LABEL.get().unwrap()).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, GROUP_LABEL.get().unwrap()).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, MEMBERSHIP_LABEL.get().unwrap()).unwrap();
    let relates_member = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_MEMBER_LABEL.get().unwrap().name().as_str(),
            Ordering::Unordered,
            None,
        )
        .unwrap();
    let membership_member_type = relates_member.role();
    let relates_group = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_GROUP_LABEL.get().unwrap().name().as_str(),
            Ordering::Unordered,
            None,
        )
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, AGE_LABEL.get().unwrap()).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, NAME_LABEL.get().unwrap()).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type.clone()).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_group_type.clone()).unwrap();

    snapshot.commit().unwrap();
}

fn execute_insert(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    query_str: &str,
    input_row_var_names: &Vec<&str>,
    input_rows: Vec<Vec<VariableValue<'static>>>,
) -> Result<Vec<Vec<VariableValue<'static>>>, WriteError> {
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let mut translation_context = TranslationContext::new();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
    let input_row_format = input_row_var_names
        .iter()
        .enumerate()
        .map(|(i, v)| (translation_context.visible_variables.get(*v).unwrap().clone(), VariablePosition::new(i as u32)))
        .collect::<HashMap<_, _>>();
    let (entry_annotations, _) = infer_types(
        &block,
        vec![],
        snapshot,
        type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    ).unwrap();

    let mut insert_plan = build_insert_plan(
        block.conjunction().constraints(), &input_row_format, &entry_annotations
    ).unwrap();

    let mut output_rows = Vec::with_capacity(input_rows.len());
    let output_width = insert_plan.output_row_plan.len();
    let mut insert_executor = InsertExecutor::new(insert_plan);
    for mut input_row in input_rows {
        let mut output_multiplicity = 1;
        output_rows.push(
            (0..output_width)
                .map(|i| input_row.get(i).map_or_else(|| VariableValue::Empty, |existing| existing.clone()))
                .collect::<Vec<_>>(),
        );
        insert_executor.execute_insert(
            snapshot,
            &thing_manager,
            &mut Row::new(output_rows.last_mut().unwrap(), &mut output_multiplicity),
        )?;
    }
    Ok(output_rows)
}

fn criterion_benchmark(c: &mut Criterion) {
    PERSON_LABEL.set(Label::new_static("person")).unwrap();
    GROUP_LABEL.set(Label::new_static("group")).unwrap();
    MEMBERSHIP_LABEL.set(Label::new_static("membership")).unwrap();
    MEMBERSHIP_MEMBER_LABEL.set(Label::new_static_scoped("member", "membership", "membership:member")).unwrap();
    MEMBERSHIP_GROUP_LABEL.set(Label::new_static_scoped("group", "membership", "membership:group")).unwrap();
    AGE_LABEL.set(Label::new_static("age")).unwrap();
    NAME_LABEL.set(Label::new_static("name")).unwrap();
    init_logging();

    let mut group = c.benchmark_group("test insert queries");
    // group.sample_size(1000);
    // group.measurement_time(Duration::from_secs(20));
    group.sampling_mode(SamplingMode::Linear);

    let (_tmp_dir, storage) = setup_storage();
    setup_schema(storage.clone());
    let (type_manager, thing_manager) = load_managers(storage.clone());

    group.bench_function("insert_queries", |b| {
        b.iter(|| {
            let mut snapshot = storage.clone().open_snapshot_write();
            let age: u32 = rand::random();
            let row = execute_insert(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                &format!("insert $p isa person, has age {age};"),
                &vec![],
                vec![vec![]],
            )
            .unwrap();
            snapshot.commit().unwrap();
        });
    });
}

pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        Self { frequency, active_profiler: None }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(flamegraph_path).expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            profiler.report().build().unwrap().flamegraph(flamegraph_file).expect("Error writing flamegraph");
        }
    }
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(10))
}

// criterion_group!(
//     name = benches;
//     config= profiled();
//     targets = criterion_benchmark
// );

// TODO: disable profiling when running on mac, since pprof seems to crash sometimes?
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
