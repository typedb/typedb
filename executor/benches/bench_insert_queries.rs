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
    vec,
};

use compiler::match_::inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types};
use concept::{
    thing::thing_manager::ThingManager,
    type_::{type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use criterion::{criterion_group, criterion_main, profiler::Profiler, Criterion, SamplingMode};
use encoding::value::{label::Label, value_type::ValueType};
use executor::{
    pipeline::{
        initial::InitialStage,
        insert::InsertStageExecutor,
        stage::{StageAPI, StageContext},
        PipelineExecutionError,
    },
    row::MaybeOwnedRow,
    write::WriteError,
};
use ir::translation::TranslationContext;
use lending_iterator::LendingIterator;
use pprof::ProfilerGuard;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_MEMBER_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();
    let mut snapshot = storage.clone().open_snapshot_write();
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

fn execute_insert<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    query_str: &str,
) -> Result<(Vec<MaybeOwnedRow<'static>>, Snapshot), WriteError> {
    let mut translation_context = TranslationContext::new();
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline().stages.pop().unwrap().into_insert();
    let block = ir::translation::writes::translate_insert(&mut translation_context, &typeql_insert).unwrap();
    let input_row_format = HashMap::new();

    let (entry_annotations, _) = infer_types(
        &block,
        vec![],
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    let variable_registry = Arc::new(translation_context.variable_registry);

    let insert_plan = compiler::insert::program::compile(
        variable_registry,
        block.conjunction().constraints(),
        &input_row_format,
        &entry_annotations,
    )
    .unwrap();

    println!("Insert Vertex:\n{:?}", &insert_plan.concept_instructions);
    println!("Insert Edges:\n{:?}", &insert_plan.connection_instructions);

    println!("Insert output row schema: {:?}", &insert_plan.output_row_schema);

    let snapshot = Arc::new(snapshot);
    let initial = InitialStage::new(StageContext { snapshot, thing_manager, parameters: Arc::default() });
    let insert_executor = InsertStageExecutor::new(insert_plan, initial);
    let (output_iter, context) = insert_executor.into_iterator().map_err(|(err, _)| match err {
        PipelineExecutionError::WriteError { source } => source,
        _ => unreachable!(),
    })?;
    let output_rows = output_iter
        .map_static(|res| res.map(|row| row.into_owned()))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| match err {
            PipelineExecutionError::WriteError { source } => source,
            _ => unreachable!(),
        })?;
    Ok((output_rows, Arc::into_inner(context.snapshot).unwrap()))
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
    // group.measurement_time(Duration::from_secs(200));
    group.sampling_mode(SamplingMode::Linear);

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.read_watermark()));

    group.bench_function("insert_queries", |b| {
        b.iter(|| {
            let snapshot = storage.clone().open_snapshot_write();
            let age: u32 = rand::random();
            let (_, snapshot) = execute_insert(
                snapshot,
                type_manager.clone(),
                thing_manager.clone(),
                &format!("insert $p isa person, has age {age};"),
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
