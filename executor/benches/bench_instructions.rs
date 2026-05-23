/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use answer::variable::Variable;
use compiler::{
    ExecutorVariable, VariablePosition,
    annotation::{
        PipelineAnnotationContext, function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types_for_block,
        pipeline::RunningVariableAnnotations,
    },
    executable::match_::instructions::{
        Inputs, VariableMode, VariableModes,
        thing::{HasInstruction, HasReverseInstruction},
    },
};
use concept::{
    thing::object::ObjectAPI,
    type_::{Ordering, OwnerAPI, annotation::AnnotationCardinality, owns::OwnsAnnotation},
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{HasExecutor, HasReverseExecutor, TupleIterator, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};
use ir::{
    pattern::constraint::IsaKind,
    pipeline::{ParameterRegistry, block::Block},
    translation::PipelineTranslationContext,
};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const NUM_PERSONS: usize = 10_000;
const AGES_PER_PERSON: usize = 5;
const NAMES_PER_PERSON: usize = 5;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, PERSON_LABEL.get().unwrap()).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, AGE_LABEL.get().unwrap()).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, NAME_LABEL.get().unwrap()).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    for attribute_type in [age_type, name_type] {
        let owns = person_type
            .set_owns(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                attribute_type,
                Ordering::Unordered,
                StorageCounters::DISABLED,
            )
            .unwrap();
        owns.set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
        )
        .unwrap();
    }

    let mut next_id: i64 = 0;
    for _ in 0..NUM_PERSONS {
        let person = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        for _ in 0..AGES_PER_PERSON {
            let age = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(next_id)).unwrap();
            person.set_has_unordered(&mut snapshot, &thing_manager, &age, StorageCounters::DISABLED).unwrap();
            next_id += 1;
        }
        for _ in 0..NAMES_PER_PERSON {
            let name = thing_manager
                .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned(format!("n{}", next_id))))
                .unwrap();
            person.set_has_unordered(&mut snapshot, &thing_manager, &name, StorageCounters::DISABLED).unwrap();
            next_id += 1;
        }
    }

    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

struct BenchVars {
    person: ExecutorVariable,
    attribute: ExecutorVariable,
    mapping: HashMap<Variable, ExecutorVariable>,
    variable_modes: VariableModes,
}

fn build_has_ir(multi_attribute_type: bool) -> (Block, BenchVars, PipelineTranslationContext, ParameterRegistry) {
    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_attribute_type = conjunction.constraints_mut().get_or_declare_variable("attribute_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_attribute = conjunction.constraints_mut().get_or_declare_variable("attribute", None).unwrap();
    conjunction.constraints_mut().add_has(var_person, var_attribute, None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_attribute, var_attribute_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.get().unwrap().clone()).unwrap();
    if !multi_attribute_type {
        conjunction.constraints_mut().add_label(var_attribute_type, AGE_LABEL.get().unwrap().clone()).unwrap();
    }
    drop(conjunction);
    let entry = builder.finish().unwrap();

    let person = ExecutorVariable::RowPosition(VariablePosition::new(0));
    let attribute = ExecutorVariable::RowPosition(VariablePosition::new(1));
    let mapping: HashMap<Variable, ExecutorVariable> = HashMap::from([
        (var_person, person),
        (var_attribute, attribute),
        (var_person_type, ExecutorVariable::Internal(var_person_type)),
        (var_attribute_type, ExecutorVariable::Internal(var_attribute_type)),
    ]);
    let mut variable_modes = VariableModes::new();
    variable_modes.insert(person, VariableMode::Output);
    variable_modes.insert(attribute, VariableMode::Output);

    (entry, BenchVars { person, attribute, mapping, variable_modes }, translation_context, value_parameters)
}

fn build_has_unbound_executor(
    storage: &Arc<MVCCStorage<WALClient>>,
) -> (HasExecutor, ExecutionContext<storage::snapshot::ReadSnapshot<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.snapshot_watermark()));
    let (entry, vars, mut translation_context, value_parameters) = build_has_ir(false);

    let snapshot = storage.clone().open_snapshot_read();
    let mut ctx = PipelineAnnotationContext::new(
        &snapshot,
        &type_manager,
        &EmptyAnnotatedFunctionSignatures,
        &mut translation_context.variable_registry,
        &value_parameters,
    );
    let previous_annotations = RunningVariableAnnotations::empty();
    let block_annotations = infer_types_for_block(&mut ctx, &previous_annotations, &entry, false).unwrap();
    let entry_annotations = block_annotations.type_annotations_of(entry.conjunction()).unwrap();
    let has = entry.conjunction().constraints().iter().find_map(|c| c.as_has()).unwrap().clone();

    let has_instruction = HasInstruction::new(has, Inputs::None([]), &entry_annotations).map(&vars.mapping);
    // sort_by = person → BinaryIterateMode::Unbound
    let executor =
        HasExecutor::new(has_instruction, vars.variable_modes, vars.person, &snapshot, &thing_manager).unwrap();
    let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::default());
    (executor, context)
}

fn build_has_reverse_unbound_executor(
    storage: &Arc<MVCCStorage<WALClient>>,
    multi_attribute_type: bool,
) -> (HasReverseExecutor, ExecutionContext<storage::snapshot::ReadSnapshot<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.snapshot_watermark()));
    let (entry, vars, mut translation_context, value_parameters) = build_has_ir(multi_attribute_type);

    let snapshot = storage.clone().open_snapshot_read();
    let mut ctx = PipelineAnnotationContext::new(
        &snapshot,
        &type_manager,
        &EmptyAnnotatedFunctionSignatures,
        &mut translation_context.variable_registry,
        &value_parameters,
    );
    let previous_annotations = RunningVariableAnnotations::empty();
    let block_annotations = infer_types_for_block(&mut ctx, &previous_annotations, &entry, false).unwrap();
    let entry_annotations = block_annotations.type_annotations_of(entry.conjunction()).unwrap();
    let has = entry.conjunction().constraints().iter().find_map(|c| c.as_has()).unwrap().clone();

    let has_reverse_instruction =
        HasReverseInstruction::new(has, Inputs::None([]), &entry_annotations).map(&vars.mapping);
    // sort_by = attribute → BinaryIterateMode::Unbound for HasReverse (from=attribute, to=owner)
    let executor = HasReverseExecutor::new(
        has_reverse_instruction,
        vars.variable_modes,
        vars.attribute,
        &snapshot,
        &thing_manager,
    )
    .unwrap();
    let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::default());
    (executor, context)
}

fn assert_count(mut iter: TupleIterator, expected_count: usize) {
    let mut count = 0;
    while let Some(result) = iter.peek() {
        result.as_ref().unwrap();
        iter.advance_single().unwrap();
        count += 1;
    }
    assert_eq!(count, expected_count);
}

fn criterion_benchmark(c: &mut Criterion) {
    PERSON_LABEL.set(Label::new_static("person")).unwrap();
    AGE_LABEL.set(Label::new_static("age")).unwrap();
    NAME_LABEL.set(Label::new_static("name")).unwrap();
    init_logging();

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    let (has_exec, has_ctx) = build_has_unbound_executor(&storage);
    let (has_reverse_single_exec, has_reverse_single_ctx) = build_has_reverse_unbound_executor(&storage, false);
    let (has_reverse_multi_exec, has_reverse_multi_ctx) = build_has_reverse_unbound_executor(&storage, true);
    let count_single = NUM_PERSONS * AGES_PER_PERSON;
    let count_multi = NUM_PERSONS * (AGES_PER_PERSON + NAMES_PER_PERSON);

    let mut group = c.benchmark_group("has_unbound");
    group.sampling_mode(SamplingMode::Linear);
    group.bench_function("single_type", |b| {
        b.iter(|| {
            let iter = has_exec
                .get_iterator(&has_ctx, MaybeOwnedRow::empty(), StorageCounters::DISABLED)
                .unwrap();
            assert_count(iter, count_single);
        })
    });
    group.finish();

    let mut group = c.benchmark_group("has_reverse_unbound");
    group.sampling_mode(SamplingMode::Linear);
    group.bench_function("single_type", |b| {
        b.iter(|| {
            let iter = has_reverse_single_exec
                .get_iterator(&has_reverse_single_ctx, MaybeOwnedRow::empty(), StorageCounters::DISABLED)
                .unwrap();
            assert_count(iter, count_single);
        })
    });
    group.bench_function("multi_type", |b| {
        b.iter(|| {
            let iter = has_reverse_multi_exec
                .get_iterator(&has_reverse_multi_ctx, MaybeOwnedRow::empty(), StorageCounters::DISABLED)
                .unwrap();
            assert_count(iter, count_multi);
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
