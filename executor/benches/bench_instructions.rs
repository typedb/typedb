/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
};

use answer::variable::Variable;
use compiler::{
    ExecutorVariable, VariablePosition,
    annotation::{
        PipelineAnnotationContext, function::EmptyAnnotatedFunctionSignatures, match_inference::infer_types_for_block,
        pipeline::RunningVariableAnnotations,
    },
    executable::match_::{
        instructions::{ConstraintInstruction, Inputs, thing::HasInstruction},
        planner::conjunction_executable::IntersectionStep,
    },
};
use concept::{
    thing::object::ObjectAPI,
    type_::{Ordering, OwnerAPI, annotation::AnnotationCardinality, owns::OwnsAnnotation},
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use compiler::executable::match_::instructions::{VariableMode, VariableModes};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{HasExecutor, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};
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

const NUM_PERSONS: usize = 1000;
const ATTRS_PER_PERSON: usize = 10;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let person_type = type_manager.create_entity_type(&mut snapshot, PERSON_LABEL.get().unwrap()).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, AGE_LABEL.get().unwrap()).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let owns = person_type
        .set_owns(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            age_type,
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

    let mut next_age: i64 = 0;
    for _ in 0..NUM_PERSONS {
        let person = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        for _ in 0..ATTRS_PER_PERSON {
            let age = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(next_age)).unwrap();
            person.set_has_unordered(&mut snapshot, &thing_manager, &age, StorageCounters::DISABLED).unwrap();
            next_age += 1;
        }
    }

    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn position_mapping<const N: usize, const M: usize>(
    row_vars: [Variable; N],
    internal_vars: [Variable; M],
) -> (HashMap<Variable, VariablePosition>, HashMap<Variable, ExecutorVariable>, HashSet<ExecutorVariable>) {
    let variable_positions: HashMap<_, _> =
        row_vars.iter().enumerate().map(|(i, var)| (*var, VariablePosition::new(i as u32))).collect();
    let mapping: HashMap<_, _> = row_vars
        .iter()
        .map(|var| (*var, ExecutorVariable::RowPosition(variable_positions[var])))
        .chain(internal_vars.iter().map(|var| (*var, ExecutorVariable::Internal(*var))))
        .collect();
    let named_variables = mapping.values().copied().collect();
    (variable_positions, mapping, named_variables)
}

fn build_has_executor(
    storage: &Arc<MVCCStorage<WALClient>>,
) -> (HasExecutor, ExecutionContext<storage::snapshot::ReadSnapshot<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.snapshot_watermark()));

    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();
    let var_age_type = conjunction.constraints_mut().get_or_declare_variable("age_type", None).unwrap();
    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_age = conjunction.constraints_mut().get_or_declare_variable("age", None).unwrap();
    let has = conjunction.constraints_mut().add_has(var_person, var_age, None).unwrap().clone();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age, var_age_type.into(), None).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, PERSON_LABEL.get().unwrap().clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type, AGE_LABEL.get().unwrap().clone()).unwrap();
    let entry = builder.finish().unwrap();

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

    let (variable_positions, mapping, named_variables) =
        position_mapping([var_person, var_age], [var_person_type, var_age_type]);
    let sort_by = mapping[&var_person];

    let has_instruction = HasInstruction::new(has, Inputs::None([]), &entry_annotations).map(&mapping);
    let mut variable_modes = VariableModes::new();
    variable_modes.insert(*mapping.get(&var_person).unwrap(), VariableMode::Output);
    variable_modes.insert(*mapping.get(&var_age).unwrap(), VariableMode::Output);
    let executor = HasExecutor::new(has_instruction, variable_modes, sort_by, &snapshot, &thing_manager).unwrap();
    let context = ExecutionContext::new(Arc::new(snapshot), thing_manager, Arc::default());
    (executor, context)
}

fn criterion_benchmark(c: &mut Criterion) {
    PERSON_LABEL.set(Label::new_static("person")).unwrap();
    AGE_LABEL.set(Label::new_static("age")).unwrap();
    init_logging();

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    let (executor, context) = build_has_executor(&storage);
    let expected_count = NUM_PERSONS * ATTRS_PER_PERSON;

    let mut group = c.benchmark_group("has_executor");
    group.sampling_mode(SamplingMode::Linear);
    group.bench_function("unbound_sorted_from", |b| {
        b.iter(|| {
            let count = executor.drain_count(&context, MaybeOwnedRow::empty(), StorageCounters::DISABLED).unwrap();
            assert_eq!(count, expected_count);
        });
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
