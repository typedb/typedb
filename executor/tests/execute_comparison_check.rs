/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compiler::{
    annotation::{
        function::EmptyAnnotatedFunctionSignatures
        match_inference::infer_types,
    },
    executable::{
        match_::{
            instructions::{CheckInstruction, CheckVertex, ConstraintInstruction, Inputs, thing::IsaInstruction},
            planner::{
                function_plan::ExecutableFunctionRegistry,
                match_executable::{ExecutionStep, IntersectionStep, MatchExecutable},
            },
        },
        next_executable_id,
    },
    ExecutorVariable, VariablePosition,
};
use concept::type_::{annotation::AnnotationIndependent, attribute_type::AttributeTypeAnnotation};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{
    error::ReadExecutionError, ExecutionInterrupt, match_executor::MatchExecutor, pipeline::stage::ExecutionContext,
    profile::QueryProfile, row::MaybeOwnedRow,
};
use ir::{
    pattern::constraint::{Comparator, IsaKind},
    pipeline::{block::Block, ParameterRegistry},
    translation::TranslationContext,
};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, MVCCStorage, snapshot::CommittableSnapshot};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

const ATTRIBUTE_INDEPENDENT: AttributeTypeAnnotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);

fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();

    let age_type = type_manager.create_attribute_type(&mut snapshot, &AGE_LABEL).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
    age_type.set_annotation(&mut snapshot, &type_manager, &thing_manager, ATTRIBUTE_INDEPENDENT).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &NAME_LABEL).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    let _age = [10, 11, 12, 13, 14]
        .map(|age| thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(age)).unwrap());

    let _name = ["John", "Alice", "Leila"].map(|name| {
        thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed(name))).unwrap()
    });

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn attribute_equality() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);

    // query:
    //     $a isa age; $b isa age; $a == $b;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let var_age_a = conjunction.get_or_declare_variable("a").unwrap();
    let var_age_b = conjunction.get_or_declare_variable("b").unwrap();
    let var_age_type_a = conjunction.get_or_declare_variable("age-a").unwrap();
    let var_age_type_b = conjunction.get_or_declare_variable("age-b").unwrap();

    let isa_a =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age_a, var_age_type_a.into()).unwrap().clone();
    let isa_b =
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age_b, var_age_type_b.into()).unwrap().clone();
    conjunction.constraints_mut().add_label(var_age_type_a, AGE_LABEL.clone()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type_b, AGE_LABEL.clone()).unwrap();
    let entry = builder.finish().unwrap();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let variable_registry = &translation_context.variable_registry;
    let previous_stage_variable_annotations = &BTreeMap::new();
    let entry_annotations = infer_types(
        &snapshot,
        &entry,
        variable_registry,
        &type_manager,
        previous_stage_variable_annotations,
        &EmptyAnnotatedFunctionSignatures,
    )
    .unwrap();

    let row_vars = vec![var_age_a, var_age_b];
    let variable_positions =
        HashMap::from_iter(row_vars.iter().copied().enumerate().map(|(i, var)| (var, VariablePosition::new(i as u32))));
    let mapping = HashMap::from([var_age_a, var_age_type_a, var_age_b, var_age_type_b].map(|var| {
        if row_vars.contains(&var) {
            (var, ExecutorVariable::RowPosition(variable_positions[&var]))
        } else {
            (var, ExecutorVariable::Internal(var))
        }
    }));
    let named_variables = mapping.values().copied().collect();

    let mut isa_with_check = IsaInstruction::new(isa_b, Inputs::None([]), &entry_annotations);
    isa_with_check.checks.push(CheckInstruction::Comparison {
        lhs: CheckVertex::Variable(var_age_b),
        rhs: CheckVertex::Variable(var_age_a),
        comparator: Comparator::Equal,
    });

    // Plan
    let steps = vec![
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_age_a],
            vec![ConstraintInstruction::Isa(IsaInstruction::new(isa_a, Inputs::None([]), &entry_annotations))
                .map(&mapping)],
            vec![variable_positions[&var_age_a]],
            &named_variables,
            1,
        )),
        ExecutionStep::Intersection(IntersectionStep::new(
            mapping[&var_age_b],
            vec![ConstraintInstruction::Isa(isa_with_check).map(&mapping)],
            vec![variable_positions[&var_age_a], variable_positions[&var_age_b]],
            &named_variables,
            2,
        )),
    ];

    let executable = MatchExecutable::new(next_executable_id(), steps, variable_positions, row_vars);

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let executor = MatchExecutor::new(
        &executable,
        &snapshot,
        &thing_manager,
        MaybeOwnedRow::empty(),
        Arc::new(ExecutableFunctionRegistry::empty()),
        &QueryProfile::new(false),
    )
    .unwrap();

    let context = ExecutionContext::new(snapshot, thing_manager, Arc::default());
    let iterator = executor.into_iterator(context, ExecutionInterrupt::new_uninterruptible());

    let rows: Vec<Result<MaybeOwnedRow<'static>, Box<ReadExecutionError>>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| Box::new(err.clone()))).collect();
    assert_eq!(rows.len(), 5);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.multiplicity(), 1);
        print!("{}", row);
    }
}
