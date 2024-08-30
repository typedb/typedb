/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use compiler::match_::{
    inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
    instructions::{CheckInstruction, ConstraintInstruction, Inputs, IsaInstruction},
    planner::{
        pattern_plan::{IntersectionProgram, MatchProgram, Program},
        program_plan::ProgramPlan,
    },
};
use concept::{
    error::ConceptReadError,
    type_::{annotation::AnnotationIndependent, attribute_type::AttributeTypeAnnotation},
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use executor::{batch::ImmutableRow, program_executor::ProgramExecutor};
use ir::{
    pattern::constraint::{Comparator, IsaKind},
    program::block::FunctionalBlock,
    translation::TranslationContext,
};
use lending_iterator::LendingIterator;
use storage::{durability_client::WALClient, snapshot::CommittableSnapshot, MVCCStorage};

use crate::common::{load_managers, setup_storage};

mod common;

const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");

const ATTRIBUTE_INDEPENDENT: AttributeTypeAnnotation = AttributeTypeAnnotation::Independent(AnnotationIndependent);

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

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
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //     $a isa age; $b isa age; $a == $b;

    // IR
    let mut translation_context = TranslationContext::new();
    let mut builder = FunctionalBlock::builder(translation_context.next_block_context());
    let mut conjunction = builder.conjunction_mut();
    let var_age_a = conjunction.get_or_declare_variable("a").unwrap();
    let var_age_b = conjunction.get_or_declare_variable("b").unwrap();
    let var_age_type_a = conjunction.get_or_declare_variable("age-a").unwrap();
    let var_age_type_b = conjunction.get_or_declare_variable("age-b").unwrap();

    let isa_a = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age_a, var_age_type_a).unwrap().clone();
    let isa_b = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_age_b, var_age_type_b).unwrap().clone();
    conjunction.constraints_mut().add_label(var_age_type_a, AGE_LABEL.scoped_name().as_str()).unwrap();
    conjunction.constraints_mut().add_label(var_age_type_b, AGE_LABEL.scoped_name().as_str()).unwrap();
    let entry = builder.finish();

    let snapshot = storage.clone().open_snapshot_read();
    let (type_manager, thing_manager) = load_managers(storage.clone());
    let (entry_annotations, _) = infer_types(
        &entry,
        Vec::new(),
        &snapshot,
        &type_manager,
        &IndexedAnnotatedFunctions::empty(),
        &translation_context.variable_registry,
    )
    .unwrap();

    // Plan
    let steps = vec![
        Program::Intersection(IntersectionProgram::new(
            var_age_a,
            vec![ConstraintInstruction::Isa(IsaInstruction::new(isa_a, Inputs::None([]), &entry_annotations))],
            &[var_age_a],
        )),
        Program::Intersection(IntersectionProgram::new(
            var_age_b,
            vec![ConstraintInstruction::Isa(IsaInstruction::new(
                isa_b,
                Inputs::None([]),
                &entry_annotations,
                // TODO
                // vec![CheckInstruction::Comparison { lhs: var_age_b, rhs: var_age_a, comparator: Comparator::Equal }],
            ))],
            &[var_age_a, var_age_b],
        )),
    ];

    let pattern_plan = MatchProgram::new(steps, translation_context.variable_registry.clone());
    let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());

    // Executor
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let thing_manager = Arc::new(thing_manager);
    let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();

    let iterator = executor.into_iterator(snapshot, thing_manager);

    let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
        iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
    assert_eq!(rows.len(), 25);

    for row in rows {
        let row = row.unwrap();
        assert_eq!(row.get_multiplicity(), 1);
        print!("{}", row);
    }
}
