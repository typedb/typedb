/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::{
    graph::definition::definition_key::{DefinitionID, DefinitionKey},
    layout::prefix::Prefix,
};
use ir::{
    pattern::{
        constraint::IsaKind,
        variable_category::{VariableCategory, VariableOptionality},
    },
    program::{
        block::FunctionalBlock,
        function_signature::{FunctionID, FunctionSignature},
        modifier::ModifierDefinitionError,
        program::Program,
    },
};

#[test]
fn build_modifiers() {
    let mut builder = FunctionalBlock::builder();
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_has(var_person, var_name).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, "person").unwrap();
    conjunction.constraints_mut().add_label(var_name_type, "name").unwrap();

    builder.add_limit(10);
    builder.add_sort(vec![("person", true), ("name", false)]).unwrap();

    let block = builder.finish();

    let _ = Program::new(block, Vec::new());
}

#[test]
fn build_invalid_modifiers() {
    let mut builder = FunctionalBlock::builder();
    let mut conjunction = builder.conjunction_mut();

    let person_name = String::from("bob");
    let var_person = conjunction.get_or_declare_variable(&person_name).unwrap();
    let var_name = conjunction.get_or_declare_variable("name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_has(var_person, var_name).unwrap();
    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(var_person_type, "person").unwrap();
    conjunction.constraints_mut().add_label(var_name_type, "name").unwrap();

    let result = builder.add_sort(vec![("bob", true), ("jane", false)]);
    assert!(
        matches!(&result, Err(ModifierDefinitionError::SortVariableNotAvailable { name }) if name == "jane"),
        "{result:?}"
    );
}

#[test]
fn build_program_with_functions() {
    let mut builder = FunctionalBlock::builder();
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.get_or_declare_variable("person").unwrap();
    let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();

    let var_count = conjunction.get_or_declare_variable("count").unwrap();
    let var_mean = conjunction.get_or_declare_variable("sum").unwrap();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type).unwrap();

    let function_argument_categories = vec![(VariableCategory::Object, VariableOptionality::Required)];
    let function_return_categories = vec![
        (VariableCategory::Value, VariableOptionality::Required),
        (VariableCategory::Value, VariableOptionality::Optional),
    ];
    let function_signature = FunctionSignature::new(
        FunctionID::Schema(DefinitionKey::build(Prefix::DefinitionStruct, DefinitionID::build(1000))),
        function_argument_categories,
        function_return_categories,
        false,
    );
    conjunction
        .constraints_mut()
        .add_function_call(vec![var_count, var_mean], &function_signature, vec![var_person])
        .unwrap();
    let block = builder.finish();
    println!("{}", block.conjunction());

    // TODO: incomplete, since we don't have the called function IR
    // let program = Program::new(Pattern::Conjunction(conjunction), HashMap::new());
}
