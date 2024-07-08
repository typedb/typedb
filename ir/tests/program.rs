/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap};

use answer::variable::Variable;
use encoding::{
    graph::definition::definition_key::{DefinitionID, DefinitionKey},
    layout::prefix::Prefix,
};
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::IsaKind,
        function_call::FunctionCall,
        variable_category::{VariableCategory, VariableOptionality},
        ScopeId,
    },
    program::{
        block::{BlockContext, FunctionalBlock},
        modifier::ModifierDefinitionError,
        program::Program,
    },
};

#[test]
fn build_program_modifiers() {
    let mut context = BlockContext::new();
    let mut conjunction = Conjunction::new(ScopeId::ROOT);

    let var_person = conjunction.get_or_declare_variable(&mut context, "person").unwrap();
    let var_name = conjunction.get_or_declare_variable(&mut context, "name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&mut context, "person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable(&mut context, "name_type").unwrap();

    conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_has(&mut context, var_person, var_name).unwrap();
    conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(&mut context, var_person_type, "person").unwrap();
    conjunction.constraints_mut().add_label(&mut context, var_name_type, "name").unwrap();

    let mut block = FunctionalBlock::from_raw_parts(context, conjunction, Vec::new());
    block.add_limit(10);
    block.add_sort(vec![("person", true), ("name", false)]).unwrap();

    let _ = Program::new(block, HashMap::new());
}

#[test]
fn build_invalid_program_modifiers() {
    let person_name = String::from("bob");

    let mut context = BlockContext::new();
    let mut conjunction = Conjunction::new(ScopeId::ROOT);

    let var_person = conjunction.get_or_declare_variable(&mut context, &person_name).unwrap();
    let var_name = conjunction.get_or_declare_variable(&mut context, "name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&mut context, "person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable(&mut context, "name_type").unwrap();

    conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_person, var_person_type).unwrap();
    conjunction.constraints_mut().add_has(&mut context, var_person, var_name).unwrap();
    conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_name, var_name_type).unwrap();
    conjunction.constraints_mut().add_label(&mut context, var_person_type, "person").unwrap();
    conjunction.constraints_mut().add_label(&mut context, var_name_type, "name").unwrap();

    let mut block = FunctionalBlock::from_raw_parts(context, conjunction, Vec::new());
    let result = block.add_sort(vec![("bob", true), ("jane", false)]);
    assert!(
        matches!(&result, Err(ModifierDefinitionError::SortVariableNotAvailable { name }) if name == "jane"),
        "{result:?}"
    );
    let _ = Program::new(block, HashMap::new());
}

#[test]
fn build_program_with_functions() {
    let mut context = BlockContext::new();
    let mut conjunction = Conjunction::new(ScopeId::ROOT);

    let var_person = conjunction.get_or_declare_variable(&mut context, "person").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&mut context, "person_type").unwrap();

    let var_count = conjunction.get_or_declare_variable(&mut context, "count").unwrap();
    let var_mean = conjunction.get_or_declare_variable(&mut context, "sum").unwrap();

    conjunction.constraints_mut().add_isa(&mut context, IsaKind::Subtype, var_person, var_person_type).unwrap();

    let mut function_call_var_mapping = BTreeMap::new();
    function_call_var_mapping.insert(var_person, Variable::new(1000));
    let mut function_call_var_categories = HashMap::new();
    function_call_var_categories.insert(var_person, VariableCategory::Object);
    let function_call = FunctionCall::new(
        DefinitionKey::build(Prefix::DefinitionStruct, DefinitionID::build(1000)),
        function_call_var_mapping,
        function_call_var_categories,
        vec![
            (VariableCategory::Value, VariableOptionality::Required),
            (VariableCategory::Value, VariableOptionality::Optional),
        ],
        false,
    );
    conjunction.constraints_mut().add_function_call(&mut context, vec![var_count, var_mean], function_call).unwrap();

    println!("{}", &conjunction);

    // TODO: incomplete, since we don't have the called function IR
    // let program = Program::new(Pattern::Conjunction(conjunction), HashMap::new());
}
