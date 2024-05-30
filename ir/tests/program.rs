/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use ir::pattern::conjunction::Conjunction;
use ir::pattern::pattern::Pattern;
use ir::program::program::Program;
use ir::program::FunctionalBlock;
use ir::program::modifier::ModifierDefinitionError;

#[test]
fn build_program_modifiers() {
    let mut conjunction = Conjunction::new_root();

    let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
    let var_name = conjunction.get_or_declare_variable(&"name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&"person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable(&"name_type").unwrap();

    conjunction.constraints().add_isa(var_person, var_person_type).unwrap();
    conjunction.constraints().add_has(var_person, var_name).unwrap();
    conjunction.constraints().add_isa(var_name, var_name_type).unwrap();
    conjunction.constraints().add_type(var_person_type, "person").unwrap();
    conjunction.constraints().add_type(var_name_type, "name").unwrap();

    let mut program = Program::new(Pattern::Conjunction(conjunction), HashMap::new());
    program.add_limit(10);
    program.add_sort(vec![("person", true), ("name", false)]).unwrap();
}

#[test]
fn build_invalid_program_modifiers() {
    let mut conjunction = Conjunction::new_root();

    let person_name = String::from("bob");
    let var_person = conjunction.get_or_declare_variable(&person_name).unwrap();
    let var_name = conjunction.get_or_declare_variable(&"name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&"person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable(&"name_type").unwrap();

    conjunction.constraints().add_isa(var_person, var_person_type).unwrap();
    conjunction.constraints().add_has(var_person, var_name).unwrap();
    conjunction.constraints().add_isa(var_name, var_name_type).unwrap();
    conjunction.constraints().add_type(var_person_type, "person").unwrap();
    conjunction.constraints().add_type(var_name_type, "name").unwrap();

    let mut program = Program::new(Pattern::Conjunction(conjunction), HashMap::new());
    let result = program.add_sort(vec![("bob", true), ("jane", false)]);
    assert!(matches!(result, Err(ModifierDefinitionError::SortVariableNotAvailable { name: person_name })));
}

#[test]
fn build_program_with_functions() {}