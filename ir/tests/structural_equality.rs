/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::{
    pipeline::function_signature::HashMapFunctionSignatureIndex, translation::function::translate_typeql_function,
};
use structural_equality::{is_structurally_equivalent, StructuralEquality};

use crate::common::MockSnapshot;

mod common;

#[test]
fn test_function_equivalence() {
    let function = "fun sum_salary($person: person) -> string:
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let parsed_function = typeql::parse_definition_function(function).unwrap();

    let function_ir =
        translate_typeql_function(&MockSnapshot {}, &HashMapFunctionSignatureIndex::empty(), &parsed_function).unwrap();

    let hash = function_ir.hash();
    let equals_self = function_ir.equals(&function_ir);
    assert!(equals_self);

    let alpha_equivalent_function = "fun different_names($not_person: person) -> string:\
        match
          $not_person has salary $not_salary;
        return sum($not_salary);
    ";
    let parsed_alpha_equivalent_function = typeql::parse_definition_function(alpha_equivalent_function).unwrap();

    let alpha_equivalent_function_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &parsed_alpha_equivalent_function,
    )
    .unwrap();

    let alpha_equivalent_hash = function_ir.hash();
    let alpha_equivalent_equals_self = function_ir.equals(&function_ir);
    assert!(alpha_equivalent_equals_self);

    assert_eq!(hash, alpha_equivalent_hash);
    assert!(function_ir.equals(&alpha_equivalent_function_ir));
    assert!(is_structurally_equivalent(&function_ir, &alpha_equivalent_function_ir));
}

#[test]
fn test_function_not_equivalence() {
    let function = "fun sum_salary($person: person) -> string:
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let function_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(function).unwrap(),
    )
    .unwrap();

    let different_1 = "fun different($person: TYPE_CHANGED) -> string:\
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let different_1_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(different_1).unwrap(),
    )
    .unwrap();

    assert!(!is_structurally_equivalent(&function_ir, &different_1_ir));

    let different_2 = "fun different($person: person) -> decimal:\
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let different_2_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(different_2).unwrap(),
    )
    .unwrap();
    assert!(!is_structurally_equivalent(&function_ir, &different_2_ir));
    assert!(!is_structurally_equivalent(&different_1_ir, &different_2_ir));

    let different_3 = "fun different($person: person) -> string:\
        match
          $person has DIFFERENT_ATTRIBUTE $salary;
        return sum($salary);
    ";
    let different_3_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(different_3).unwrap(),
    )
    .unwrap();
    assert!(!is_structurally_equivalent(&function_ir, &different_3_ir));
    assert!(!is_structurally_equivalent(&different_1_ir, &different_3_ir));
    assert!(!is_structurally_equivalent(&different_2_ir, &different_3_ir));

    let different_4 = "fun different($person: person) -> string:\
        match
          $person links ($salary);
        return sum($salary);
    ";
    let different_4_ir = translate_typeql_function(
        &MockSnapshot {},
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(different_4).unwrap(),
    )
    .unwrap();
    assert!(!is_structurally_equivalent(&function_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_1_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_2_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_3_ir, &different_4_ir));
}
