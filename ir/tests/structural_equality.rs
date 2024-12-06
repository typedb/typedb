/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::{
    pipeline::function_signature::HashMapFunctionSignatureIndex,
    translation::{
        function::translate_typeql_function,
        pipeline::{translate_pipeline, TranslatedPipeline},
    },
};
use structural_equality::{is_structurally_equivalent, StructuralEquality};
use test_utils_storage::mock_snapshot::MockSnapshot;

#[test]
fn test_function_equivalence() {
    let function = "fun sum_salary($person: person) -> string:
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let parsed_function = typeql::parse_definition_function(function).unwrap();

    let function_ir =
        translate_typeql_function(&MockSnapshot::new(), &HashMapFunctionSignatureIndex::empty(), &parsed_function)
            .unwrap();

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
        &MockSnapshot::new(),
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
fn test_function_non_equivalence() {
    let function = "fun sum_salary($person: person) -> string:
        match
          $person has salary $salary;
        return sum($salary);
    ";
    let function_ir = translate_typeql_function(
        &MockSnapshot::new(),
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
        &MockSnapshot::new(),
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
        &MockSnapshot::new(),
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
        &MockSnapshot::new(),
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
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_definition_function(different_4).unwrap(),
    )
    .unwrap();
    assert!(!is_structurally_equivalent(&function_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_1_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_2_ir, &different_4_ir));
    assert!(!is_structurally_equivalent(&different_3_ir, &different_4_ir));
}

#[test]
fn test_pipeline_equivalence() {
    let pipeline = "
with fun avg_salary($x: person) -> double:
  match $x has salary $salary;
        $salary_plus_1 = $salary + 1;
  return mean($salary_plus_1);
match
  $c isa CUSTOMER, has C_ID 42029, has C_BALANCE $c_balance;
  $c_balance_new = $c_balance + 1;
  $o links (customer: $c), isa ORDER, has O_ID 69, has O_NEW_ORDER $o_new_order, has O_CARRIER_ID $o_carrier_id;
delete
  has $o_new_order of $o;
  has $o_carrier_id of $o;
  has $c_balance of $c;
insert
  $o has O_NEW_ORDER false, has O_CARRIER_ID 7;
  $c has C_BALANCE == $c_balance_new;
select $o;
match $ol links (order: $o), isa ORDER_LINE;
insert $ol has OL_DELIVERY_D 2024-11-18T15:06:09.224;
fetch {
  'my-key': { $ol.* }
};
";
    let TranslatedPipeline { translated_preamble, translated_stages, translated_fetch, .. } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(pipeline).unwrap().into_pipeline(),
    )
    .unwrap();
    assert!(translated_preamble.equals(&translated_preamble));
    assert!(translated_stages.equals(&translated_stages));
    assert!(translated_fetch.equals(&translated_fetch));

    let structurally_equivalent_pipeline = "\
with fun avg_salary($DIFF: person) -> double:
  match $DIFF has salary $salary;
        $salary_plus_2 = $salary + 2;
  return mean($salary_plus_2);
match
  $C_DIFF isa CUSTOMER, has C_ID 0, has C_BALANCE $c_balance;
  $c_balance_new = $c_balance + 5;
  $o links (customer: $C_DIFF), isa ORDER, has O_ID 0, has O_NEW_ORDER $o_new_order, has O_CARRIER_ID $o_carrier_id;
delete
  has $o_new_order of $o;
  has $o_carrier_id of $o;
  has $c_balance of $C_DIFF;
insert
  $o has O_NEW_ORDER false, has O_CARRIER_ID 0;
  $C_DIFF has C_BALANCE == $c_balance_new;
select $o;
match $ol links (order: $o), isa ORDER_LINE;
insert $ol has OL_DELIVERY_D 2024-01-01T00:00:00.000;
fetch {
  'different key': { $ol.* }
};
";
    let TranslatedPipeline {
        translated_preamble: equivalent_translated_preamble,
        translated_stages: equivalent_translated_stages,
        translated_fetch: equivalent_translated_fetch,
        ..
    } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(structurally_equivalent_pipeline).unwrap().into_pipeline(),
    )
    .unwrap();

    assert!(equivalent_translated_preamble.equals(&equivalent_translated_preamble));
    assert!(equivalent_translated_stages.equals(&equivalent_translated_stages));
    assert!(equivalent_translated_fetch.equals(&equivalent_translated_fetch));

    assert!(is_structurally_equivalent(&translated_preamble, &equivalent_translated_preamble));
    assert!(is_structurally_equivalent(&translated_stages, &equivalent_translated_stages));
    assert!(is_structurally_equivalent(&translated_fetch, &equivalent_translated_fetch));
}

#[test]
fn test_pipeline_non_equivalence() {
    let pipeline = "
with fun avg_salary($x: person) -> double:
  match $x has avg_salary $salary;
  return mean($salary);
match
  $c isa CUSTOMER, has C_ID 42029, has C_BALANCE $c_balance;
  $c_balance_new = $c_balance + 1;
  $o links (customer: $c), isa ORDER, has O_ID 69, has O_NEW_ORDER $o_new_order, has O_CARRIER_ID $o_carrier_id;
delete
  has $o_new_order of $o;
  has $o_carrier_id of $o;
  has $c_balance of $c;
insert
  $o has O_NEW_ORDER false, has O_CARRIER_ID 7;
  $c has C_BALANCE == $c_balance_new;
select $o;
match
  $ol links (order: $o), isa ORDER_LINE;
insert $ol has OL_DELIVERY_D 2024-11-18T15:06:09.224;
fetch {
  'my-key': { $ol.* }
};";
    let TranslatedPipeline { translated_preamble, translated_stages, translated_fetch, .. } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(pipeline).unwrap().into_pipeline(),
    )
    .unwrap();
    assert!(translated_preamble.equals(&translated_preamble));
    assert!(translated_stages.equals(&translated_stages));
    assert!(translated_fetch.equals(&translated_fetch));

    let different = "\
with fun avg_age($x: person) -> double:
  match $x has age $age;
  return mean($age);
match
  $c isa CUSTOMER, has C_ID 0, has C_BALANCE $c_balance;
  $c_balance_new = $c_balance + 5;
  $o links (customer: $c), isa ORDER, has O_ID 0, has O_NEW_ORDER $o_new_order;
delete
  has $o_new_order of $o;
  has $o_carrier_id of $o;
  has $c_balance of $c;
insert
  $o has O_NEW_ORDER false, has O_CARRIER_ID 0;
  $c has C_BALANCE == $c_balance_new;
select $o;
match
  $ol links (order: $o), isa ORDER_LINE;
insert $ol has OL_DELIVERY_D 2024-01-01T00:00:00.000;
fetch {
  'my-key': [ $ol.name ]
};";
    let TranslatedPipeline {
        translated_preamble: different_translated_preamble,
        translated_stages: different_translated_stages,
        translated_fetch: different_translated_fetch,
        ..
    } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(different).unwrap().into_pipeline(),
    )
    .unwrap();

    assert!(different_translated_preamble.equals(&different_translated_preamble));
    assert!(different_translated_stages.equals(&different_translated_stages));
    assert!(different_translated_fetch.equals(&different_translated_fetch));

    assert!(!is_structurally_equivalent(&translated_preamble, &different_translated_preamble));
    assert!(!is_structurally_equivalent(&translated_stages, &different_translated_stages));
    assert!(!is_structurally_equivalent(&translated_fetch, &different_translated_fetch));
}

#[test]
fn test_anonymous_non_equivalence() {
    let query = "match $x relates $_ as parent;";
    let TranslatedPipeline { translated_stages, .. } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_pipeline(),
    )
    .unwrap();
    assert!(translated_stages.equals(&translated_stages));

    let non_equivalent_query = "match $x relates $role as parent;";
    let TranslatedPipeline { translated_stages: different_translated_stages, .. } = translate_pipeline(
        &MockSnapshot::new(),
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(non_equivalent_query).unwrap().into_pipeline(),
    )
    .unwrap();
    assert!(different_translated_stages.equals(&different_translated_stages));

    assert!(!is_structurally_equivalent(&translated_stages, &different_translated_stages));
}
