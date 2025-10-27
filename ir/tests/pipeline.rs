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
    pipeline::{
        block::Block,
        function_signature::{FunctionID, FunctionSignature, HashMapFunctionSignatureIndex},
        ParameterRegistry,
    },
    translation::{pipeline::translate_pipeline, PipelineTranslationContext},
};

// TODO: if we re-instante modifiers/stream operators as part of blocks, then we can bring this test back
// #[test]
// fn build_modifiers() {
//     let mut context = TranslationContext::new();
//     let mut builder = Block::builder(context.next_block_context());
//     let mut conjunction = builder.conjunction_mut();
//
//     let var_person = conjunction.get_or_declare_variable("person").unwrap();
//     let var_name = conjunction.get_or_declare_variable("name").unwrap();
//     let var_person_type = conjunction.get_or_declare_variable("person_type").unwrap();
//     let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();
//
//     conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into()).unwrap();
//     conjunction.constraints_mut().add_has(var_person, var_name).unwrap();
//     conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
//     conjunction.constraints_mut().add_label(var_person_type, "person").unwrap();
//     conjunction.constraints_mut().add_label(var_name_type, "name").unwrap();
//
//     builder.add_limit(10);
//     builder.add_sort(vec![(var_person.clone(), true), (var_name.clone(), false)]);
//
//     let block = builder.finish();
// }

#[test]
fn build_with_functions() {
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let mut builder = Block::builder(context.new_block_builder_context(&mut value_parameters));
    let mut conjunction = builder.conjunction_mut();

    let var_person = conjunction.constraints_mut().get_or_declare_variable("person", None).unwrap();
    let var_person_type = conjunction.constraints_mut().get_or_declare_variable("person_type", None).unwrap();

    let var_count = conjunction.constraints_mut().get_or_declare_variable("count", None).unwrap();
    let var_mean = conjunction.constraints_mut().get_or_declare_variable("sum", None).unwrap();

    conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_person, var_person_type.into(), None).unwrap();

    let function_argument_categories = vec![VariableCategory::Object];
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
        .add_function_binding(vec![var_count, var_mean], &function_signature, vec![var_person], "test_fn", None)
        .unwrap();
    let block = builder.finish().unwrap();
    println!("{}", block.conjunction());

    // TODO: incomplete, since we don't have the called function IR
}

#[test]
fn optional_writes() {
    let query = r#"
        match $p isa person; try { $p has name $name; };
        delete try { has $name of $p; };
        insert $q isa person; try { $q has $name; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        insert try { $p isa person; };
        delete try { $p; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        delete try { has $name of $p; }; try { has $age of $p; };
        insert $q isa person; try { $q has $name; }; try { $q has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name; };
        put $q isa person; try { $q has $name; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name; };
        update try { $p has $name; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");
}

#[test]
fn multiple_optional_writes_in_a_block() {
    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        delete try { has $name of $p; has $age of $p; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        insert $q isa person; try { $q has $name; $q has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        insert $q isa person; try { $q has $name, has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        put $q isa person; try { $q has $name; $q has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        put $q isa person; try { $q has $name, has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        update try { $p has $name; $p has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        update try { $p has $name, has $age; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_ok(), "{translation_result:?}");
}

#[test]
fn nested_optional_blocks_in_write() {
    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        delete try { has $name of $p; try { has $age of $p; }; };
    "#;
    if let Ok(parsed) = typeql::parse_query(query) {
        // currently nested try blocks don't even parse in delete
        let translation_result =
            translate_pipeline(&HashMapFunctionSignatureIndex::empty(), &parsed.into_structure().into_pipeline());
        assert!(translation_result.is_err(), "Nested try blocks are not yet supported in write stages: {query}");
    }

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        insert $q isa person; try { $q has $name; try { $q has $age; }; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_err(), "Nested try blocks are not yet supported in write stages: {query}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        put $q isa person; try { $q has $name; try { $q has $age; }; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_err(), "Nested try blocks are not yet supported in write stages: {query}");

    let query = r#"
        match $p isa person; try { $p has name $name, has age $age; };
        update try { $p has $name; try { $p has $age; }; };
    "#;
    let translation_result = translate_pipeline(
        &HashMapFunctionSignatureIndex::empty(),
        &typeql::parse_query(query).unwrap().into_structure().into_pipeline(),
    );
    assert!(translation_result.is_err(), "Nested try blocks are not yet supported in write stages: {query}");
}
