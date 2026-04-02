/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::BTreeMap;

use ir::{
    pattern::{BindingMode, Pattern},
    pipeline::{function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{match_::translate_match, PipelineTranslationContext},
    RepresentationError,
};
use typeql::query::stage::Stage;

fn binding_modes<'a>(
    context: &'a PipelineTranslationContext,
    conjunction: &impl Pattern,
) -> BTreeMap<&'a str, BindingMode> {
    conjunction
        .variable_binding_modes()
        .iter()
        .filter_map(|(v, b)| context.variable_registry.get_variable_name(*v).map(|s| (s.as_str(), *b)))
        .collect()
}

#[test]
fn test_negation() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match $x isa person; not { $x has $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translated_match = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap();
    let conjunction = translated_match.conjunction();
    let negation = conjunction.nested_patterns().first().unwrap().as_negation().unwrap();
    let conjunction_modes = binding_modes(&context, conjunction);
    let negation_modes = binding_modes(&context, negation);
    let inner_modes = binding_modes(&context, negation.conjunction());
    assert_eq!(
        conjunction_modes,
        BTreeMap::from([("x", BindingMode::AlwaysBinding), ("y", BindingMode::LocallyBindingInChild)])
    );
    assert_eq!(
        negation_modes,
        BTreeMap::from([("x", BindingMode::RequirePrebound), ("y", BindingMode::LocallyBindingInChild)])
    );
    assert_eq!(inner_modes, BTreeMap::from([("x", BindingMode::RequirePrebound), ("y", BindingMode::AlwaysBinding)]));
}

#[test]
fn test_disjunction() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $x has age 10;
        { $x has name "John"; } or { $x has name "James"; };
        { $x has height 16; } or { $y has name "Alice"; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translated_match = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap();
    let conjunction = translated_match.conjunction();
    let first_disjunction = conjunction.nested_patterns()[0].as_disjunction().unwrap();
    let second_disjunction = conjunction.nested_patterns()[1].as_disjunction().unwrap();

    let conjunction_modes = binding_modes(&context, conjunction);
    let d1 = binding_modes(&context, first_disjunction);
    let d2 = binding_modes(&context, second_disjunction);
    let b11 = binding_modes(&context, &first_disjunction.conjunctions()[0]);
    let b12 = binding_modes(&context, &first_disjunction.conjunctions()[1]);
    let b21 = binding_modes(&context, &second_disjunction.conjunctions()[0]);
    let b22 = binding_modes(&context, &second_disjunction.conjunctions()[1]);
    assert_eq!(
        conjunction_modes,
        BTreeMap::from([("x", BindingMode::AlwaysBinding), ("y", BindingMode::LocallyBindingInChild)])
    );
    assert_eq!(d1, BTreeMap::from([("x", BindingMode::AlwaysBinding)]));
    assert_eq!(d2, BTreeMap::from([("x", BindingMode::RequirePrebound), ("y", BindingMode::LocallyBindingInChild)]));
    assert_eq!(b11, BTreeMap::from([("x", BindingMode::AlwaysBinding)]));
    assert_eq!(b12, BTreeMap::from([("x", BindingMode::AlwaysBinding)]));
    assert_eq!(b21, BTreeMap::from([("x", BindingMode::RequirePrebound)]));
    assert_eq!(b22, BTreeMap::from([("y", BindingMode::AlwaysBinding)]));
}

#[test]
fn test_optional() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $x isa person;
        try { $x has name $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translated_match = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap();
    let conjunction = translated_match.conjunction();
    let optional = conjunction.nested_patterns().first().unwrap().as_optional().unwrap();
    let conjunction_modes = binding_modes(&context, conjunction);
    let optional_modes = binding_modes(&context, optional);
    let inner_modes = binding_modes(&context, optional.conjunction());
    assert_eq!(
        conjunction_modes,
        BTreeMap::from([("x", BindingMode::AlwaysBinding), ("y", BindingMode::OptionallyBinding)])
    );
    assert_eq!(
        optional_modes,
        BTreeMap::from([("x", BindingMode::RequirePrebound), ("y", BindingMode::OptionallyBinding)])
    );
    assert_eq!(inner_modes, BTreeMap::from([("x", BindingMode::RequirePrebound), ("y", BindingMode::AlwaysBinding)]));
}

#[test]
fn test_disjoint_negation() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match $x isa person; not { $x has name $y; }; not { $x has age $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translation_error = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap_err();
    assert!(match *translation_error {
        RepresentationError::UnboundRequiredVariable { variable } => {
            variable == "y"
        }
        _ => false,
    });
}

#[test]
fn test_disjoint_disjunction() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match $x isa person; { $x has name $y; } or { $x has age $y; } or { $x has height $z; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translation_error = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap_err();
    assert!(match *translation_error {
        RepresentationError::UnboundRequiredVariable { variable } => {
            variable == "y" || variable == "z"
        }
        _ => false,
    });
}

#[test]
fn test_disjoint_optional() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $x isa person;
        $z isa person;
        try { $x has name $y; };
        try { $z has name $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure(); // TODO
    let typeql::query::QueryStructure::Pipeline(typeql::query::Pipeline { stages, .. }) = parsed else {
        unreachable!()
    };
    let Stage::Match(typeql_match) = stages.first().unwrap() else { unreachable!() };
    let mut context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let translation_error = translate_match(&mut context, &mut value_parameters, &empty_function_index, typeql_match)
        .unwrap()
        .finish()
        .unwrap_err();
    assert!(match *translation_error {
        RepresentationError::UnboundRequiredVariable { variable } => {
            variable == "y"
        }
        _ => false,
    });
}
