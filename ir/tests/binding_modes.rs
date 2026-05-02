/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashSet;

use ir::{
    RepresentationError,
    pattern::Pattern,
    pipeline::{
        FunctionRepresentationError, ParameterRegistry, VariableRegistry,
        function_signature::{FunctionID, HashMapFunctionSignatureIndex},
    },
    translation::{
        PipelineTranslationContext,
        match_::translate_match,
        pipeline::{TranslatedStage, translate_pipeline},
    },
};
use itertools::Itertools;
use typeql::query::stage::Stage;

fn get_bound<'reg>(pattern: &impl Pattern, variable_registry: &'reg VariableRegistry) -> Vec<&'reg str> {
    let required = pattern.required_inputs().collect::<HashSet<_>>();
    pattern
        .named_visible_referenced_variables()
        .filter(|v| !required.contains(v))
        .map(|v| variable_registry.get_variable_name(v).unwrap().as_str())
        .sorted()
        .collect::<Vec<_>>()
}

fn get_required<'reg>(pattern: &impl Pattern, variable_registry: &'reg VariableRegistry) -> Vec<&'reg str> {
    pattern
        .required_inputs()
        .map(|v| variable_registry.get_variable_name(v).unwrap().as_str())
        .sorted()
        .collect::<Vec<_>>()
}

macro_rules! assert_vars {
    ($registry:expr, $pattern:expr, Required $required:tt, Bound $bound:tt) => {
        let mut bound: Vec<&'static str> = vec!$bound;
        let mut required: Vec<&'static str> = vec!$required;
        bound.sort();
        required.sort();
        assert_eq!(&get_bound($pattern, $registry), &bound);
        assert_eq!(&get_required($pattern, $registry), &required);
    }
}

macro_rules! assert_optionals {
    ($registry:expr, $optionals:tt) => {
        let mut optionals: Vec<&'static str> = vec!$optionals;
        optionals.sort();
        assert_eq!(&get_optionals($registry), &optionals);
    }
}

fn get_optionals(variable_registry: &VariableRegistry) -> Vec<&'_ str> {
    variable_registry
        .variable_names()
        .iter()
        .filter(|(v, _)| variable_registry.is_variable_optional(**v))
        .map(|(_, name)| name.as_str())
        .sorted()
        .collect()
}

#[test]
fn test_negation() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match $x isa person; not { $x has $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
    assert_vars!(&context.variable_registry, conjunction, Required[], Bound["x"]);
    assert_vars!(&context.variable_registry, negation, Required["x"], Bound[]);
    assert_vars!(&context.variable_registry, negation.conjunction(), Required["x"], Bound["y"]);
    assert_optionals!(&context.variable_registry, []);
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
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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

    let b11 = &first_disjunction.conjunctions()[0];
    let b12 = &first_disjunction.conjunctions()[1];
    let b21 = &second_disjunction.conjunctions()[0];
    let b22 = &second_disjunction.conjunctions()[1];
    assert_vars!(&context.variable_registry, conjunction, Required[], Bound["x"]);
    assert_vars!(&context.variable_registry, first_disjunction, Required[], Bound["x"]);
    assert_vars!(&context.variable_registry, second_disjunction, Required["x"], Bound[]);
    assert_vars!(&context.variable_registry, b11, Required[], Bound["x"]);
    assert_vars!(&context.variable_registry, b12, Required[], Bound["x"]);
    assert_vars!(&context.variable_registry, b21, Required["x"], Bound[]);
    assert_vars!(&context.variable_registry, b22, Required[], Bound["y"]);
    assert_optionals!(&context.variable_registry, []);
}

#[test]
fn test_optional() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $x isa person;
        try { $x has name $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
    assert_vars!(&context.variable_registry, conjunction, Required[], Bound["x", "y"]);
    assert_vars!(&context.variable_registry, optional, Required["x"], Bound["y"]);
    assert_vars!(&context.variable_registry, optional.conjunction(), Required["x"], Bound["y"]);
    assert_optionals!(&context.variable_registry, ["y"]);
}

#[test]
fn test_disjoint_negation() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match $x isa person; not { $x has name $y; }; not { $x has age $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
        RepresentationError::UnboundRequiredVariable { variable, .. } => {
            variable == "y"
        }
        _ => false,
    });
}

#[test]
fn problematic_is() {
    // This is from BDD, and fails because
    // `is` declares itself to require both bound, but it doesn't actually.
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        { $a isa! $_; } or { $_ isa! $_; };
        { $a is $b; $b isa! $_; } or { $a isa! $_; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
    assert_vars!(&context.variable_registry, conjunction, Required[], Bound["a"]);
    assert_optionals!(&context.variable_registry, []);
}

#[test]
fn test_disjoint_disjunction() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let query = r#"
    match $x isa person; { $x has name $y; } or { $x has age $y; } or { $x has height $z; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
        RepresentationError::UnboundRequiredVariable { variable, .. } => {
            variable == "y" || variable == "z"
        }
        _ => false,
    });
}

#[test]
fn test_disjoint_disjunction_again() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let query = r#"
      match { $x isa person; } or { $x isa company; } or { $a isa name; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translation_error = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap_err();
    assert!(match *translation_error {
        RepresentationError::UnboundRequiredVariable { variable, .. } => {
            variable == "x"
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
    let parsed = typeql::parse_query(query).unwrap().into_structure();
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
        RepresentationError::UnboundRequiredVariable { variable, .. } => {
            variable == "y"
        }
        _ => false,
    });
}

#[test]
fn test_negation_with_inputs() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $y isa name;
    match
        $x isa person;
        not { $x has $y; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block: second_block, .. } = &translated_pipeline.translated_stages[1] else {
        unreachable!();
    };
    let conjunction = second_block.conjunction();
    let negation = conjunction.nested_patterns().first().unwrap().as_negation().unwrap();
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required["y"], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, negation, Required["x", "y"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, negation.conjunction(), Required["x", "y"], Bound[]);
    assert_optionals!(&translated_pipeline.variable_registry, []);
}

#[test]
fn test_disjunction_with_inputs() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let query = r#"
    match
        $y isa person;
    match
        { $x has name "John"; } or { $x has name "James"; };
        { $x has height 16; } or { $y has name "Alice"; };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block: first_block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let TranslatedStage::Match { block: second_block, .. } = &translated_pipeline.translated_stages[1] else {
        unreachable!();
    };
    let conjunction = second_block.conjunction();

    let first_disjunction = conjunction.nested_patterns()[0].as_disjunction().unwrap();
    let second_disjunction = conjunction.nested_patterns()[1].as_disjunction().unwrap();

    let b11 = &first_disjunction.conjunctions()[0];
    let b12 = &first_disjunction.conjunctions()[1];
    let b21 = &second_disjunction.conjunctions()[0];
    let b22 = &second_disjunction.conjunctions()[1];

    assert_vars!(&translated_pipeline.variable_registry, first_block.conjunction(), Required[], Bound["y"]);
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required["y"], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, first_disjunction, Required[], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, second_disjunction, Required["x", "y"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, b11, Required[], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, b12, Required[], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, b21, Required["x"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, b22, Required["y"], Bound[]);
    assert_optionals!(&translated_pipeline.variable_registry, []);
}

#[test]
fn test_optional_with_inputs() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let query = r#"
    match
        try { $y isa name; };
    match
        $x isa person;
        try { $x has $y;};
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block: first_block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let TranslatedStage::Match { block: second_block, .. } = &translated_pipeline.translated_stages[1] else {
        unreachable!();
    };
    let conjunction = second_block.conjunction();
    let optional = conjunction.nested_patterns().first().unwrap().as_optional().unwrap();
    assert_vars!(&translated_pipeline.variable_registry, first_block.conjunction(), Required[], Bound["y"]);
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required["y"], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, optional, Required["x", "y"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, optional.conjunction(), Required["x", "y"], Bound[]);
    assert_optionals!(&translated_pipeline.variable_registry, ["y"]);
}

#[test]
fn test_optional_skip_a_stage() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();
    let query = r#"
    match
        try { $y isa name; };
    match
        let $_ = 0;
    match
        $x isa person;
        try { $x has $y;};
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block: first_block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let TranslatedStage::Match { block: second_block, .. } = &translated_pipeline.translated_stages[1] else {
        unreachable!();
    };
    let TranslatedStage::Match { block: third_block, .. } = &translated_pipeline.translated_stages[2] else {
        unreachable!();
    };
    let conjunction = third_block.conjunction();
    let optional = conjunction.nested_patterns().first().unwrap().as_optional().unwrap();
    assert_vars!(&translated_pipeline.variable_registry, first_block.conjunction(), Required[], Bound["y"]);
    assert_vars!(&translated_pipeline.variable_registry, second_block.conjunction(), Required[], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required["y"], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, optional, Required["x", "y"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, optional.conjunction(), Required["x", "y"], Bound[]);
    assert_optionals!(&translated_pipeline.variable_registry, ["y"]);
}

#[test]
fn test_nested_negation() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        $x isa person;
        not {
            $y isa person;
            not { $f isa friendship, links ($x, $y); };
        };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let conjunction = block.conjunction();
    let outer_negation = conjunction.nested_patterns().first().unwrap().as_negation().unwrap();
    let inner_negation = outer_negation.conjunction().nested_patterns().first().unwrap().as_negation().unwrap();
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required[], Bound["x"]);
    assert_vars!(&translated_pipeline.variable_registry, outer_negation, Required["x"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, inner_negation, Required["x", "y"], Bound[]);
    assert_vars!(&translated_pipeline.variable_registry, outer_negation.conjunction(), Required["x"], Bound["y"]);
    assert_vars!(&translated_pipeline.variable_registry, inner_negation.conjunction(), Required["x", "y"], Bound["f"]);
    assert_optionals!(&translated_pipeline.variable_registry, []);
}

#[test]
fn test_nested_optional() {
    let empty_function_index = HashMapFunctionSignatureIndex::empty();

    let query = r#"
    match
        try {
            $x isa person;
            try { $x has name $y; };
        };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure();
    let translated_pipeline = translate_pipeline(&empty_function_index, &parsed.into_pipeline()).unwrap();
    let TranslatedStage::Match { block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let conjunction = block.conjunction();
    let outer_optional = conjunction.nested_patterns().first().unwrap().as_optional().unwrap();
    let inner_optional = outer_optional.conjunction().nested_patterns().first().unwrap().as_optional().unwrap();
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required[], Bound["x", "y"]);
    assert_vars!(&translated_pipeline.variable_registry, outer_optional, Required[], Bound["x", "y"]);
    assert_vars!(&translated_pipeline.variable_registry, inner_optional, Required["x"], Bound["y"]);
    assert_vars!(&translated_pipeline.variable_registry, outer_optional.conjunction(), Required[], Bound["x", "y"]);
    assert_vars!(&translated_pipeline.variable_registry, inner_optional.conjunction(), Required["x"], Bound["y"]);
    assert_optionals!(&translated_pipeline.variable_registry, ["x", "y"]);
}

#[test]
fn test_unmarked_optional_return_errors() {
    let query = r#"
    with fun first_is_opt() -> {integer?, integer}:
    match try { let $x = 6; }; let $y = 5;
    return { $x, $y };

    match let $x, $y in first_is_opt();
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        parsed.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let translation_error = translate_pipeline(&preamble_signatures, &parsed).unwrap_err();
    assert!(match *translation_error {
        RepresentationError::UnmarkedOptionalAssignment { variable, .. } => {
            variable == "x"
        }
        _ => false,
    });
}

#[test]
fn test_optional_return() {
    let query = r#"
    with fun first_is_opt() -> {integer?, integer}:
    match let $y = 5; try { let $x = 6; };
    return { $x, $y };

    match let $x?, $y in first_is_opt();
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        parsed.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let translated_pipeline = translate_pipeline(&preamble_signatures, &parsed).unwrap();
    let TranslatedStage::Match { block, .. } = &translated_pipeline.translated_stages[0] else {
        unreachable!();
    };
    let conjunction = block.conjunction();
    assert_vars!(&translated_pipeline.variable_registry, conjunction, Required[], Bound["x", "y"]);
    assert_optionals!(&translated_pipeline.variable_registry, ["x"]);
}

#[test]
fn test_optional_return_must_be_declared() {
    let query = r#"
    with fun first_is_opt() -> {integer, integer}:
    match try { let $x = 6; }; let $y = 5;
    return { $x, $y };

    match let $x, $y in first_is_opt();
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        parsed.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let translation_error = translate_pipeline(&preamble_signatures, &parsed).unwrap_err();
    assert!(match *translation_error {
        RepresentationError::FunctionRepresentation {
            typedb_source: FunctionRepresentationError::InconsistentReturnOptionality { mismatch_index, .. },
        } => {
            mismatch_index == 0
        }
        _ => false,
    });
}

#[test]
fn test_optional_return_reuse_errors() {
    let query = r#"
    with fun age_opt() -> age?:
    match try { $age isa age; };
    return first $age;

    match
        let $age? in age_opt();
        $person has $age;
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        parsed.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let translation_error = translate_pipeline(&preamble_signatures, &parsed).unwrap_err();
    assert!(match *translation_error {
        RepresentationError::OptionalFunctionReturnReferenced { variable, .. } => {
            variable == "age"
        }
        _ => false,
    });
}

#[test]
fn test_optional_return_reuse_errors_with_disjunctions() {
    // Just a convoluted case.
    let query = r#"
    with fun age_opt() -> age?:
    match try { $age isa age; };
    return first $age;

    match
        $cat isa cat; $dog isa dog;
        {

            $dog has $name;
            {
                let $age? in age_opt();
            } or {
                { $cat has $age; } or { $dog has $age; };
                $cat has name $name;
            };
        } or {
            $dog has $name; $dog has $other_age;
        };
    "#;
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        parsed.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let translation_error = translate_pipeline(&preamble_signatures, &parsed).unwrap_err();
    assert!(match *translation_error {
        RepresentationError::OptionalFunctionReturnReferenced { variable, .. } => {
            variable == "age"
        }
        _ => false,
    });
}
