/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    pattern::conjunction::ConjunctionBuilder,
    program::{
        block::{FunctionalBlock, FunctionalBlockBuilder},
        function_signature::FunctionSignatureIndex,
    },
    translator::constraints::add_statement,
    PatternDefinitionError,
};

pub fn translate_match(
    function_index: &impl FunctionSignatureIndex,
    match_: &typeql::query::stage::Match,
) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder();
    add_patterns(function_index, &mut builder.conjunction_mut(), &match_.patterns)?;
    Ok(builder)
}

fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_>,
    patterns: &Vec<typeql::pattern::Pattern>,
) -> Result<(), PatternDefinitionError> {
    patterns.iter().try_for_each(|pattern| match pattern {
        typeql::pattern::Pattern::Conjunction(nested) => add_patterns(function_index, conjunction, &nested.patterns),
        typeql::pattern::Pattern::Disjunction(disjunction) => add_disjunction(function_index, conjunction, disjunction),
        typeql::pattern::Pattern::Negation(negation) => add_negation(function_index, conjunction, negation),
        typeql::pattern::Pattern::Optional(optional) => add_optional(function_index, conjunction, optional),
        typeql::pattern::Pattern::Statement(statement) => {
            add_statement(function_index, &mut conjunction.constraints_mut(), statement)
        }
    })?;
    Ok(())
}

fn add_disjunction(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_>,
    disjunction: &typeql::pattern::Disjunction,
) -> Result<(), PatternDefinitionError> {
    let mut disjunction_builder = conjunction.add_disjunction();
    disjunction
        .branches
        .iter()
        .try_for_each(|branch| add_patterns(function_index, &mut disjunction_builder.add_conjunction(), branch))?;
    Ok(())
}

fn add_negation(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_>,
    negation: &typeql::pattern::Negation,
) -> Result<(), PatternDefinitionError> {
    let mut negation_builder = conjunction.add_negation();
    add_patterns(function_index, &mut negation_builder, &negation.patterns)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_>,
    optional: &typeql::pattern::Optional,
) -> Result<(), PatternDefinitionError> {
    let mut optional_builder = conjunction.add_optional();
    add_patterns(function_index, &mut optional_builder, &optional.patterns)
}
