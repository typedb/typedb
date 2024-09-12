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
        ParameterRegistry,
    },
    translation::{constraints::add_statement, TranslationContext},
    PatternDefinitionError,
};

pub fn translate_match<'a>(
    context: &'a mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    match_: &typeql::query::stage::Match,
) -> Result<FunctionalBlockBuilder<'a>, PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder(context.next_block_context());
    add_patterns(function_index, &mut builder.conjunction_mut(), &match_.patterns)?;
    Ok(builder)
}

pub(crate) fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    patterns: &[typeql::Pattern],
) -> Result<(), PatternDefinitionError> {
    patterns.iter().try_for_each(|pattern| match pattern {
        typeql::Pattern::Conjunction(nested) => add_patterns(function_index, conjunction, &nested.patterns),
        typeql::Pattern::Disjunction(disjunction) => add_disjunction(function_index, conjunction, disjunction),
        typeql::Pattern::Negation(negation) => add_negation(function_index, conjunction, negation),
        typeql::Pattern::Optional(optional) => add_optional(function_index, conjunction, optional),
        typeql::Pattern::Statement(statement) => add_statement(function_index, conjunction, statement),
    })?;
    Ok(())
}

fn add_disjunction(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
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
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    negation: &typeql::pattern::Negation,
) -> Result<(), PatternDefinitionError> {
    let mut negation_builder = conjunction.add_negation();
    add_patterns(function_index, &mut negation_builder, &negation.patterns)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    optional: &typeql::pattern::Optional,
) -> Result<(), PatternDefinitionError> {
    let mut optional_builder = conjunction.add_optional();
    add_patterns(function_index, &mut optional_builder, &optional.patterns)
}
