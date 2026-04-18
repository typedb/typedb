/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    pattern::conjunction::ConjunctionBuilderWithContext,
    pipeline::{
        block::{Block, BlockBuilder},
        function_signature::FunctionSignatureIndex,
        ParameterRegistry,
    },
    translation::{constraints::add_statement, PipelineTranslationContext},
    RepresentationError,
};

pub fn translate_match<'a>(
    context: &'a mut PipelineTranslationContext,
    value_parameters: &'a mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    match_: &typeql::query::stage::Match,
) -> Result<BlockBuilder<'a>, Box<RepresentationError>> {
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let mut conjunction = builder.conjunction_mut();
    add_patterns(function_index, &mut conjunction, &match_.patterns)?;
    Ok(builder)
}

pub(crate) fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    patterns: &[typeql::Pattern],
) -> Result<(), Box<RepresentationError>> {
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
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    disjunction: &typeql::pattern::Disjunction,
) -> Result<(), Box<RepresentationError>> {
    let mut disjunction_builder = conjunction.add_disjunction();
    disjunction.branches.iter().try_for_each(|branch| {
        let mut conj = disjunction_builder.add_conjunction();
        add_patterns(function_index, &mut conj, branch)
    })?;
    Ok(())
}

fn add_negation(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    negation: &typeql::pattern::Negation,
) -> Result<(), Box<RepresentationError>> {
    let mut negation_builder = conjunction.add_negation();
    add_patterns(function_index, &mut negation_builder, &negation.patterns)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    parent_conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    optional: &typeql::pattern::Optional,
) -> Result<(), Box<RepresentationError>> {
    let mut optional_builder = parent_conjunction.add_optional(optional.span)?;
    add_patterns(function_index, &mut optional_builder, &optional.patterns)?;
    Ok(())
}
