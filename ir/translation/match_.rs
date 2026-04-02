/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    pattern::conjunction::ConjunctionBuilder,
    pipeline::{
        block::{Block, BlockBuilder, BlockBuilderContext},
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
    let (context, conjunction) = builder.to_parts_mut();
    add_patterns(function_index, context, conjunction, &match_.patterns)?;
    Ok(builder)
}

pub(crate) fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    context: &mut BlockBuilderContext<'_>,
    conjunction: &mut ConjunctionBuilder,
    patterns: &[typeql::Pattern],
) -> Result<(), Box<RepresentationError>> {
    patterns.iter().try_for_each(|pattern| match pattern {
        typeql::Pattern::Conjunction(nested) => add_patterns(function_index, context, conjunction, &nested.patterns),
        typeql::Pattern::Disjunction(disjunction) => add_disjunction(function_index, context, conjunction, disjunction),
        typeql::Pattern::Negation(negation) => add_negation(function_index, context, conjunction, negation),
        typeql::Pattern::Optional(optional) => add_optional(function_index, context, conjunction, optional),
        typeql::Pattern::Statement(statement) => add_statement(function_index, context, conjunction, statement),
    })?;
    Ok(())
}

fn add_disjunction(
    function_index: &impl FunctionSignatureIndex,
    context: &mut BlockBuilderContext<'_>,
    conjunction: &mut ConjunctionBuilder,
    disjunction: &typeql::pattern::Disjunction,
) -> Result<(), Box<RepresentationError>> {
    let mut disjunction_builder = conjunction.add_disjunction(context);
    disjunction.branches.iter().try_for_each(|branch| {
        let conj = disjunction_builder.add_conjunction(context);
        add_patterns(function_index, context, conj, branch)
    })?;
    Ok(())
}

fn add_negation(
    function_index: &impl FunctionSignatureIndex,
    context: &mut BlockBuilderContext<'_>,
    conjunction: &mut ConjunctionBuilder,
    negation: &typeql::pattern::Negation,
) -> Result<(), Box<RepresentationError>> {
    let mut negation_builder = conjunction.add_negation(context);
    add_patterns(function_index, context, &mut negation_builder.conjunction_mut(), &negation.patterns)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    context: &mut BlockBuilderContext<'_>,
    parent_conjunction: &mut ConjunctionBuilder,
    optional: &typeql::pattern::Optional,
) -> Result<(), Box<RepresentationError>> {
    let mut optional_builder = parent_conjunction.add_optional(optional.span, context)?;
    add_patterns(function_index, context, &mut optional_builder.conjunction_mut(), &optional.patterns)?;
    Ok(())
}
