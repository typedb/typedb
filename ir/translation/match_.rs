/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use typeql::common::Span;

use crate::{
    RepresentationError,
    pattern::conjunction::ConjunctionBuilderWithContext,
    pipeline::{
        ParameterRegistry,
        block::{Block, BlockBuilder},
        function_signature::FunctionSignatureIndex,
    },
    translation::{
        PipelineTranslationContext,
        constraints::{PatternTranslationMode, add_statement},
    },
};

pub fn translate_match<'a>(
    context: &'a mut PipelineTranslationContext,
    value_parameters: &'a mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    match_: &typeql::query::stage::Match,
) -> Result<BlockBuilder<'a>, Box<RepresentationError>> {
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let mut conjunction = builder.conjunction_mut();
    add_patterns(function_index, &mut conjunction, &match_.patterns, PatternTranslationMode::Match)?;
    Ok(builder)
}

pub(super) fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    patterns: &[typeql::Pattern],
    mode: PatternTranslationMode,
) -> Result<(), Box<RepresentationError>> {
    patterns.iter().try_for_each(|pattern| match pattern {
        typeql::Pattern::Conjunction(nested) => add_patterns(function_index, conjunction, &nested.patterns, mode),
        typeql::Pattern::Disjunction(disjunction) => add_disjunction(function_index, conjunction, disjunction, mode),
        typeql::Pattern::Negation(negation) => add_negation(function_index, conjunction, negation, mode),
        typeql::Pattern::Optional(optional) => add_optional(function_index, conjunction, optional, mode),
        typeql::Pattern::Statement(statement) => add_statement(function_index, conjunction, statement, mode),
    })?;
    Ok(())
}

fn add_disjunction(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    disjunction: &typeql::pattern::Disjunction,
    mode: PatternTranslationMode,
) -> Result<(), Box<RepresentationError>> {
    let mut disjunction_builder = conjunction.add_disjunction(disjunction.span);
    disjunction.branches.iter().try_for_each(|branch| {
        let mut conj = disjunction_builder.add_conjunction();
        add_patterns(function_index, &mut conj, branch, mode)
    })?;
    Ok(())
}

fn add_negation(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    negation: &typeql::pattern::Negation,
    mode: PatternTranslationMode,
) -> Result<(), Box<RepresentationError>> {
    let mut negation_builder = conjunction.add_negation(negation.span);
    add_patterns(function_index, &mut negation_builder, &negation.patterns, mode)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    parent_conjunction: &mut ConjunctionBuilderWithContext<'_, '_>,
    optional: &typeql::pattern::Optional,
    mode: PatternTranslationMode,
) -> Result<(), Box<RepresentationError>> {
    let mut optional_builder = parent_conjunction.add_optional(optional.span)?;
    add_patterns(function_index, &mut optional_builder, &optional.patterns, mode)?;
    Ok(())
}
