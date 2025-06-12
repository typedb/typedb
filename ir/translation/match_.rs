/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    pattern::conjunction::ConjunctionBuilder,
    pipeline::{
        block::{Block, BlockBuilder},
        function_signature::FunctionSignatureIndex,
        ParameterRegistry,
    },
    translation::{constraints::add_statement, PipelineTranslationContext},
    RepresentationError,
};
use crate::pattern::Scope;

pub fn translate_match<'a>(
    context: &'a mut PipelineTranslationContext,
    value_parameters: &'a mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    match_: &typeql::query::stage::Match,
) -> Result<BlockBuilder<'a>, Box<RepresentationError>> {
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    add_patterns(function_index, &mut builder.conjunction_mut(), &match_.patterns)?;
    Ok(builder)
}

pub(crate) fn add_patterns(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
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
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    disjunction: &typeql::pattern::Disjunction,
) -> Result<(), Box<RepresentationError>> {
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
) -> Result<(), Box<RepresentationError>> {
    let mut negation_builder = conjunction.add_negation();
    add_patterns(function_index, &mut negation_builder, &negation.patterns)
}

fn add_optional(
    function_index: &impl FunctionSignatureIndex,
    parent_conjunction: &mut ConjunctionBuilder<'_, '_>,
    optional: &typeql::pattern::Optional,
) -> Result<(), Box<RepresentationError>> {
    let parent_scope = parent_conjunction.conjunction.scope_id();
    let mut optional_builder = parent_conjunction.add_optional(optional.span)?;
    add_patterns(function_index, &mut optional_builder, &optional.patterns)?;
    let ConjunctionBuilder {
        conjunction,
        context,
    } = optional_builder;
    for var in conjunction.referenced_variables() {
        // if the variable is available in the parent scope, it's bound externally and passed in so not optional
        if !context.is_variable_in_scope_or_parent(parent_scope, var) {
            context.set_variable_optionality(var, true)
        }
    };
    Ok(())
}
