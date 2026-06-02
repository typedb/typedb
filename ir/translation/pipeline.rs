/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    hash::{DefaultHasher, Hasher},
    iter::empty,
    mem,
};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::{
    common::{Span, Spanned},
    query::stage::{Operator as TypeQLOperator, Stage as TypeQLStage, Stage},
    type_::NamedTypeAny,
};

use crate::{
    RepresentationError,
    pattern::{Pattern, variable_category::VariableCategory},
    pipeline::{
        ParameterRegistry, VariableRegistry,
        block::{Block, BlockBuilder},
        fetch::FetchObject,
        function::Function,
        function_signature::FunctionSignatureIndex,
        modifier::{Distinct, Limit, Offset, Require, Select, Sort},
        reduce::Reduce,
    },
    translation::{
        PipelineTranslationContext,
        fetch::translate_fetch,
        function::{function_argument_name_and_category, translate_typeql_function},
        match_::translate_match,
        modifiers::{
            translate_distinct, translate_limit, translate_offset, translate_require, translate_select, translate_sort,
        },
        reduce::translate_reduce,
        writes::{translate_delete, translate_insert, translate_put, translate_update},
    },
};

#[derive(Debug, Clone)]
pub struct TranslatedPipeline {
    pub translated_preamble: Vec<Function>,
    pub translated_given: Option<TranslatedGiven>,
    pub translated_stages: Vec<TranslatedStage>,
    pub translated_fetch: Option<FetchObject>,
    pub variable_registry: VariableRegistry,
    pub value_parameters: ParameterRegistry,
}

impl TranslatedPipeline {
    pub(crate) fn new(
        translation_context: PipelineTranslationContext,
        value_parameters: ParameterRegistry,
        translated_preamble: Vec<Function>,
        translated_given: Option<TranslatedGiven>,
        translated_stages: Vec<TranslatedStage>,
        translated_fetch: Option<FetchObject>,
    ) -> Self {
        TranslatedPipeline {
            translated_preamble,
            translated_given: translated_given,
            translated_stages,
            translated_fetch,
            variable_registry: translation_context.variable_registry,
            value_parameters,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TranslatedStage {
    Match { block: Block, source_span: Option<Span> },
    Insert { block: Block, source_span: Option<Span> },
    Update { block: Block, source_span: Option<Span> },
    Put { block: Block, source_span: Option<Span> },
    Delete { block: Block, deleted_variables: Vec<Variable>, source_span: Option<Span> },

    // ...
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Require(Require),
    Reduce(Reduce),
    Distinct(Distinct),
}

impl TranslatedStage {
    pub fn variables(&self) -> Box<dyn Iterator<Item = Variable> + '_> {
        match self {
            Self::Match { block, .. }
            | Self::Insert { block, .. }
            | Self::Update { block, .. }
            | Self::Put { block, .. }
            | Self::Delete { block, .. } => Box::new(block.conjunction().visible_referenced_variables()),
            Self::Select(select) => Box::new(select.variables.iter().cloned()),
            Self::Sort(sort) => Box::new(sort.variables.iter().map(|sort_var| sort_var.variable())),
            Self::Offset(_) => Box::new(empty()),
            Self::Limit(_) => Box::new(empty()),
            Self::Require(require) => Box::new(require.variables.iter().cloned()),
            Self::Distinct(distinct) => Box::new(empty()),
            Self::Reduce(reduce) => Box::new(reduce.variables()),
        }
    }
}

impl StructuralEquality for TranslatedStage {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            ^ match self {
                Self::Match { block, .. } => block.hash(),
                Self::Insert { block, .. } => block.hash(),
                Self::Update { block, .. } => block.hash(),
                Self::Put { block, .. } => block.hash(),
                Self::Delete { block, deleted_variables, .. } => {
                    let mut hasher = DefaultHasher::new();
                    block.hash_into(&mut hasher);
                    deleted_variables.hash_into(&mut hasher);
                    hasher.finish()
                }
                Self::Select(select) => select.hash(),
                Self::Sort(sort) => sort.hash(),
                Self::Offset(offset) => offset.hash(),
                Self::Limit(limit) => limit.hash(),
                Self::Require(require) => require.hash(),
                Self::Distinct(distinct) => distinct.hash(),
                Self::Reduce(reduce) => reduce.hash(),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Match { block, .. }, Self::Match { block: other_block, .. }) => block.equals(other_block),
            (Self::Insert { block, .. }, Self::Insert { block: other_block, .. }) => block.equals(other_block),
            (Self::Put { block, .. }, Self::Put { block: other_block, .. }) => block.equals(other_block),
            (Self::Update { block, .. }, Self::Update { block: other_block, .. }) => block.equals(other_block),
            (Self::Delete { block, .. }, Self::Delete { block: other_block, .. }) => block.equals(other_block),
            (Self::Select(select), Self::Select(other_select)) => select.equals(other_select),
            (Self::Sort(sort), Self::Sort(other_sort)) => sort.equals(other_sort),
            (Self::Offset(offset), Self::Offset(other_offset)) => offset.equals(other_offset),
            (Self::Limit(limit), Self::Limit(other_limit)) => limit.equals(other_limit),
            (Self::Require(require), Self::Require(other_require)) => require.equals(other_require),
            (Self::Distinct(distinct), Self::Distinct(other_distinct)) => distinct.equals(other_distinct),
            (Self::Reduce(reduce), Self::Reduce(other_reduce)) => reduce.equals(other_reduce),
            // note: this style forces updating the match when the variants change
            (Self::Match { .. }, _)
            | (Self::Insert { .. }, _)
            | (Self::Update { .. }, _)
            | (Self::Put { .. }, _)
            | (Self::Delete { .. }, _)
            | (Self::Select { .. }, _)
            | (Self::Sort { .. }, _)
            | (Self::Offset { .. }, _)
            | (Self::Limit { .. }, _)
            | (Self::Require { .. }, _)
            | (Self::Distinct { .. }, _)
            | (Self::Reduce { .. }, _) => false,
        }
    }
}

pub fn translate_pipeline(
    all_function_signatures: &impl FunctionSignatureIndex,
    query: &typeql::query::Pipeline,
) -> Result<TranslatedPipeline, Box<RepresentationError>> {
    // all_function_signatures contains the preambles already!
    let translated_preamble = query
        .preambles
        .iter()
        .map(|preamble| translate_typeql_function(all_function_signatures, &preamble.function))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| RepresentationError::FunctionRepresentation { typedb_source: *source })?;

    let mut translation_context = PipelineTranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let (translated_given, translated_stages, translated_fetch) = translate_pipeline_stages(
        all_function_signatures,
        &mut translation_context,
        &mut value_parameters,
        &query.stages,
    )?;

    Ok(TranslatedPipeline::new(
        translation_context,
        value_parameters,
        translated_preamble,
        translated_given,
        translated_stages,
        translated_fetch,
    ))
}

pub(crate) fn translate_pipeline_stages(
    all_function_signatures: &impl FunctionSignatureIndex,
    translation_context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    stages: &[Stage],
) -> Result<(Option<TranslatedGiven>, Vec<TranslatedStage>, Option<FetchObject>), Box<RepresentationError>> {
    let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(stages.len());
    let mut translated_given = None;
    let mut translated_fetch = None;

    for (i, stage) in stages.iter().enumerate() {
        let translated = translate_stage(translation_context, value_parameters, all_function_signatures, stage)?;
        match translated {
            TranslatedPipelinePart::Stage(stage) => translated_stages.push(stage),
            TranslatedPipelinePart::Given(given) => {
                if i != 0 {
                    return Err(Box::new(RepresentationError::NonInitialGiven { source_span: stage.span() }));
                } else {
                    translated_given = Some(given);
                }
            }
            TranslatedPipelinePart::Fetch(fetch) => {
                if i != stages.len() - 1 {
                    return Err(Box::new(RepresentationError::NonTerminalFetch { source_span: stage.span() }));
                } else {
                    translated_fetch = Some(fetch)
                }
            }
        }
    }
    Ok((translated_given, translated_stages, translated_fetch))
}

fn translate_stage(
    translation_context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    all_function_signatures: &impl FunctionSignatureIndex,
    typeql_stage: &TypeQLStage,
) -> Result<TranslatedPipelinePart, Box<RepresentationError>> {
    match typeql_stage {
        TypeQLStage::Given(given) => {
            translate_given_stage(translation_context, given).map(TranslatedPipelinePart::Given)
        }
        TypeQLStage::Match(match_) => {
            translate_match(translation_context, value_parameters, all_function_signatures, match_).and_then(
                |builder| {
                    Ok(TranslatedPipelinePart::Stage(TranslatedStage::Match {
                        block: builder.finish()?,
                        source_span: match_.span(),
                    }))
                },
            )
        }
        TypeQLStage::Insert(insert) => translate_insert(translation_context, value_parameters, insert)
            .map(|block| TranslatedPipelinePart::Stage(TranslatedStage::Insert { block, source_span: insert.span() })),
        TypeQLStage::Update(update) => translate_update(translation_context, value_parameters, update)
            .map(|block| TranslatedPipelinePart::Stage(TranslatedStage::Update { block, source_span: update.span() })),
        TypeQLStage::Put(put) => translate_put(translation_context, value_parameters, put)
            .map(|block| TranslatedPipelinePart::Stage(TranslatedStage::Put { block, source_span: put.span() })),
        TypeQLStage::Delete(delete) => {
            translate_delete(translation_context, value_parameters, delete).map(|(block, deleted_variables)| {
                TranslatedPipelinePart::Stage(TranslatedStage::Delete {
                    block,
                    deleted_variables,
                    source_span: delete.span(),
                })
            })
        }
        TypeQLStage::Fetch(fetch) => {
            translate_fetch(translation_context, value_parameters, all_function_signatures, fetch)
                .map(TranslatedPipelinePart::Fetch)
                .map_err(|err| Box::new(RepresentationError::FetchRepresentation { typedb_source: err }))
        }
        TypeQLStage::Operator(modifier) => match modifier {
            TypeQLOperator::Select(select) => translate_select(translation_context, select)
                .map(|filter| TranslatedPipelinePart::Stage(TranslatedStage::Select(filter))),
            TypeQLOperator::Sort(sort) => translate_sort(translation_context, sort)
                .map(|sort| TranslatedPipelinePart::Stage(TranslatedStage::Sort(sort))),
            TypeQLOperator::Offset(offset) => translate_offset(translation_context, offset)
                .map(|offset| TranslatedPipelinePart::Stage(TranslatedStage::Offset(offset))),
            TypeQLOperator::Limit(limit) => translate_limit(translation_context, limit)
                .map(|limit| TranslatedPipelinePart::Stage(TranslatedStage::Limit(limit))),
            TypeQLOperator::Reduce(reduce) => translate_reduce(translation_context, reduce)
                .map(|reduce| TranslatedPipelinePart::Stage(TranslatedStage::Reduce(reduce))),
            TypeQLOperator::Require(require) => translate_require(translation_context, require)
                .map(|require| TranslatedPipelinePart::Stage(TranslatedStage::Require(require))),
            TypeQLOperator::Distinct(distinct) => translate_distinct(translation_context, distinct)
                .map(|distinct| TranslatedPipelinePart::Stage(TranslatedStage::Distinct(distinct))),
        },
    }
}

fn translate_given_stage(
    context: &mut PipelineTranslationContext,
    typeql_given: &typeql::query::pipeline::stage::Given,
) -> Result<TranslatedGiven, Box<RepresentationError>> {
    let mut variables = Vec::with_capacity(typeql_given.variables.len());
    let mut labels = Vec::with_capacity(typeql_given.variables.len());
    typeql_given.variables.iter().try_for_each(|arg| {
        let all_info = function_argument_name_and_category(arg)
            .map_err(|typedb_source| Box::new(RepresentationError::FunctionRepresentation { typedb_source }))?;
        variables.push(context.register_input_variable(all_info)?);
        labels.push(arg.type_.clone());
        Ok::<(), Box<RepresentationError>>(())
    })?;
    Ok(TranslatedGiven { variables, labels, span: typeql_given.span() })
}

#[derive(Debug, Clone)]
pub struct TranslatedGiven {
    pub variables: Vec<Variable>,
    pub labels: Vec<NamedTypeAny>,
    pub span: Option<Span>,
}

impl StructuralEquality for TranslatedGiven {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.variables.hash_into(&mut hasher);
        self.labels.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variables.equals(&other.variables) && self.labels.equals(&other.labels)
    }
}

enum TranslatedPipelinePart {
    Given(TranslatedGiven),
    Stage(TranslatedStage),
    Fetch(FetchObject),
}
