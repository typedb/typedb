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
use primitive::either::Either;
use storage::snapshot::ReadableSnapshot;
use structural_equality::StructuralEquality;
use typeql::{
    common::{Span, Spanned},
    query::stage::{Operator as TypeQLOperator, Stage as TypeQLStage, Stage},
};

use crate::{
    pipeline::{
        block::Block,
        fetch::FetchObject,
        function::Function,
        function_signature::FunctionSignatureIndex,
        modifier::{Distinct, Limit, Offset, Require, Select, Sort},
        reduce::Reduce,
        ParameterRegistry, VariableRegistry,
    },
    translation::{
        fetch::translate_fetch,
        function::translate_typeql_function,
        match_::translate_match,
        modifiers::{
            translate_distinct, translate_limit, translate_offset, translate_require, translate_select, translate_sort,
        },
        reduce::translate_reduce,
        writes::{translate_delete, translate_insert, translate_update},
        TranslationContext,
    },
    RepresentationError,
};

#[derive(Debug, Clone)]
pub struct TranslatedPipeline {
    pub translated_preamble: Vec<Function>,
    pub translated_stages: Vec<TranslatedStage>,
    pub translated_fetch: Option<FetchObject>,
    pub variable_registry: VariableRegistry,
    pub value_parameters: ParameterRegistry,
}

impl TranslatedPipeline {
    pub(crate) fn new(
        translation_context: TranslationContext,
        value_parameters: ParameterRegistry,
        translated_preamble: Vec<Function>,
        translated_fetch: Option<FetchObject>,
        translated_stages: Vec<TranslatedStage>,
    ) -> Self {
        TranslatedPipeline {
            translated_preamble,
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
            | Self::Delete { block, .. } => Box::new(block.variables()),
            Self::Select(select) => Box::new(select.variables.iter().cloned()),
            Self::Sort(sort) => Box::new(sort.variables.iter().map(|sort_var| sort_var.variable())),
            Self::Offset(_) => Box::new(empty()),
            Self::Limit(_) => Box::new(empty()),
            Self::Require(require) => Box::new(require.variables.iter().cloned()),
            Self::Distinct(distinct) => Box::new(empty()),
            Self::Reduce(reduce) => Box::new(reduce.groupby.iter().cloned()),
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
    snapshot: &impl ReadableSnapshot,
    all_function_signatures: &impl FunctionSignatureIndex,
    query: &typeql::query::Pipeline,
) -> Result<TranslatedPipeline, Box<RepresentationError>> {
    // all_function_signatures contains the preambles already!
    let translated_preamble = query
        .preambles
        .iter()
        .map(|preamble| translate_typeql_function(snapshot, all_function_signatures, &preamble.function))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| RepresentationError::FunctionRepresentation { typedb_source: source })?;

    let mut translation_context = TranslationContext::new();
    let mut value_parameters = ParameterRegistry::new();
    let (translated_stages, translated_fetch) = translate_pipeline_stages(
        snapshot,
        all_function_signatures,
        &mut translation_context,
        &mut value_parameters,
        &query.stages,
    )?;

    Ok(TranslatedPipeline {
        translated_preamble,
        translated_stages,
        translated_fetch,
        variable_registry: translation_context.variable_registry,
        value_parameters,
    })
}

pub(crate) fn translate_pipeline_stages(
    snapshot: &impl ReadableSnapshot,
    all_function_signatures: &impl FunctionSignatureIndex,
    translation_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    stages: &[Stage],
) -> Result<(Vec<TranslatedStage>, Option<FetchObject>), Box<RepresentationError>> {
    let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(stages.len());
    for (i, stage) in stages.iter().enumerate() {
        let translated =
            translate_stage(snapshot, translation_context, value_parameters, all_function_signatures, stage)?;
        match translated {
            Either::First(stage) => translated_stages.push(stage),
            Either::Second(fetch) => {
                if i != stages.len() - 1 {
                    return Err(Box::new(RepresentationError::NonTerminalFetch { source_span: stage.span() }));
                } else {
                    return Ok((translated_stages, Some(fetch)));
                }
            }
        }
    }
    Ok((translated_stages, None))
}

fn translate_stage(
    snapshot: &impl ReadableSnapshot,
    translation_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    all_function_signatures: &impl FunctionSignatureIndex,
    typeql_stage: &TypeQLStage,
) -> Result<Either<TranslatedStage, FetchObject>, Box<RepresentationError>> {
    match typeql_stage {
        TypeQLStage::Match(match_) => {
            translate_match(translation_context, value_parameters, all_function_signatures, match_).and_then(
                |builder| {
                    Ok(Either::First(TranslatedStage::Match { block: builder.finish()?, source_span: match_.span() }))
                },
            )
        }
        TypeQLStage::Insert(insert) => translate_insert(translation_context, value_parameters, insert)
            .map(|block| Either::First(TranslatedStage::Insert { block, source_span: insert.span() })),
        TypeQLStage::Update(update) => translate_update(translation_context, value_parameters, update)
            .map(|block| Either::First(TranslatedStage::Update { block, source_span: update.span() })),
        TypeQLStage::Delete(delete) => {
            translate_delete(translation_context, value_parameters, delete).map(|(block, deleted_variables)| {
                Either::First(TranslatedStage::Delete { block, deleted_variables, source_span: delete.span() })
            })
        }
        TypeQLStage::Fetch(fetch) => {
            translate_fetch(snapshot, translation_context, value_parameters, all_function_signatures, fetch)
                .map(Either::Second)
                .map_err(|err| Box::new(RepresentationError::FetchRepresentation { typedb_source: err }))
        }
        TypeQLStage::Operator(modifier) => match modifier {
            TypeQLOperator::Select(select) => translate_select(translation_context, select)
                .map(|filter| Either::First(TranslatedStage::Select(filter))),
            TypeQLOperator::Sort(sort) => {
                translate_sort(translation_context, sort).map(|sort| Either::First(TranslatedStage::Sort(sort)))
            }
            TypeQLOperator::Offset(offset) => translate_offset(translation_context, offset)
                .map(|offset| Either::First(TranslatedStage::Offset(offset))),
            TypeQLOperator::Limit(limit) => {
                translate_limit(translation_context, limit).map(|limit| Either::First(TranslatedStage::Limit(limit)))
            }
            TypeQLOperator::Reduce(reduce) => translate_reduce(translation_context, reduce)
                .map(|reduce| Either::First(TranslatedStage::Reduce(reduce))),
            TypeQLOperator::Require(require) => translate_require(translation_context, require)
                .map(|require| Either::First(TranslatedStage::Require(require))),
            TypeQLOperator::Distinct(distinct) => translate_distinct(translation_context, distinct)
                .map(|distinct| Either::First(TranslatedStage::Distinct(distinct))),
        },
        _ => Err(Box::new(RepresentationError::UnrecognisedClause { source_span: typeql_stage.span() })),
    }
}
