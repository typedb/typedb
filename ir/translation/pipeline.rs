/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use storage::snapshot::ReadableSnapshot;
use typeql::query::stage::{Operator as TypeQLOperator, Stage as TypeQLStage, Stage};

use crate::{
    program::{
        block::Block,
        function::Function,
        function_signature::FunctionSignatureIndex,
        modifier::{Limit, Offset, Require, Select, Sort},
        reduce::Reduce,
        ParameterRegistry, VariableRegistry,
    },
    translation::{
        function::translate_function,
        match_::translate_match,
        modifiers::{translate_limit, translate_offset, translate_require, translate_select, translate_sort},
        reduce::translate_reduce,
        writes::{translate_delete, translate_insert},
        TranslationContext,
    },
    RepresentationError,
};
use crate::program::fetch::FetchObject;
use crate::translation::fetch::translate_fetch;

#[derive(Debug, Clone)]
pub struct TranslatedPipeline {
    pub translated_preamble: Vec<Function>,
    pub translated_stages: Vec<TranslatedStage>,
    pub variable_registry: VariableRegistry,
    pub value_parameters: ParameterRegistry,
}

impl TranslatedPipeline {
    pub(crate) fn new(
        translation_context: TranslationContext,
        translated_preamble: Vec<Function>,
        translated_stages: Vec<TranslatedStage>,
    ) -> Self {
        TranslatedPipeline {
            translated_preamble,
            translated_stages,
            variable_registry: translation_context.variable_registry,
            value_parameters: translation_context.parameters,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TranslatedStage {
    Match { block: Block },
    Insert { block: Block },
    Delete { block: Block, deleted_variables: Vec<Variable> },

    Fetch { fetch_object: FetchObject },

    // ...
    Select(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Reduce(Reduce),
    Require(Require),
}

pub fn translate_pipeline(
    snapshot: &impl ReadableSnapshot,
    all_function_signatures: &impl FunctionSignatureIndex,
    query: &typeql::query::Pipeline,
) -> Result<TranslatedPipeline, RepresentationError> {
    // all_function_signatures contains the preambles already!
    let translated_preamble = query
        .preambles
        .iter()
        .map(|preamble| translate_function(snapshot, all_function_signatures, &preamble.function))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| RepresentationError::FunctionRepresentation { typedb_source: source })?;

    let mut translation_context = TranslationContext::new();
    let translated_stages =
        translate_pipeline_stages(snapshot, all_function_signatures, &mut translation_context, &query.stages)?;

    Ok(TranslatedPipeline {
        translated_preamble,
        translated_stages,
        variable_registry: translation_context.variable_registry,
        value_parameters: translation_context.parameters,
    })
}

pub(crate) fn translate_pipeline_stages(
    snapshot: &impl ReadableSnapshot,
    all_function_signatures: &impl FunctionSignatureIndex,
    translation_context: &mut TranslationContext,
    stages: &[Stage],
) -> Result<Vec<TranslatedStage>, RepresentationError> {
    let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(stages.len());
    for (i, stage) in stages.iter().enumerate() {
        let translated = translate_stage(snapshot, translation_context, all_function_signatures, stage)?;
        if matches!(translated, TranslatedStage::Fetch { .. }) && i != stages.len() - 1 {
            return Err(RepresentationError::NonTerminalFetch { declaration: stage.clone() });
        }
        translated_stages.push(translated);
    }
    Ok(translated_stages)
}

fn translate_stage(
    snapshot: &impl ReadableSnapshot,
    translation_context: &mut TranslationContext,
    all_function_signatures: &impl FunctionSignatureIndex,
    typeql_stage: &TypeQLStage,
) -> Result<TranslatedStage, RepresentationError> {
    match typeql_stage {
        TypeQLStage::Match(match_) => translate_match(translation_context, all_function_signatures, match_)
            .map(|builder| TranslatedStage::Match { block: builder.finish() }),
        TypeQLStage::Insert(insert) => {
            translate_insert(translation_context, insert).map(|block| TranslatedStage::Insert { block })
        }
        TypeQLStage::Delete(delete) => translate_delete(translation_context, delete)
            .map(|(block, deleted_variables)| TranslatedStage::Delete { block, deleted_variables }),
        TypeQLStage::Fetch(fetch) => {
            translate_fetch(snapshot, translation_context, all_function_signatures, fetch)
                .map(|translated| TranslatedStage::Fetch { fetch_object: translated })
                .map_err(|err| RepresentationError::FetchRepresentation { typedb_source: err })
        }
        TypeQLStage::Operator(modifier) => match modifier {
            TypeQLOperator::Select(select) => {
                translate_select(translation_context, select).map(|filter| TranslatedStage::Select(filter))
            }
            TypeQLOperator::Sort(sort) => {
                translate_sort(translation_context, sort).map(|sort| TranslatedStage::Sort(sort))
            }
            TypeQLOperator::Offset(offset) => {
                translate_offset(translation_context, offset).map(|offset| TranslatedStage::Offset(offset))
            }
            TypeQLOperator::Limit(limit) => {
                translate_limit(translation_context, limit).map(|limit| TranslatedStage::Limit(limit))
            }
            TypeQLOperator::Reduce(reduce) => {
                translate_reduce(translation_context, reduce).map(|reduce| TranslatedStage::Reduce(reduce))
            }
            TypeQLOperator::Require(require) => {
                translate_require(translation_context, require).map(|require| TranslatedStage::Require(require))
            }
        },
        _ => Err(RepresentationError::UnrecognisedClause { declaration: typeql_stage.clone() }),
    }
}
