/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    program::{
        block::{FunctionalBlock, ParameterRegistry, VariableRegistry},
        function::Function,
        function_signature::{FunctionID, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        modifier::{Limit, Offset, Select, Sort},
    },
    translation::{
        function::translate_function,
        match_::translate_match,
        modifiers::{translate_limit, translate_offset, translate_select, translate_sort},
        writes::{translate_delete, translate_insert},
        TranslationContext,
    },
};
use storage::snapshot::ReadableSnapshot;
use typeql::query::stage::{Modifier, Stage as TypeQLStage, Stage};
use ir::program::reduce::Reduce;
use ir::translation::reduce::translate_reduce;

use crate::error::QueryError;

pub(super) struct TranslatedPipeline {
    pub(super) translated_preamble: Vec<Function>,
    pub(super) translated_stages: Vec<TranslatedStage>,
    pub(super) variable_registry: VariableRegistry,
    pub(super) parameters: ParameterRegistry,
}

pub(super) enum TranslatedStage {
    Match { block: FunctionalBlock },
    Insert { block: FunctionalBlock },
    Delete { block: FunctionalBlock, deleted_variables: Vec<Variable> },

    // ...
    Filter(Select),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
    Reduce(Reduce),
}

pub(super) fn translate_pipeline(
    snapshot: &impl ReadableSnapshot,
    function_manager: &FunctionManager,
    query: &typeql::query::Pipeline,
) -> Result<TranslatedPipeline, QueryError> {
    let preamble_signatures = HashMapFunctionSignatureIndex::build(
        query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
    );
    let all_function_signatures =
        ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
    let translated_preamble = query
        .preambles
        .iter()
        .map(|preamble| translate_function(&all_function_signatures, &preamble.function))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| QueryError::FunctionDefinition { typedb_source: source })?;

    let mut translation_context = TranslationContext::new();
    let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(query.stages.len());
    for stage in &query.stages {
        let translated = translate_stage(&mut translation_context, &all_function_signatures, stage)?;
        translated_stages.push(translated);
    }
    Ok(TranslatedPipeline {
        translated_preamble,
        translated_stages,
        variable_registry: translation_context.variable_registry,
        parameters: translation_context.parameters,
    })
}

fn translate_stage(
    translation_context: &mut TranslationContext,
    all_function_signatures: &impl FunctionSignatureIndex,
    typeql_stage: &TypeQLStage,
) -> Result<TranslatedStage, QueryError> {
    match typeql_stage {
        TypeQLStage::Match(match_) => translate_match(translation_context, all_function_signatures, match_)
            .map(|builder| TranslatedStage::Match { block: builder.finish() }),
        TypeQLStage::Insert(insert) => {
            translate_insert(translation_context, insert).map(|block| TranslatedStage::Insert { block })
        }
        TypeQLStage::Delete(delete) => translate_delete(translation_context, delete)
            .map(|(block, deleted_variables)| TranslatedStage::Delete { block, deleted_variables }),
        TypeQLStage::Modifier(modifier) => match modifier {
            Modifier::Select(select) => {
                translate_select(translation_context, select).map(|filter| TranslatedStage::Filter(filter))
            }
            Modifier::Sort(sort) => translate_sort(translation_context, sort).map(|sort| TranslatedStage::Sort(sort)),
            Modifier::Offset(offset) => {
                translate_offset(translation_context, offset).map(|offset| TranslatedStage::Offset(offset))
            }
            Modifier::Limit(limit) => {
                translate_limit(translation_context, limit).map(|limit| TranslatedStage::Limit(limit))
            }
        },
        Stage::Reduce(reduce) => translate_reduce(translation_context, reduce).map(|reduce| TranslatedStage::Reduce(reduce)),
        _ => todo!(),
    }
    .map_err(|source| QueryError::PatternDefinition { typedb_source: source })
}
