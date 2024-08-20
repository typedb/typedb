/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::{
    program::{
        block::{FunctionalBlock, VariableRegistry},
        function::Function,
        function_signature::{FunctionID, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        modifier::{Filter, Limit, Offset, Sort},
    },
    translation::{
        function::translate_function,
        match_::translate_match,
        writes::{translate_delete, translate_insert},
        TranslationContext,
    },
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::{stage::Stage as TypeQLStage, SchemaQuery};

use crate::error::QueryError;

pub(super) struct TranslatedPipeline {
    pub(super) translated_preamble: Vec<Function>,
    pub(super) translated_stages: Vec<TranslatedStage>,
    pub(super) variable_registry: VariableRegistry,
}

pub(super) enum TranslatedStage {
    Match { block: FunctionalBlock },
    Insert { block: FunctionalBlock },
    Delete { block: FunctionalBlock, deleted_variables: Vec<Variable> },

    // ...
    Filter(Filter),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
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
        .map_err(|source| QueryError::FunctionDefinition { source })?;

    let mut translation_context = TranslationContext::new();
    let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(query.stages.len());
    for typeql_stage in &query.stages {
        let translated = translate_stage(&mut translation_context, &all_function_signatures, typeql_stage)?;
        translated_stages.push(translated);
    }
    Ok(TranslatedPipeline {
        translated_preamble,
        translated_stages,
        variable_registry: translation_context.variable_registry,
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
        _ => todo!(),
    }
    .map_err(|source| QueryError::PatternDefinition { source })
}
