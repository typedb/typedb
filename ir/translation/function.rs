/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::{
    schema::definable::function::{Output, ReturnReduction, ReturnSingle, ReturnStatement, ReturnStream},
    TypeRefAny,
};
use typeql::schema::definable::function::FunctionBlock;

use answer::variable::Variable;
use storage::snapshot::ReadableSnapshot;

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::{
        function::{Function, ReturnOperation},
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex},
        FunctionRepresentationError,
    },
    translation::{pipeline::translate_pipeline_stages, TranslationContext},
};
use crate::program::function::FunctionBody;
use crate::translation::reduce::build_reducer;

pub fn translate_function(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    function: &typeql::Function,
) -> Result<Function, FunctionRepresentationError> {
    let mut context = TranslationContext::new();
    let arguments: Vec<Variable> = function
        .signature
        .args
        .iter()
        .map(|typeql_arg| {
            context.get_variable(typeql_arg.var.name().unwrap()).ok_or_else(||
                FunctionRepresentationError::FunctionArgumentUnused {
                    argument_variable: typeql_arg.var.name().unwrap().to_string(),
                    declaration: function.clone(),
                }
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let body = translate_function_block(snapshot, function_index, &mut context, &function.block)?;
    Ok(Function::new(
        function.signature.ident.as_str(),
        context,
        arguments,
        body,
    ))
}

pub(crate) fn translate_function_block(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    context: &mut TranslationContext,
    function_block: &FunctionBlock,
) -> Result<FunctionBody, FunctionRepresentationError> {
    let stages = translate_pipeline_stages(snapshot, function_index, context, &function_block.stages)
        .map_err(|err| {
            FunctionRepresentationError::BlockDefinition {
                declaration: function_block.clone(),
                typedb_source: Box::new(err),
            }
        })?;

    let return_operation = match &function_block.return_stmt {
        ReturnStatement::Stream(stream) => build_return_stream(&context, stream),
        ReturnStatement::Single(single) => build_return_single(&context, single),
        ReturnStatement::Reduce(reduction) => build_return_reduce(&context, reduction),
    }?;
    Ok(FunctionBody::new(stages, return_operation))
}

pub fn build_signature(function_id: FunctionID, function: &typeql::Function) -> FunctionSignature {
    let args = function
        .signature
        .args
        .iter()
        .map(|arg| type_any_to_category_and_optionality(&arg.type_).0)
        .collect::<Vec<_>>();

    let return_is_stream = matches!(function.signature.output, Output::Stream(_));
    let returns = match &function.signature.output {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    }
        .iter()
        .map(type_any_to_category_and_optionality)
        .collect::<Vec<_>>();
    FunctionSignature::new(function_id.clone(), args, returns, return_is_stream)
}

fn type_any_to_category_and_optionality(type_any: &TypeRefAny) -> (VariableCategory, VariableOptionality) {
    match type_any {
        TypeRefAny::Type(_) => (VariableCategory::Thing, VariableOptionality::Required),
        TypeRefAny::Optional(_) => (VariableCategory::Thing, VariableOptionality::Optional),
        TypeRefAny::List(_) => (VariableCategory::ThingList, VariableOptionality::Required),
    }
}

fn build_return_stream(
    context: &TranslationContext,
    stream: &ReturnStream,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    let variables = stream
        .vars
        .iter()
        .map(|typeql_var| {
            context.get_variable(typeql_var.name().unwrap()).ok_or_else(||
                FunctionRepresentationError::StreamReturnVariableUnavailable {
                    return_variable: typeql_var.name().unwrap().to_string(),
                    declaration: stream.clone(),
                }
            )
        })
        .collect::<Result<Vec<Variable>, FunctionRepresentationError>>()?;
    Ok(ReturnOperation::Stream(variables))
}

fn build_return_single(
    context: &TranslationContext,
    single: &ReturnSingle,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    let variables = single
        .vars
        .iter()
        .map(|typeql_var| {
            context.get_variable(typeql_var.name().unwrap()).ok_or_else(||
                FunctionRepresentationError::SingleReturnVariableUnavailable {
                    return_variable: typeql_var.name().unwrap().to_string(),
                    declaration: single.clone(),
                }
            )
        })
        .collect::<Result<Vec<Variable>, FunctionRepresentationError>>()?;
    let selector = single.selector.clone();
    Ok(ReturnOperation::Single(selector, variables))
}

fn build_return_reduce(
    context: &TranslationContext,
    reduction: &ReturnReduction,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    match reduction {
        ReturnReduction::Check(_) => Ok(ReturnOperation::ReduceCheck()),
        ReturnReduction::Value(typeql_reducers) => {
            let mut reducers = Vec::new();
            for typeql_reducer in typeql_reducers {
                let reducer = build_reducer(context, typeql_reducer)
                    .map_err(|err| FunctionRepresentationError::ReturnReduction {
                        declaration: reduction.clone(),
                        typedb_source: Box::new(err.clone()),
                    })?;
                reducers.push(reducer);
            }
            Ok(ReturnOperation::ReduceReducer(reducers))
        }
    }
}
