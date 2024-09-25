/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use typeql::{
    schema::definable::function::{Output, ReturnStatement, ReturnStream},
    TypeRefAny,
};
use typeql::schema::definable::function::{ReturnReduction, ReturnSingle};
use storage::snapshot::ReadableSnapshot;

use crate::{
    pattern::{
        variable_category::{VariableCategory, VariableOptionality},
        ScopeId,
    },
    program::{
        block::{BlockBuilderContext, Block},
        function::{Function, ReturnOperation},
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex},
        FunctionRepresentationError,
    },
    translation::{match_::add_patterns, TranslationContext},
};
use crate::translation::pipeline::{translate_pipeline, translate_pipeline_stages};

pub fn translate_function(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    function: &typeql::Function,
) -> Result<Function, FunctionRepresentationError> {
    let mut context = TranslationContext::new();
    let pipeline = translate_pipeline_stages(
        snapshot,
        function_index,
        &mut context,
        &function.block.stages
    ).map_err(|err| FunctionRepresentationError::PatternDefinition { declaration: function.clone(), typedb_source: Box::new(err) })?;

    // TODO: update...
    let mut builder = Block::builder(context.next_block_context());
    let return_operation = match &function.block.return_stmt {
        ReturnStatement::Stream(stream) => build_return_stream(builder.context_mut(), stream),
        ReturnStatement::Single(single) => build_return_single(builder.context_mut(), single),
        ReturnStatement::Reduce(reduction) => build_return_reduce(builder.context_mut(), reduction),
    }?;
    let arguments: Vec<Variable> = function
        .signature
        .args
        .iter()
        .map(|typeql_arg| {
            get_variable_in_block_root(builder.context_mut(), &typeql_arg.var, |var| {
                FunctionRepresentationError::FunctionArgumentUnused {
                    argument_variable: var.name().unwrap().to_string(),
                    declaration: function.clone(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Function::new(
        function.signature.ident.as_str(),
        pipeline,
        context.variable_registry,
        arguments,
        return_operation,
    ))
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
    context: &BlockBuilderContext<'_>,
    stream: &ReturnStream,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    let variables = stream
        .vars
        .iter()
        .map(|typeql_var| {
            get_variable_in_block_root(context, typeql_var, |var| {
                FunctionRepresentationError::ReturnVariableUnavailable {
                    return_variable: var.name().unwrap().to_string(),
                    declaration: stream.clone(),
                }
            })
        })
        .collect::<Result<Vec<Variable>, FunctionRepresentationError>>()?;
    Ok(ReturnOperation::Stream(variables))
}

fn build_return_single(context: &BlockBuilderContext<'_>, single: &ReturnSingle) -> Result<ReturnOperation, FunctionRepresentationError> {
    todo!()
}

fn build_return_reduce(
    context: &BlockBuilderContext<'_>,
    reduction: &ReturnReduction,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    // Ok(ReturnOperation::Reduction(reducers))
    todo!()
}

fn get_variable_in_block_root<F>(
    context: &BlockBuilderContext<'_>,
    typeql_var: &typeql::Variable,
    err: F,
) -> Result<Variable, FunctionRepresentationError>
where
    F: FnOnce(&typeql::Variable) -> FunctionRepresentationError,
{
    context
        .get_variable_named(typeql_var.name().unwrap(), ScopeId::ROOT)
        .map_or_else(|| Err(err(typeql_var)), |var| Ok(*var))
}
