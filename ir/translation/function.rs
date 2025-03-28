/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use error::needs_update_when_feature_is_implemented;
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;
use typeql::{
    common::Spanned,
    schema::definable::function::{
        FunctionBlock, Output, ReturnReduction, ReturnSingle, ReturnStatement, ReturnStream,
    },
    type_::{NamedType, NamedTypeAny},
};

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    pipeline::{
        function::{Function, FunctionBody, ReturnOperation},
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex},
        FunctionRepresentationError, ParameterRegistry,
    },
    translation::{
        pipeline::{translate_pipeline_stages, TranslatedStage},
        reduce::build_reducer,
        TranslationContext,
    },
};

macro_rules! verify_variable_available {
    ($context:ident, $var:expr => $error:ident ) => {
        match $context.get_variable(
            $var.name()
                .ok_or(FunctionRepresentationError::NonAnonymousVariableExpected { source_span: $var.span() })?,
        ) {
            Some(translated) => Ok(translated),
            None => Err(FunctionRepresentationError::$error {
                variable: $var.name().unwrap().to_owned(),
                source_span: $var.span(),
            }),
        }
    };
}
pub fn translate_typeql_function(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    function: &typeql::Function,
) -> Result<Function, Box<FunctionRepresentationError>> {
    translate_function_from(snapshot, function_index, &function.signature, &function.block, Some(function))
}

pub fn translate_function_from(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    signature: &typeql::schema::definable::function::Signature,
    block: &FunctionBlock,
    declaration: Option<&typeql::Function>,
) -> Result<Function, Box<FunctionRepresentationError>> {
    let checked_name = &signature.ident.as_str_unreserved().map_err(|_source| {
        FunctionRepresentationError::IllegalKeywordAsIdentifier {
            identifier: signature.ident.as_str_unchecked().to_owned(),
            source_span: signature.ident.span(),
        }
    })?;
    let argument_labels = signature.args.iter().map(|arg| arg.type_.clone()).collect();
    let args_sources_categories = signature
        .args
        .iter()
        .map(|arg| {
            let name = arg
                .var
                .name()
                .ok_or(FunctionRepresentationError::NonAnonymousVariableExpected { source_span: arg.var.span() })?
                .to_owned();
            Ok::<_, FunctionRepresentationError>((
                name,
                arg.var.span(),
                named_type_any_to_category_and_optionality(&arg.type_).0,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (mut context, arguments) = TranslationContext::new_with_function_arguments(args_sources_categories);
    let mut value_parameters = ParameterRegistry::new();
    let body = translate_function_block(snapshot, function_index, &mut context, &mut value_parameters, block)?;

    // Check for unused arguments
    for (index, &arg) in arguments.iter().enumerate() {
        if !body.stages.iter().any(|stage| {
            if let TranslatedStage::Match { block, .. } = stage {
                block.conjunction().referenced_variables().contains(&arg)
            } else {
                false
            }
        }) {
            let argument_variable = context.variable_registry.get_variable_name(arg).unwrap();
            return Err(Box::new(FunctionRepresentationError::FunctionArgumentUnused {
                variable: argument_variable.clone(),
                source_span: signature.args[index].span.clone(),
            }));
        }
    }
    // Check return declaration aligns with definition
    let returns_consistent = match (&signature.output, &body.return_operation) {
        (Output::Stream(declared_vars), ReturnOperation::Stream(defined_vars, _)) => {
            defined_vars.len() == declared_vars.types.len()
        }
        (Output::Single(declared_vars), ReturnOperation::Single(_, defined_vars, _)) => {
            defined_vars.len() == declared_vars.types.len()
        }
        (Output::Single(declared_vars), ReturnOperation::ReduceReducer(reducers, _)) => {
            reducers.len() == declared_vars.types.len()
        }
        (Output::Single(declared_vars), ReturnOperation::ReduceCheck(_)) => declared_vars.types.len() == 1,
        _ => false,
    };
    if !returns_consistent {
        return Err(Box::new(FunctionRepresentationError::InconsistentReturn {
            signature: signature.clone(),
            return_: block.return_stmt.clone(),
        }));
    }
    Ok(Function::new(
        checked_name,
        context,
        value_parameters,
        arguments,
        Some(argument_labels),
        Some(signature.output.clone()),
        body,
    ))
}

pub(crate) fn translate_function_block(
    snapshot: &impl ReadableSnapshot,
    function_index: &impl FunctionSignatureIndex,
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_block: &FunctionBlock,
) -> Result<FunctionBody, Box<FunctionRepresentationError>> {
    let (stages, fetch) =
        translate_pipeline_stages(snapshot, function_index, context, value_parameters, &function_block.stages)
            .map_err(|typedb_source| FunctionRepresentationError::BlockDefinition { typedb_source })?;

    let has_illegal_stages = stages.iter().any(|stage| match stage {
        TranslatedStage::Insert { .. }
        | TranslatedStage::Update { .. }
        | TranslatedStage::Put { .. }
        | TranslatedStage::Delete { .. } => true,
        TranslatedStage::Match { .. }
        | TranslatedStage::Distinct(_)
        | TranslatedStage::Require(_)
        | TranslatedStage::Select(_)
        | TranslatedStage::Sort(_)
        | TranslatedStage::Offset(_)
        | TranslatedStage::Limit(_)
        | TranslatedStage::Reduce(_) => false,
    });

    if has_illegal_stages {
        return Err(Box::new(FunctionRepresentationError::IllegalStages { source_span: function_block.span.clone() }));
    }
    if let Some(fetch) = fetch {
        return Err(Box::new(FunctionRepresentationError::IllegalFetch { source_span: function_block.span.clone() }));
    }

    let return_operation = match &function_block.return_stmt {
        ReturnStatement::Stream(stream) => build_return_stream(context, stream),
        ReturnStatement::Single(single) => build_return_single(context, single),
        ReturnStatement::Reduce(reduction) => build_return_reduce(context, reduction),
    }?;
    Ok(FunctionBody::new(stages, return_operation))
}

pub fn build_signature(function_id: FunctionID, function: &typeql::Function) -> FunctionSignature {
    let args = function
        .signature
        .args
        .iter()
        .map(|arg| named_type_any_to_category_and_optionality(&arg.type_).0)
        .collect::<Vec<_>>();

    let return_is_stream = matches!(function.signature.output, Output::Stream(_));
    let returns = match &function.signature.output {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    }
    .iter()
    .map(named_type_any_to_category_and_optionality)
    .collect::<Vec<_>>();
    FunctionSignature::new(function_id.clone(), args, returns, return_is_stream)
}

fn named_type_any_to_category_and_optionality(
    named_type_any: &NamedTypeAny,
) -> (VariableCategory, VariableOptionality) {
    let (inner, is_list, optionality) = match named_type_any {
        NamedTypeAny::Simple(inner) => (inner, false, VariableOptionality::Required),
        NamedTypeAny::Optional(optional) => (&optional.inner, false, VariableOptionality::Optional),
        NamedTypeAny::List(list) => (&list.inner, true, VariableOptionality::Required),
    };
    let category = match inner {
        NamedType::Label(_) => {
            needs_update_when_feature_is_implemented!(
                Structs,
                "This could be a struct label. Implement a ThingOrStructValue category"
            );
            if is_list {
                VariableCategory::ThingList
            } else {
                VariableCategory::Thing
            }
        }
        NamedType::BuiltinValueType(_) => {
            if is_list {
                VariableCategory::ValueList
            } else {
                VariableCategory::Value
            }
        }
    };
    (category, optionality)
}

fn build_return_stream(
    context: &TranslationContext,
    stream: &ReturnStream,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    let variables = stream
        .vars
        .iter()
        .map(|typeql_var| verify_variable_available!(context, typeql_var => StreamReturnVariableUnavailable))
        .collect::<Result<Vec<Variable>, FunctionRepresentationError>>()?;
    Ok(ReturnOperation::Stream(variables, stream.span()))
}

fn build_return_single(
    context: &TranslationContext,
    single: &ReturnSingle,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    let variables = single
        .vars
        .iter()
        .map(|typeql_var| verify_variable_available!(context, typeql_var => SingleReturnVariableUnavailable))
        .collect::<Result<Vec<Variable>, FunctionRepresentationError>>()?;
    let selector = single.selector.clone();
    Ok(ReturnOperation::Single(selector, variables, single.span()))
}

fn build_return_reduce(
    context: &TranslationContext,
    reduction: &ReturnReduction,
) -> Result<ReturnOperation, FunctionRepresentationError> {
    match reduction {
        ReturnReduction::Check(_) => Ok(ReturnOperation::ReduceCheck(reduction.span())),
        ReturnReduction::Value(typeql_reducers, _) => {
            let mut reducers = Vec::new();
            for typeql_reducer in typeql_reducers {
                let reducer = build_reducer(context, typeql_reducer)
                    .map_err(|typedb_source| FunctionRepresentationError::ReturnReduction { typedb_source })?;
                reducers.push(reducer);
            }
            Ok(ReturnOperation::ReduceReducer(reducers, reduction.span()))
        }
    }
}
