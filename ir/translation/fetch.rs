/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use error::typedb_error;
use storage::snapshot::ReadableSnapshot;
use typeql::{
    expression::{FunctionCall, FunctionName},
    query::stage::{
        fetch::{
            FetchList as TypeQLFetchList, FetchObject as TypeQLFetchObject, FetchObjectBody as TypeQLFetchObjectBody,
            FetchSingle as TypeQLFetchSingle, FetchSingle, FetchSome as TypeQLFetchSome,
        },
        Fetch as TypeQLFetch,
    },
    schema::definable::function::{FunctionBlock, SingleSelector},
    type_::NamedType,
    value::StringLiteral,
    Expression, TypeRef, TypeRefAny, Variable as TypeQLVariable,
};

use crate::{
    pattern::ParameterID,
    pipeline::{
        block::{Block, BlockBuilder, BlockBuilderContext},
        fetch::{
            FetchListAttributeFromList, FetchObject, FetchObjectAttributes, FetchObjectEntries, FetchSingleAttribute,
            FetchSingleVar, FetchSome,
        },
        function::{AnonymousFunction, FunctionBody, ReturnOperation},
        function_signature::FunctionSignatureIndex,
        FunctionReadError,
    },
    translation::{
        expression::build_expression,
        fetch::FetchRepresentationError::{
            AnonymousVariableEncountered, InvalidAttributeLabelEncountered, NamedVariableEncountered,
            OptionalVariableEncountered, VariableNotAvailable,
        },
        function::translate_function_block,
        pipeline::TranslatedStage,
        TranslationContext,
    },
    RepresentationError,
};

pub(super) fn translate_fetch(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    fetch: &TypeQLFetch,
) -> Result<FetchObject, FetchRepresentationError> {
    translate_fetch_object(snapshot, parent_context, function_index, &fetch.object)
}

// This function returns a specific `FetchObject`, rather than the FetchSome` higher-level enum
// This gives us a simpler entry point for the fetch clause translation
fn translate_fetch_object(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    object: &TypeQLFetchObject,
) -> Result<FetchObject, FetchRepresentationError> {
    match &object.body {
        TypeQLFetchObjectBody::Entries(entries) => {
            let mut object = HashMap::new();
            for entry in entries {
                let (key, value) = (&entry.key, &entry.value);
                let key_id = register_key(parent_context, key);
                object.insert(key_id, translate_fetch_some(snapshot, parent_context, function_index, value)?);
            }
            Ok(FetchObject::Entries(FetchObjectEntries { entries: object }))
        }
        TypeQLFetchObjectBody::AttributesAll(variable) => {
            let var = try_get_variable(parent_context, &variable)?;
            Ok(FetchObject::Attributes(FetchObjectAttributes { variable: var }))
        }
    }
}

fn translate_fetch_some(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    fetch_some: &TypeQLFetchSome,
) -> Result<FetchSome, FetchRepresentationError> {
    match fetch_some {
        TypeQLFetchSome::Object(object) => translate_fetch_object(snapshot, parent_context, function_index, object)
            .map(|object| FetchSome::Object(Box::new(object))),
        TypeQLFetchSome::List(list) => translate_fetch_list(snapshot, parent_context, function_index, list),
        TypeQLFetchSome::Single(some) => translate_fetch_single(snapshot, parent_context, function_index, some),
    }
}

fn translate_fetch_list(
    _snapshot: &impl ReadableSnapshot,
    _parent_context: &mut TranslationContext,
    _function_index: &impl FunctionSignatureIndex,
    _list: &TypeQLFetchList,
) -> Result<FetchSome, FetchRepresentationError> {
    return Err(FetchRepresentationError::Unimplemented {});
}

// Note: TypeQL fetch-single can turn either into a List or a Single IR
fn translate_fetch_single(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    single: &TypeQLFetchSingle,
) -> Result<FetchSome, FetchRepresentationError> {
    match single {
        FetchSingle::Attribute(fetch_attribute) => {
            let owner = try_get_variable(parent_context, &fetch_attribute.owner)?;
            match &fetch_attribute.attribute {
                TypeRefAny::Type(type_ref) => match &type_ref {
                    TypeRef::Named(type_) => match type_ {
                        NamedType::Label(label) => Ok(FetchSome::SingleAttribute(FetchSingleAttribute {
                            variable: owner,
                            attribute: label.ident.as_str().to_owned(),
                        })),
                        NamedType::Role(_) | NamedType::BuiltinValueType(_) => {
                            Err(InvalidAttributeLabelEncountered { declaration: type_ref.clone() })
                        }
                    },
                    TypeRef::Variable(_) => Err(NamedVariableEncountered { declaration: type_ref.clone() }),
                },
                TypeRefAny::List(list) => match &list.inner {
                    TypeRef::Named(named_type) => match named_type {
                        NamedType::Label(label) => Ok(FetchSome::ListAttributesFromList(FetchListAttributeFromList {
                            variable: owner,
                            attribute: label.ident.as_str().to_owned(),
                        })),
                        NamedType::Role(_) | NamedType::BuiltinValueType(_) => {
                            Err(InvalidAttributeLabelEncountered { declaration: list.inner.clone() })
                        }
                    },
                    TypeRef::Variable(_) => Err(NamedVariableEncountered { declaration: list.inner.clone() }),
                },
                TypeRefAny::Optional(_) => {
                    Err(OptionalVariableEncountered { declaration: fetch_attribute.attribute.clone() })
                }
            }
        }
        FetchSingle::Expression(expression) => {
            match &expression {
                Expression::Variable(variable) => {
                    let var = try_get_variable(parent_context, variable)?;
                    Ok(FetchSome::SingleVar(FetchSingleVar { variable: var }))
                }
                Expression::ListIndex(_) | Expression::Value(_) | Expression::Operation(_) | Expression::Paren(_) => {
                    translate_inline_expression(parent_context, function_index, &expression)
                }
                // function expressions may return Single or List, depending on signature invoked
                Expression::Function(call) => {
                    let function_name = match &call.name {
                        FunctionName::Builtin(_) => {
                            return translate_inline_expression(parent_context, function_index, &expression);
                        }
                        FunctionName::Identifier(name) => name.as_str(),
                    };
                    translate_inline_user_function_call(parent_context, function_index, &call, function_name)
                }
                // list expressions should be mapped to FetchList
                Expression::List(_) => {
                    return Err(FetchRepresentationError::Unimplemented {});
                }
                Expression::ListIndexRange(_) => {
                    return Err(FetchRepresentationError::Unimplemented {});
                }
            }
        }
        FetchSingle::FunctionBlock(block) => {
            // clone context, since we don't want the inline function to affect the parent context
            let mut local_context = parent_context.clone();
            let translated = translate_function_block(snapshot, function_index, &mut local_context, &block)
                .map_err(|err| FetchRepresentationError::FunctionRepresentation { declaration: block.clone() })?;
            if translated.return_operation().is_stream() {
                Err(FetchRepresentationError::ExpectedSingleInlineFunction { declaration: block.clone() })
            } else {
                let inline_function = AnonymousFunction::new(local_context, translated);
                Ok(FetchSome::SingleFunction(inline_function))
            }
        }
    }
}

fn translate_inline_expression(
    context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    expression: &Expression,
) -> Result<FetchSome, FetchRepresentationError> {
    // because expressions expect to be able to extract out function calls, we'll translate an expression
    // into a match-return
    // we also don't want anything here to affect the parent contexts, so clone
    let mut local_context = context.clone();
    let builder_context = BlockBuilderContext::new(
        &mut local_context.variable_registry,
        &mut local_context.visible_variables,
        &mut local_context.parameters,
    );
    let mut builder = Block::builder(builder_context);
    let assign_var = add_expression(function_index, &mut builder, &expression)?;
    let block = TranslatedStage::Match { block: builder.finish() };
    let return_ = ReturnOperation::Single(SingleSelector::First, vec![assign_var]);
    Ok(FetchSome::SingleFunction(AnonymousFunction::new(local_context, FunctionBody::new(vec![block], return_))))
}

fn translate_inline_user_function_call(
    context: &mut TranslationContext,
    function_index: &impl FunctionSignatureIndex,
    call: &FunctionCall,
    function_name: &str,
) -> Result<FetchSome, FetchRepresentationError> {
    let signature = function_index
        .get_function_signature(function_name)
        .map_err(|err| FetchRepresentationError::FunctionRetrieval { name: function_name.to_owned(), source: err })?
        .ok_or_else(|| FetchRepresentationError::FunctionNotFound {
            name: function_name.to_owned(),
            declaration: call.clone(),
        })?;

    // because function calls expect to be able to extract out expression calls, we'll translate
    // into a match-return
    // we also don't want anything here to affect the parent contexts, so clone
    let mut local_context = context.clone();
    let builder_context = BlockBuilderContext::new(
        &mut local_context.variable_registry,
        &mut local_context.visible_variables,
        &mut local_context.parameters,
    );
    let mut builder = Block::builder(builder_context);
    let mut assign_vars = Vec::new();
    for _ in &signature.returns {
        assign_vars.push(builder.conjunction_mut().declare_variable_anonymous().map_err(|err| {
            FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: Box::new(err) }
        })?);
    }
    let mut arg_vars = Vec::new();
    for arg in &call.args {
        arg_vars.push(add_expression(function_index, &mut builder, arg)?);
    }

    builder
        .conjunction_mut()
        .constraints_mut()
        .add_function_binding(assign_vars.clone(), &signature, arg_vars, function_name)
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: Box::new(err) })?;

    let stage = TranslatedStage::Match { block: builder.finish() };
    if signature.return_is_stream {
        let return_ = ReturnOperation::Stream(assign_vars);
        Ok(FetchSome::ListFunction(AnonymousFunction::new(local_context, FunctionBody::new(vec![stage], return_))))
    } else {
        let return_ = ReturnOperation::Single(SingleSelector::First, assign_vars);
        Ok(FetchSome::SingleFunction(AnonymousFunction::new(local_context, FunctionBody::new(vec![stage], return_))))
    }
}

fn add_expression(
    function_index: &impl FunctionSignatureIndex,
    builder: &mut BlockBuilder<'_>,
    expression: &Expression,
) -> Result<Variable, FetchRepresentationError> {
    let mut conjunction_builder = builder.conjunction_mut();
    let assign_var = conjunction_builder
        .declare_variable_anonymous()
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: Box::new(err) })?;
    let expression = build_expression(function_index, &mut conjunction_builder.constraints_mut(), &expression)
        .map_err(|err| FetchRepresentationError::ExpressionRepresentation { typedb_source: Box::new(err) })?;
    let _ = conjunction_builder
        .constraints_mut()
        .add_assignment(assign_var.clone(), expression)
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: Box::new(err) })?;
    Ok(assign_var)
}

fn try_get_variable(
    context: &TranslationContext,
    variable: &TypeQLVariable,
) -> Result<Variable, FetchRepresentationError> {
    let name = match variable {
        TypeQLVariable::Anonymous { .. } => return Err(AnonymousVariableEncountered { declaration: variable.clone() }),
        TypeQLVariable::Named { .. } => variable.name().unwrap(),
    };
    context
        .get_variable(name)
        .ok_or_else(|| VariableNotAvailable { variable: name.to_owned(), declaration: variable.clone() })
}

fn register_key(context: &mut TranslationContext, key: &StringLiteral) -> ParameterID {
    context.parameters.register_fetch_key(key.value.to_owned())
}

typedb_error!(
    pub FetchRepresentationError(component = "Fetch representation", prefix = "FER") {
        Unimplemented(
            0,
            "Functionality is not implemented."
        ),
        AnonymousVariableEncountered(
            1,
            "Encountered anonymous variable where it is not permitted.\nSource:\n{declaration}",
            declaration: TypeQLVariable
        ),
        OptionalVariableEncountered(
            2,
            "Encountered optional variable where it is not permitted.\nSource:\n{declaration}",
            declaration: TypeRefAny
        ),
        NamedVariableEncountered(
            3,
            "Encountered named variable where it is not permitted.\nSource:\n{declaration}",
            declaration: TypeRef
        ),
        InvalidAttributeLabelEncountered(
            4,
            "Encountered invalid label, this pattern requires an attribute label.\nSource:\n{declaration}",
            declaration: TypeRef
        ),
        VariableNotAvailable(
            5,
            "The variable '{variable}' is not available.\nSource:\n{declaration}",
            variable: String,
            declaration: TypeQLVariable
        ),
        ExpressionRepresentation(
            6,
            "Error building representation of expression.",
            ( typedb_source : Box<RepresentationError> )
        ),
        ExpressionAsMatchRepresentation(
            7,
            "Failed to convert fetch-expression ('key': <expression>) into full match-return ('key': (match $anon = <expression>; return first $anon;)) failed.",
            ( typedb_source : Box<RepresentationError> )
        ),
        FunctionRetrieval(
            8,
            "Error while retrieving function '{name}'.",
            name: String,
            ( source : FunctionReadError )
        ),
        FunctionNotFound(
            9,
            "Function '{name}' was not found.",
            name: String,
            declaration: FunctionCall
        ),
        FunctionRepresentation(
            10,
            "Failed to build inline function representation.",
            declaration: FunctionBlock
        ),
        ExpectedSingleInlineFunction(
            11,
            "The inline match-return function declaration returns a stream, which must be wrapped in `[]` to collect into a list.\nSource:\n{declaration}",
            declaration: FunctionBlock
        ),
    }
);
