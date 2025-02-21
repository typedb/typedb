/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use encoding::value::label::Label;
use error::typedb_error;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::ReadableSnapshot;
use typeql::{
    common::{Span, Spanned},
    expression::{FunctionCall, FunctionName},
    query::stage::{
        fetch::{
            FetchAttribute, FetchList as TypeQLFetchList, FetchObject as TypeQLFetchObject,
            FetchObjectBody as TypeQLFetchObjectBody, FetchSingle as TypeQLFetchSingle, FetchSingle,
            FetchSome as TypeQLFetchSome, FetchStream,
        },
        Fetch as TypeQLFetch,
    },
    schema::definable::function::{FunctionBlock, SingleSelector},
    value::StringLiteral,
    Expression, TypeRef, TypeRefAny, Variable as TypeQLVariable,
};

use crate::{
    pattern::ParameterID,
    pipeline::{
        block::{Block, BlockBuilder, BlockBuilderContext},
        fetch::{
            FetchListAttributeAsList, FetchListAttributeFromList, FetchListSubFetch, FetchObject, FetchSingleAttribute,
            FetchSome,
        },
        function::{Function, FunctionBody, ReturnOperation},
        function_signature::{FunctionSignature, FunctionSignatureIndex},
        FunctionReadError, ParameterRegistry,
    },
    translation::{
        expression::{add_user_defined_function_call, build_expression},
        fetch::FetchRepresentationError::{
            AnonymousVariableEncountered, InvalidAttributeLabelEncountered, NamedVariableEncountered,
            VariableNotAvailable,
        },
        function::translate_function_block,
        literal::FromTypeQLLiteral,
        pipeline::{translate_pipeline_stages, TranslatedStage},
        tokens::checked_identifier,
        TranslationContext,
    },
    RepresentationError,
};

pub(super) fn translate_fetch(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    fetch: &TypeQLFetch,
) -> Result<FetchObject, Box<FetchRepresentationError>> {
    translate_fetch_object(snapshot, parent_context, value_parameters, function_index, &fetch.object)
}

// This function returns a specific `FetchObject`, rather than the FetchSome` higher-level enum
// This gives us a simpler entry point for the fetch stage translation
fn translate_fetch_object(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    typeql_object: &TypeQLFetchObject,
) -> Result<FetchObject, Box<FetchRepresentationError>> {
    match &typeql_object.body {
        TypeQLFetchObjectBody::Entries(entries) => {
            let mut object = HashMap::new();
            let mut source_spans = HashMap::new();
            let mut unique_keys: HashSet<&str> = HashSet::new();
            for entry in entries {
                let (key, value) = (&entry.key, &entry.value);
                if !unique_keys.insert(key.value.as_ref()) {
                    return Err(Box::new(FetchRepresentationError::DuplicatedObjectKeyEncountered {
                        key: key.value.clone(),
                        declaration: typeql_object.clone(),
                    }));
                }

                let key_id = register_key(
                    value_parameters,
                    key,
                    entry.span().expect("Parser did not provide Fetch key-value text range."),
                );
                source_spans.insert(key_id, entry.span());
                object.insert(
                    key_id,
                    translate_fetch_some(snapshot, parent_context, value_parameters, function_index, value)?,
                );
            }
            Ok(FetchObject::Entries(object, source_spans))
        }
        TypeQLFetchObjectBody::AttributesAll(variable) => {
            let var = try_get_variable(parent_context, variable)?;
            Ok(FetchObject::Attributes(var, variable.span()))
        }
    }
}

fn translate_fetch_some(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    fetch_some: &TypeQLFetchSome,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    match fetch_some {
        TypeQLFetchSome::Object(object) => {
            translate_fetch_object(snapshot, parent_context, value_parameters, function_index, object)
                .map(|object| FetchSome::Object(Box::new(object)))
        }
        TypeQLFetchSome::List(list) => {
            translate_fetch_list(snapshot, parent_context, value_parameters, function_index, list)
        }
        TypeQLFetchSome::Single(some) => {
            translate_fetch_single(snapshot, parent_context, value_parameters, function_index, some)
        }
    }
}

fn translate_fetch_list(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    list: &TypeQLFetchList,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    match &list.stream {
        FetchStream::Attribute(fetch_attribute) => {
            let owner = try_get_variable(parent_context, &fetch_attribute.owner)?;
            let (is_list, attribute) = extract_fetch_attribute(fetch_attribute)?;
            if is_list {
                Err(Box::new(FetchRepresentationError::AttributeListInList { declaration: fetch_attribute.clone() }))
            } else {
                Ok(FetchSome::ListAttributesAsList(FetchListAttributeAsList { variable: owner, attribute }))
            }
        }
        FetchStream::Function(call) => {
            match &call.name {
                FunctionName::Builtin(name) => {
                    // built-in functions always return single values, so should not be wrapped in a list (although we could allow it if it's something interesting)
                    Err(Box::new(FetchRepresentationError::BuiltinFunctionInList { declaration: call.clone() }))
                }
                FunctionName::Identifier(name) => {
                    let checked_name = checked_identifier(name)
                        .map_err(|typedb_source| FetchRepresentationError::SubFetchRepresentation { typedb_source })?;
                    translate_inline_user_function_call_stream(
                        parent_context,
                        value_parameters,
                        function_index,
                        call,
                        checked_name,
                    )
                }
            }
        }
        FetchStream::SubQueryFetch(stages) => {
            // clone context, since we don't want the inline function to affect the parent context
            let mut local_context = parent_context.clone();
            let (translated_stages, subfetch) =
                translate_pipeline_stages(snapshot, function_index, &mut local_context, value_parameters, stages)
                    .map_err(|err| FetchRepresentationError::SubFetchRepresentation { typedb_source: err })?;
            if let Some(subfetch) = subfetch {
                let input_variables = find_sub_fetch_inputs(parent_context, &translated_stages, &subfetch);
                Ok(FetchSome::ListSubFetch(FetchListSubFetch {
                    context: local_context,
                    input_variables,
                    stages: translated_stages,
                    fetch: subfetch,
                }))
            } else {
                Err(Box::new(FetchRepresentationError::SubQueryIsNotFetch { declaration: list.clone() }))
            }
        }
        FetchStream::SubQueryFunctionBlock(block) => {
            // clone context, since we don't want the inline function to affect the parent context
            let mut local_context = parent_context.clone();
            let body = translate_function_block(snapshot, function_index, &mut local_context, value_parameters, block)
                .map_err(|err| FetchRepresentationError::FunctionRepresentation { declaration: block.clone() })?;
            if !body.return_operation.is_scalar()
                && !matches!(body.return_operation, ReturnOperation::ReduceReducer(_, _))
            {
                return Err(Box::new(FetchRepresentationError::ExpectedScalarOrReduceFunctionBlock {
                    declaration: block.clone(),
                }));
            }
            let args = find_function_body_arguments(parent_context, &body);
            Ok(FetchSome::ListFunction(create_anonymous_function(local_context, value_parameters.clone(), args, body)))
        }
    }
}

// Note: TypeQL fetch-single can turn either into a List or a Single IR
fn translate_fetch_single(
    snapshot: &impl ReadableSnapshot,
    parent_context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    single: &TypeQLFetchSingle,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    match single {
        FetchSingle::Attribute(fetch_attribute) => {
            let owner = try_get_variable(parent_context, &fetch_attribute.owner)?;
            let (is_list, attribute) = extract_fetch_attribute(fetch_attribute)?;
            if is_list {
                Ok(FetchSome::ListAttributesFromList(FetchListAttributeFromList { variable: owner, attribute }))
            } else {
                Ok(FetchSome::SingleAttribute(FetchSingleAttribute { variable: owner, attribute }))
            }
        }
        FetchSingle::Expression(expression) => match &expression {
            Expression::Variable(variable) => {
                let var = try_get_variable(parent_context, variable)?;
                Ok(FetchSome::SingleVar(var))
            }
            Expression::ListIndex(_) | Expression::Value(_) | Expression::Operation(_) | Expression::Paren(_) => {
                translate_inline_expression_single(parent_context, value_parameters, function_index, expression)
            }
            Expression::Function(call) => match &call.name {
                FunctionName::Builtin(_) => {
                    translate_inline_expression_single(parent_context, value_parameters, function_index, expression)
                }
                FunctionName::Identifier(name) => {
                    let checked_name = checked_identifier(name)
                        .map_err(|typedb_source| FetchRepresentationError::SubFetchRepresentation { typedb_source })?;
                    translate_inline_user_function_call_single(
                        parent_context,
                        value_parameters,
                        function_index,
                        call,
                        checked_name,
                    )
                }
            },
            Expression::List(_) => {
                Err(Box::new(FetchRepresentationError::Unimplemented { description: "list expressions".to_string() }))
            }
            Expression::ListIndexRange(_) => {
                Err(Box::new(FetchRepresentationError::Unimplemented { description: "list index range".to_string() }))
            }
        },
        FetchSingle::FunctionBlock(block) => {
            // clone context, since we don't want the inline function to affect the parent context
            let mut local_context = parent_context.clone();
            let body = translate_function_block(snapshot, function_index, &mut local_context, value_parameters, block)
                .map_err(|err| FetchRepresentationError::FunctionRepresentation { declaration: block.clone() })?;
            if body.return_operation().is_stream() {
                return Err(Box::new(FetchRepresentationError::ExpectedSingleFunctionBlock {
                    declaration: block.clone(),
                }));
            }
            if !body.return_operation.is_scalar() {
                return Err(Box::new(FetchRepresentationError::ExpectedScalarFunctionBlock {
                    declaration: block.clone(),
                }));
            }
            Ok(FetchSome::SingleFunction(create_anonymous_function(
                local_context,
                value_parameters.clone(),
                find_function_body_arguments(parent_context, &body),
                body,
            )))
        }
    }
}

fn extract_fetch_attribute(fetch_attribute: &FetchAttribute) -> Result<(bool, Label), Box<FetchRepresentationError>> {
    match &fetch_attribute.attribute {
        TypeRefAny::Type(type_ref) => match &type_ref {
            TypeRef::Label(label) => {
                let checked_label = checked_identifier(&label.ident).map_err(|typedb_source| {
                    Box::new(FetchRepresentationError::SubFetchRepresentation { typedb_source })
                })?;
                Ok((false, Label::parse_from(checked_label, label.span())))
            }
            TypeRef::Scoped(_) => Err(Box::new(InvalidAttributeLabelEncountered { declaration: type_ref.clone() })),
            TypeRef::Variable(_) => Err(Box::new(NamedVariableEncountered { declaration: type_ref.clone() })),
        },
        TypeRefAny::List(list) => match &list.inner {
            TypeRef::Label(label) => {
                let checked_label = checked_identifier(&label.ident).map_err(|typedb_source| {
                    Box::new(FetchRepresentationError::SubFetchRepresentation { typedb_source })
                })?;
                Ok((true, Label::parse_from(checked_label, label.span())))
            }
            TypeRef::Scoped(_) => Err(Box::new(InvalidAttributeLabelEncountered { declaration: list.inner.clone() })),
            TypeRef::Variable(_) => Err(Box::new(NamedVariableEncountered { declaration: list.inner.clone() })),
        },
    }
}

// translate an expression that produces a single output (not a stream)
fn translate_inline_expression_single(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    expression: &Expression,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    // because expressions expect to be able to extract out function calls, we'll translate an expression
    // into a match-return
    // we also don't want anything here to affect the parent contexts, so clone
    let mut local_context = context.clone();
    let builder_context = BlockBuilderContext::new(
        &mut local_context.variable_registry,
        &mut local_context.visible_variables,
        value_parameters,
    );
    let mut builder = Block::builder(builder_context);
    let assign_var = add_expression(function_index, &mut builder, expression)?;
    let block = builder
        .finish()
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: err })?;
    let match_stage = TranslatedStage::Match { block, source_span: expression.span() };
    let return_ = ReturnOperation::Single(SingleSelector::First, vec![assign_var], expression.span());
    let body = FunctionBody::new(vec![match_stage], return_);
    let args = find_function_body_arguments(context, &body);
    Ok(FetchSome::SingleFunction(create_anonymous_function(local_context, value_parameters.clone(), args, body)))
}

fn translate_inline_user_function_call_single(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    call: &FunctionCall,
    function_name: &str,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    let (local_context, stage, assign_vars, signature) =
        translate_inline_user_function_call(context, value_parameters, function_index, call, function_name)?;
    if signature.return_is_stream {
        Err(Box::new(FetchRepresentationError::ExpectedSingleInlineFunctionCall { declaration: call.clone() }))
    } else {
        let parameters = value_parameters.clone();
        let return_ = ReturnOperation::Single(SingleSelector::First, assign_vars, call.span());
        let body = FunctionBody::new(vec![stage], return_);
        let args = find_function_body_arguments(context, &body);
        Ok(FetchSome::SingleFunction(create_anonymous_function(local_context, parameters, args, body)))
    }
}

fn translate_inline_user_function_call_stream(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &impl FunctionSignatureIndex,
    call: &FunctionCall,
    function_name: &str,
) -> Result<FetchSome, Box<FetchRepresentationError>> {
    let (local_context, stage, assign_vars, _) =
        translate_inline_user_function_call(context, value_parameters, function_index, call, function_name)?;
    let parameters = value_parameters.clone();
    let return_ = ReturnOperation::Stream(assign_vars, call.span());
    let body = FunctionBody::new(vec![stage], return_);
    let args = find_function_body_arguments(context, &body);
    Ok(FetchSome::ListFunction(create_anonymous_function(local_context, parameters, args, body)))
}

fn translate_inline_user_function_call<'a>(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    function_index: &'a impl FunctionSignatureIndex,
    call: &FunctionCall,
    function_name: &str,
) -> Result<
    (TranslationContext, TranslatedStage, Vec<Variable>, MaybeOwns<'a, FunctionSignature>),
    Box<FetchRepresentationError>,
> {
    let signature = function_index
        .get_function_signature(function_name)
        .map_err(|err| FetchRepresentationError::FunctionRetrieval {
            name: function_name.to_owned(),
            typedb_source: err,
        })?
        .ok_or_else(|| FetchRepresentationError::FunctionNotFound {
            name: function_name.to_owned(),
            declaration: call.clone(),
        })?;

    if signature.returns.len() > 1 {
        return Err(Box::new(FetchRepresentationError::ExpectedScalarInlineFunctionCall { declaration: call.clone() }));
    }

    // because function calls expect to be able to extract out expression calls, we'll translate
    // into a match-return
    // we also don't want anything here to affect the parent contexts, so clone
    let mut local_context = context.clone();
    let builder_context = BlockBuilderContext::new(
        &mut local_context.variable_registry,
        &mut local_context.visible_variables,
        value_parameters,
    );
    let mut builder = Block::builder(builder_context);

    let mut assign_vars = Vec::new();
    for _ in &signature.returns {
        assign_vars.push(
            builder
                .conjunction_mut()
                .constraints_mut()
                .create_anonymous_variable(None)
                .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: err })?,
        );
    }

    add_user_defined_function_call(
        function_index,
        &mut builder.conjunction_mut().constraints_mut(),
        function_name,
        assign_vars.clone(),
        &call.args,
        call.span(),
    )
    .map_err(|typedb_source| FetchRepresentationError::ExpressionRepresentation { typedb_source })?;

    let block = builder
        .finish()
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: err })?;
    let stage = TranslatedStage::Match { block, source_span: call.span() };

    Ok((local_context, stage, assign_vars, signature))
}

fn add_expression(
    function_index: &impl FunctionSignatureIndex,
    builder: &mut BlockBuilder<'_>,
    typeql_expression: &Expression,
) -> Result<Variable, Box<FetchRepresentationError>> {
    let mut conjunction_builder = builder.conjunction_mut();
    let assign_var = conjunction_builder
        .constraints_mut()
        .create_anonymous_variable(None)
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: err })?;
    let expression = build_expression(function_index, &mut conjunction_builder.constraints_mut(), typeql_expression)
        .map_err(|err| FetchRepresentationError::ExpressionRepresentation { typedb_source: err })?;
    let _ = conjunction_builder
        .constraints_mut()
        .add_assignment(assign_var, expression, typeql_expression.span())
        .map_err(|err| FetchRepresentationError::ExpressionAsMatchRepresentation { typedb_source: err })?;
    Ok(assign_var)
}

fn create_anonymous_function(
    context: TranslationContext,
    parameters: ParameterRegistry,
    args: Vec<Variable>,
    body: FunctionBody,
) -> Function {
    Function::new("_generated_fetch_inline_function_", context, parameters, args, None, None, body)
}

// Given a function body, and the _parent_ translation context, we can reconstruct which are arguments
fn find_function_body_arguments(parent_context: &TranslationContext, function_body: &FunctionBody) -> Vec<Variable> {
    let mut arguments = HashSet::new();
    // Note: we rely on the fact that named variables that are "the same" become the same Variable, and the logic of
    //       selecting variables in/out is handled by the translation of the stages
    for stage in function_body.stages() {
        for var in stage.variables() {
            if parent_context.variable_registry.has_variable_as_named(&var) {
                arguments.insert(var);
            }
        }
    }
    for var in function_body.return_operation.variables().iter() {
        if parent_context.variable_registry.has_variable_as_named(var) {
            arguments.insert(*var);
        }
    }
    Vec::from_iter(arguments)
}

fn find_sub_fetch_inputs(
    parent_context: &TranslationContext,
    stages: &[TranslatedStage],
    fetch: &FetchObject,
) -> HashSet<Variable> {
    let mut arguments = HashSet::new();
    // Note: we rely on the fact that named variables that are "the same" become the same Variable, and the logic of
    //       selecting variables in/out is handled by the translation of the stages
    for stage in stages {
        for var in stage.variables() {
            if parent_context.variable_registry.has_variable_as_named(&var) {
                arguments.insert(var);
            }
        }
    }
    let mut fetch_vars = HashSet::new();
    fetch.record_variables_recursive(&mut fetch_vars);
    for var in fetch_vars {
        if parent_context.variable_registry.has_variable_as_named(&var) {
            arguments.insert(var);
        }
    }
    arguments
}

fn try_get_variable(
    context: &TranslationContext,
    variable: &TypeQLVariable,
) -> Result<Variable, Box<FetchRepresentationError>> {
    let name = match variable {
        TypeQLVariable::Anonymous { .. } => {
            return Err(Box::new(AnonymousVariableEncountered { declaration: variable.clone() }))
        }
        TypeQLVariable::Named { .. } => variable.name().unwrap(),
    };
    context
        .get_variable(name)
        .ok_or_else(|| Box::new(VariableNotAvailable { variable: name.to_owned(), declaration: variable.clone() }))
}

fn register_key(parameters: &mut ParameterRegistry, key: &StringLiteral, span: Span) -> ParameterID {
    parameters.register_fetch_key(String::from_typeql_literal(key, Some(span)).unwrap(), span)
}

typedb_error! {
    pub FetchRepresentationError(component = "Fetch representation", prefix = "FER") {
        Unimplemented(
            0,
            "Functionality is not implemented: {description}.",
            description: String
        ),
        AnonymousVariableEncountered(
            1,
            "Encountered anonymous variable where it is not permitted.\nSource:\n{declaration}",
            declaration: TypeQLVariable
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
            typedb_source: Box<RepresentationError>
        ),
        ExpressionAsMatchRepresentation(
            7,
            "Failed to convert fetch-expression ('key': <expression>) into full match-return ('key': (match $anon = <expression>; return first $anon;)) failed.",
            typedb_source: Box<RepresentationError>
        ),
        FunctionRetrieval(
            8,
            "Error while retrieving function '{name}'.",
            name: String,
            typedb_source: FunctionReadError
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
        ExpectedSingleFunctionBlock(
            11,
            "The match-return returns a stream, which must be wrapped in `[]` to collect into a list.\nSource:\n{declaration}",
            declaration: FunctionBlock
        ),
        ExpectedScalarFunctionBlock(
            12,
            "The match-return returns a non-scalar result, which cannot be represented as a single value.\nSource:\n{declaration}",
            declaration: FunctionBlock
        ),
        ExpectedScalarOrReduceFunctionBlock(
            13,
            "The match-return returns a non-scalar non-reduce result, which cannot be represented in a list.\nSource:\n{declaration}",
            declaration: FunctionBlock
        ),
        ExpectedSingleInlineFunctionCall(
            14,
            "The function returns a stream, which must be wrapped in `[]` to collect into a list.\nSource:\n{declaration}",
            declaration: FunctionCall
        ),
        ExpectedScalarInlineFunctionCall(
            15,
            "The function returns a non-scalar result, which cannot be used as an inline call. Consider using a function block instead.\nSource:\n{declaration}",
            declaration: FunctionCall
        ),
        ExpectedStreamUserFunctionInList(
            16,
            "Illegal call to non-streaming function '{name}' inside a list '[]'. Use '()' or no bracketing to invoke functions that do not return streams.\nSource:\n{declaration}",
            name: String,
            declaration: FunctionCall
        ),
        BuiltinFunctionInList(
            17,
            "Built-in functions returning a single value should not be wrapped in '[]'. User-defined functions that return streams can be wrapped in '[]'.\nSource:\n{declaration}",
            declaration: FunctionCall
        ),
        AttributeListInList(
            18,
            "Fetching attributes as list should not be wrapped in another list, please remove the outer [].\nSource:\n{declaration}",
            declaration: FetchAttribute
        ),
        SubFetchRepresentation(
            19,
            "Error building representation of fetch sub-query.",
            typedb_source: Box<RepresentationError>
        ),
        SubQueryIsNotFetch(
            20,
            "Error building representation of fetch sub-query: no fetch found. Consider using expressions or functions.\nSource:\n{declaration}",
            declaration: TypeQLFetchList
        ),
        DuplicatedObjectKeyEncountered(
            21,
            "Encountered multiple mappings for one key {key} in a single object.\nSource:\n{declaration}",
            key: String,
            declaration: TypeQLFetchObject
        ),
    }
}
