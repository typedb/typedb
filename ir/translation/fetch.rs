/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use typeql::query::stage::Fetch as TypeQLFetch;
use typeql::query::stage::fetch::{FetchObject as TypeQLFetchObject, FetchObjectBody as TypeQLFetchObjectBody, FetchSome as TypeQLFetchSome, FetchList as TypeQLFetchList, FetchSingle as TypeQLFetchSingle, FetchSingle};
use typeql::value::StringLiteral;
use typeql::{Expression, TypeRef, TypeRefAny, Variable as TypeQLVariable};
use typeql::type_::NamedType;
use typeql::Variable::Named;
use answer::variable::Variable;
use error::typedb_error;
use crate::pattern::ParameterID;
use crate::program::block::{Block, BlockBuilderContext};
use crate::program::fetch::{FetchListAttributeFromList, FetchObject, FetchObjectAttributes, FetchObjectStatic, FetchSingleAttribute, FetchSingleVar, FetchSome};
use crate::translation::expression::build_expression;
use crate::translation::fetch::FetchRepresentationError::{AnonymousVariableEncountered, InvalidAttributeLabelEncountered, NamedVariableEncountered, OptionalVariableEncountered, VariableNotAvailable};

use crate::translation::TranslationContext;

pub(super) fn translate_fetch(context: &mut TranslationContext, fetch: TypeQLFetch) -> Result<FetchObject, FetchRepresentationError> {
    let object = fetch.object;
    translate_fetch_object(context, object)
}

// This function returns a specific `FetchObject`, rather than the FetchSome` higher-level enum
// This gives us a simpler entry point for the fetch clause translation
fn translate_fetch_object(context: &mut TranslationContext, object: TypeQLFetchObject) -> Result<FetchObject, FetchRepresentationError> {
    match object.body {
        TypeQLFetchObjectBody::Entries(entries) => {
            let mut object = HashMap::new();
            for entry in entries {
                let (key, value) = (entry.key, entry.value);
                let key_id = register_key(context, key);
                object.insert(key_id, translate_fetch_some(context, value)?);
            }
            Ok(FetchObject::Static(FetchObjectStatic { object }))
        }
        TypeQLFetchObjectBody::AttributesAll(variable) => {
            let var = try_get_variable(context, variable)?;
            Ok(FetchObject::Attributes(FetchObjectAttributes { variable: var }))
        }
    }
}

fn translate_fetch_some(context: &mut TranslationContext, fetch_some: TypeQLFetchSome) -> Result<FetchSome, FetchRepresentationError> {
    match fetch_some {
        TypeQLFetchSome::Object(object) => {
            translate_fetch_object(context, object).map(|object| FetchSome::Object(Box::new(object)))
        }
        TypeQLFetchSome::List(list) => {
            translate_fetch_list(context, list)
        }
        TypeQLFetchSome::Single(some) => {
            translate_fetch_single(context, some)
        }
    }
}

fn translate_fetch_list(context: &mut TranslationContext, list: TypeQLFetchList) -> Result<FetchSome, FetchRepresentationError> {
    todo!()
}

// Note: TypeQL fetch-single can turn either into a List or a Single IR
fn translate_fetch_single(context: &mut TranslationContext, single: TypeQLFetchSingle) -> Result<FetchSome, FetchRepresentationError> {
    match single {
        FetchSingle::Attribute(fetch_attribute) => {
            let owner = try_get_variable(context, fetch_attribute.owner)?;
            match fetch_attribute.attribute {
                TypeRefAny::Type(type_ref) => {
                    match type_ref {
                        TypeRef::Named(type_) => {
                            match type_ {
                                NamedType::Label(label) => {
                                    Ok(FetchSome::SingleAttribute(FetchSingleAttribute { variable: owner, attribute: label.ident.as_str().to_owned()}))
                                }
                                NamedType::Role(_) | NamedType::BuiltinValueType(_) => {
                                    Err(InvalidAttributeLabelEncountered { declaration: type_ref.clone() })
                                }
                            }
                        }
                        TypeRef::Variable(_) => Err(NamedVariableEncountered { declaration: type_ref.clone() })
                    }
                }
                TypeRefAny::List(list) => {
                    match list.inner {
                        TypeRef::Named(named_type) => {
                            match named_type {
                                NamedType::Label(label) => {
                                    Ok(FetchSome::ListAttributesFromList(FetchListAttributeFromList { variable: owner, attribute: label.ident.as_str().to_owned() }))
                                }
                                NamedType::Role(_) | NamedType::BuiltinValueType(_) => {
                                    Err(InvalidAttributeLabelEncountered { declaration: list.inner.clone() })
                                }
                            }
                        }
                        TypeRef::Variable(_) => Err(NamedVariableEncountered { declaration: list.inner.clone() }),
                    }
                }
                TypeRefAny::Optional(_) => Err(OptionalVariableEncountered { declaration: fetch_attribute.attribute })
            }
        }
        FetchSingle::Expression(expression) => {
            match expression {
                Expression::Variable(variable) => {
                    let var = try_get_variable(context, variable)?;
                    Ok(FetchSome::SingleVar(FetchSingleVar { variable: var }))
                }
                Expression::ListIndex(_) | Expression::Value(_) | Expression::Operation(_) | Expression::Paren(_) => {
                    // because expressions expect to be able to extract out function calls, we'll translate an expression
                    // into a match-return

                    // we don't want anything here to affect the parent contexts, so clone
                    let builder_context = BlockBuilderContext::new(
                        &mut context.variable_registry.clone(),
                        &mut context.visible_variables.clone(),
                        &mut context.parameters.clone(),
                    );
                    let builder = Block::builder(builder_context);
                    let constraints_builder = builder.conjunction_mut();
                    let expression = build_expression()
                }
                // function expressions may return Single or List, depending on signature invoked
                Expression::Function(_) => {
                    todo!()
                }
                // list expressions should be mapped to FetchList
                Expression::List(_) => {
                    todo!()
                }
                Expression::ListIndexRange(_) => {
                    todo!()
                }
            }
        }
        FetchSingle::FunctionBlock(_) => {}
    }
}

fn try_get_variable(context: &TranslationContext, variable: TypeQLVariable) -> Result<Variable, FetchRepresentationError> {
    let name = match variable {
        TypeQLVariable::Anonymous { .. } => return Err(AnonymousVariableEncountered { declaration: variable }),
        TypeQLVariable::Named { .. } => variable.name().unwrap(),
    };
    context.get_variable(name)
        .ok_or_else(|| VariableNotAvailable { variable: name.to_owned(), declaration: variable.clone() })
}

fn register_key(context: &mut TranslationContext, key: StringLiteral) -> ParameterID {
    context.parameters.register_fetch_key(key.value)
}

typedb_error!(
    pub FetchRepresentationError(component = "Fetch representation", prefix = "FER") {
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
    }
);
