/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::Type;
use encoding::{graph::definition::definition_key::DefinitionKey, value::value_type::ValueType};
use ir::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::function::FunctionIR,
};

/// Function represents the user-defined structure:
/// fun <name>(<args>) -> <return type> { <body> }
pub struct Function {
    definition_key: DefinitionKey<'static>,

    // parsed representation
    name: String,
    arguments: Vec<FunctionArgument>,
    return_type: FunctionReturn,

    // pre-compiled arguments, body, return // TODO maybe deferred compilation
    ir_body: FunctionIR,
}

struct FunctionArgument {
    name: String,
    type_: FunctionValuePrototype,
}

enum FunctionReturn {
    Stream(Vec<FunctionValuePrototype>),
    Single(FunctionValuePrototype),
}

impl Function {
    // TODO: receive a string, which can either come from the User or from Storage (deserialised)
    //       will require type manager to convert labels into Types and Definitions etc.
    fn new(definition: &str) -> Self {
        // 1. parse into TypeQL
        // 2. extract into data structures
        // possible later: // TODO: what if recursive - the function call will need information from this function! -> defer?
        // 3. create IR & apply inference
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub enum FunctionValuePrototype {
    Thing(Type),
    ThingOptional(Type),
    Value(ValueType),
    ValueOptional(ValueType),
    ThingList(Type),
    ThingListOptional(Type),
    ValueList(ValueType),
    ValueListOptional(ValueType),
}

impl Into<VariableCategory> for FunctionValuePrototype {
    fn into(self) -> VariableCategory {
        match self {
            FunctionValuePrototype::Thing(type_) | FunctionValuePrototype::ThingOptional(type_) => match type_ {
                Type::Entity(_) | Type::Relation(_) => VariableCategory::Object,
                Type::Attribute(_) => VariableCategory::Attribute,
                Type::RoleType(_) => unreachable!("A function cannot use role typed instances"),
            },
            FunctionValuePrototype::Value(_) | FunctionValuePrototype::ValueOptional(_) => VariableCategory::Value,
            FunctionValuePrototype::ThingList(type_) | FunctionValuePrototype::ThingListOptional(type_) => {
                match type_ {
                    Type::Entity(_) | Type::Relation(_) => VariableCategory::ObjectList,
                    Type::Attribute(_) => VariableCategory::AttributeList,
                    Type::RoleType(_) => unreachable!("A function cannot use role-list typed instances"),
                }
            }
            FunctionValuePrototype::ValueList(_) | FunctionValuePrototype::ValueListOptional(_) => {
                VariableCategory::ValueList
            }
        }
    }
}

impl Into<VariableOptionality> for FunctionValuePrototype {
    fn into(self) -> VariableOptionality {
        match self {
            FunctionValuePrototype::Thing(_)
            | FunctionValuePrototype::Value(_)
            | FunctionValuePrototype::ThingList(_)
            | FunctionValuePrototype::ValueList(_) => VariableOptionality::Required,
            FunctionValuePrototype::ThingOptional(_)
            | FunctionValuePrototype::ValueOptional(_)
            | FunctionValuePrototype::ThingListOptional(_)
            | FunctionValuePrototype::ValueListOptional(_) => VariableOptionality::Optional,
        }
    }
}
