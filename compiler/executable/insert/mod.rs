/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
};

use answer::{variable::Variable, Type};
use encoding::graph::type_::Kind;
use error::typedb_error;
use ir::{
    pattern::{constraint::Comparator, ParameterID},
    pipeline::VariableRegistry,
};

use crate::VariablePosition;

pub mod executable;
pub mod instructions;
pub mod type_check;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum VariableSource {
    InputVariable(VariablePosition), // TODO: This needs to be renamed
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum TypeSource {
    InputVariable(VariablePosition),
    Constant(answer::Type),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ValueSource {
    Variable(VariablePosition),
    Parameter(ParameterID),
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct ThingPosition(pub VariablePosition);

pub(crate) fn get_thing_input_position(
    input_variables: &HashMap<Variable, VariablePosition>,
    variable: Variable,
    variable_registry: &VariableRegistry,
) -> Result<ThingPosition, Box<WriteCompilationError>> {
    match input_variables.get(&variable) {
        Some(input) => Ok(ThingPosition(*input)),
        None => Err(Box::new(WriteCompilationError::MissingExpectedInput {
            variable: variable_registry
                .variable_names()
                .get(&variable)
                .cloned()
                .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
        })),
    }
}

pub(crate) fn get_kinds_from_types(types: &BTreeSet<Type>) -> HashSet<Kind> {
    types.iter().map(Type::kind).collect()
}

typedb_error! {
    pub WriteCompilationError(component = "Write compile", prefix = "WCP") {
        InsertIsaStatementForInputVariable(
            1,
            "Illegal 'isa' provided for variable '{variable}' that is input from a previous stage - 'isa's should only be used to create new instances in insert clauses.",
            variable: String
        ),
        InsertVariableAmbiguousAttributeOrObject(
            2,
            "Insert variable '{variable}' is ambiguously an attribute or an object (entity/relation).",
            variable: String,
        ),
        InsertVariableUnknownType(
            3,
            "Could not determine the type of the insert variable '{variable}'.",
            variable: String
        ),
        InsertAttributeMissingValue(
            4,
            "Could not determine the value of the insert attribute '{variable}'.",
            variable: String
        ),
        InsertIllegalPredicate(
            5,
            "Illegal predicate in insert for variable '{variable}' with comparator '{comparator}'.",
            variable: String,
            comparator: Comparator
        ),
        MissingExpectedInput(
            6,
            "Missing expected input variable in compilation data '{variable}'.",
            variable: String
        ),
        AmbiguousRoleType(
            7,
            "Could not uniquely resolve the role type for variable '{variable}'. Possible role types are: {role_types}.",
            variable: String,
            role_types: String,
        ),
        InsertLinksAmbiguousRoleType(
            8,
            "Links insert for player '{player_variable}' requires unambiguous role type, but inferred: {role_types}.",
            player_variable: String,
            role_types: String,
        ),
        DeleteIllegalRoleVariable(
            9,
            "Illegal delete for variable '{variable}', which represents role types.",
            variable: String
        ),
        InsertIllegalRole(
            10,
            "Illegal role type insert for variable '{variable}'.",
            variable: String,
        ),
        DeletedThingWasNotInInput(
            11,
            "Deleted variable '{variable}' is not available as input from previous stages.",
            variable: String
        ),
    }
}
