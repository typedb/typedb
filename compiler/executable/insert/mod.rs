/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fmt,
};

use answer::{variable::Variable, Type};
use encoding::graph::type_::Kind;
use error::typedb_error;
use ir::pattern::{
    constraint::{Comparator, Isa},
    ParameterID,
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
pub struct ThingSource(pub VariablePosition);

pub(crate) fn get_thing_source(
    input_variables: &HashMap<Variable, VariablePosition>,
    variable: Variable,
) -> Result<ThingSource, Box<WriteCompilationError>> {
    match input_variables.get(&variable) {
        Some(input) => Ok(ThingSource(*input)),
        None => Err(Box::new(WriteCompilationError::CouldNotDetermineThingVariableSource { variable })),
    }
}

pub(crate) fn get_kinds_from_annotations(annotations: &BTreeSet<Type>) -> HashSet<Kind> {
    annotations.iter().map(Type::kind).collect()
}

typedb_error! {
    pub WriteCompilationError(component = "Write Compilation", prefix = "WRC") {
        // TODO: Update error message to something more understandable
        IsaStatementForInputVariable(1, "'isa' statement provided for input variable '{variable}'.", variable: Variable),
        // TODO: Update error message to something more understandable
        IsaTypeMayBeAttributeOrObject(2, "'isa' type '{isa}' may be an attribute or object.", isa: Isa<Variable>),
        CouldNotDetermineTypeOfInsertedVariable(3, "Could not determine the type of inserted variable '{variable}'.", variable: Variable),
        CouldNotDetermineValueOfInsertedAttribute(4, "Could not determine the value of inserted attribute '{variable}'.", variable: Variable),
        IllegalPredicateInAttributeInsert(5, "Illegal predicate in attribute insert for variable '{variable}' with comparator '{comparator}'.", variable: Variable, comparator: Comparator),
        CouldNotDetermineThingVariableSource(6, "Could not determine the source of thing variable '{variable}'.", variable: Variable),
        CouldNotUniquelyResolveRoleTypeFromName(7, "Could not uniquely resolve role type from name for variable '{variable}'.", variable: Variable),
        CouldNotUniquelyDetermineRoleType(8, "Could not uniquely determine role type for variable '{variable}'.", variable: Variable),
        IllegalRoleDelete(9, "Illegal role delete for variable '{variable}'.", variable: Variable),
        IllegalInsertForRole(10, "Illegal insert for role in 'isa' statement '{isa}'.", isa: Isa<Variable>),
        DeletedInstanceWasNotInInput(11, "Deleted instance variable '{variable}' was not in input.", variable: Variable),
    }
}
