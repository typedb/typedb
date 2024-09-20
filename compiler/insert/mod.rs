/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
};

use answer::{variable::Variable, Type};
use encoding::graph::type_::Kind;
use ir::pattern::{constraint::Isa, ParameterID};

use crate::VariablePosition;

pub mod instructions;
pub mod program;
pub mod type_check;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum VariableSource {
    InputVariable(VariablePosition),
    InsertedThing(usize),
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
) -> Result<ThingSource, WriteCompilationError> {
    match input_variables.get(&variable) {
        Some(input) => Ok(ThingSource(*input)),
        None => Err(WriteCompilationError::CouldNotDetermineThingVariableSource { variable }),
    }
}

pub(crate) fn get_kinds_from_annotations(annotations: &BTreeSet<Type>) -> HashSet<Kind> {
    annotations.iter().map(Type::kind).collect()
}

#[derive(Debug, Clone)]
pub enum WriteCompilationError {
    IsaStatementForInputVariable { variable: Variable },
    IsaStatementForRoleType { isa: Isa<Variable> },
    IsaTypeMayBeAttributeOrObject { isa: Isa<Variable> },
    CouldNotDetermineTypeOfInsertedVariable { variable: Variable },
    CouldNotDetermineValueOfInsertedAttribute { variable: Variable },
    CouldNotDetermineThingVariableSource { variable: Variable },
    CouldNotUniquelyResolveRoleTypeFromName { variable: Variable },
    CouldNotUniquelyDetermineRoleType { variable: Variable },

    IllegalRoleDelete { variable: Variable },
    DeleteHasMultipleKinds { isa: Isa<Variable> },
    IllegalInsertForRole { isa: Isa<Variable> },
    DeletedThingWasNotInInput { variable: Variable },
}

impl Display for WriteCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteCompilationError {}
