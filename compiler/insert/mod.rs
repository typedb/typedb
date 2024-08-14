/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
};

use answer::{variable::Variable, Type};
use encoding::{graph::type_::Kind, value::value::Value};
use ir::pattern::constraint::Isa;
use itertools::Itertools;

pub mod insert;
pub mod instructions;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum VariableSource {
    InputVariable(VariablePosition),
    InsertedThing(usize),
}

type VariablePosition = u32;
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum TypeSource {
    InputVariable(VariablePosition),
    TypeConstant(answer::Type),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ValueSource {
    InputVariable(VariablePosition),
    ValueConstant(Value<'static>),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ThingSource {
    InputVariable(VariablePosition),
    InsertedThing(usize),
}

pub(crate) fn get_thing_source(
    input_variables: &HashMap<Variable, usize>,
    inserted_concepts: &HashMap<Variable, usize>,
    variable: Variable,
) -> Result<ThingSource, WriteCompilationError> {
    match (input_variables.get(&variable), inserted_concepts.get(&variable)) {
        (Some(input), None) => Ok(ThingSource::InputVariable(*input as u32)),
        (None, Some(inserted)) => Ok(ThingSource::InsertedThing(*inserted)),
        (Some(_), Some(_)) => Err(WriteCompilationError::VariableIsBothInsertedAndInput { variable }), // TODO: I think this is unreachable
        (None, None) => Err(WriteCompilationError::CouldNotDetermineThingVariableSource { variable }),
    }
}

pub(crate) fn get_kinds_from_annotations(annotations: &HashSet<Type>) -> Vec<Kind> {
    annotations.iter().map(|annotation| annotation.kind().clone()).dedup().collect::<Vec<_>>()
}

#[derive(Debug, Clone)]
pub enum WriteCompilationError {
    VariableIsBothInsertedAndInput { variable: Variable },
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
}

impl Display for WriteCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteCompilationError {}
