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
use typeql::common::Span;
use crate::executable::WriteCompilationError;

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
    source_span: Option<Span>,
) -> Result<ThingPosition, Box<WriteCompilationError>> {
    match input_variables.get(&variable) {
        Some(input) => Ok(ThingPosition(*input)),
        None => Err(Box::new(WriteCompilationError::MissingExpectedInput {
            variable: variable_registry
                .variable_names()
                .get(&variable)
                .cloned()
                .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
            source_span,
        })),
    }
}

pub(crate) fn get_kinds_from_types(types: &BTreeSet<Type>) -> HashSet<Kind> {
    types.iter().map(Type::kind).collect()
}
