/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap, HashSet};

use answer::{variable::Variable, Type};
use encoding::{graph::type_::Kind, value::value::Value};
use itertools::Itertools;

use crate::write::insert::WriteCompilationError;

pub mod delete;
pub mod insert;
pub mod write_instructions;

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

fn get_thing_source(
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

pub(crate) fn determine_unique_kind(annotations: &HashSet<Type>) -> Result<Kind, ()> {
    // TODO: Maybe we don't care and want a run-time switch?
    let kinds = annotations.iter().map(|annotation| annotation.kind().clone()).dedup().collect::<Vec<_>>();
    match kinds.len() {
        1 => Ok(kinds[0]),
        _ => Err(()),
    }
}
