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
    pattern::{constraint, constraint::Comparator, ParameterID, Vertex},
    pipeline::VariableRegistry,
};
use itertools::Itertools;
use typeql::common::Span;

use crate::{annotation::type_annotations::TypeAnnotations, executable::WriteCompilationError, VariablePosition};

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

pub(crate) fn resolve_links_role(
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    named_role_types: &HashMap<Variable, Type>,
    links: &constraint::Links<Variable>,
) -> Result<TypeSource, Box<WriteCompilationError>> {
    let &Vertex::Variable(role_variable) = links.role_type() else { unreachable!() };
    match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
        (Some(&input), None) => Ok(TypeSource::InputVariable(input)),
        (None, Some(type_)) => Ok(TypeSource::Constant(*type_)),
        (None, None) => {
            // TODO: Do we want to support inserts with unspecified role-types?
            let annotations = type_annotations.vertex_annotations_of(&Vertex::Variable(role_variable)).unwrap();
            if annotations.len() == 1 {
                Ok(TypeSource::Constant(*annotations.iter().find(|_| true).unwrap()))
            } else {
                return Err(Box::new(WriteCompilationError::InsertLinksAmbiguousRoleType {
                    player_variable: variable_registry
                        .variable_names()
                        .get(&links.relation().as_variable().unwrap())
                        .cloned()
                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                    role_types: annotations.iter().join(", "),
                    source_span: links.source_span(),
                }));
            }
        }
        (Some(_), Some(_)) => unreachable!(),
    }
}

pub(crate) fn prepare_output_row_schema(
    input_variables: &HashMap<Variable, VariablePosition>,
) -> Vec<Option<(Variable, VariableSource)>> {
    let output_width = input_variables.values().map(|i| i.position + 1).max().unwrap_or(0);
    let mut output_row_schema = vec![None; output_width as usize];
    input_variables.iter().map(|(v, i)| (i, v)).for_each(|(&i, &v)| {
        output_row_schema[i.position as usize] = Some((v, VariableSource::InputVariable(i)));
    });
    output_row_schema
}
