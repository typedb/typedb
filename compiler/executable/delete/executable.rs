/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::{
    pattern::{constraint::Constraint, Vertex},
    pipeline::VariableRegistry,
};
use itertools::Itertools;
use typeql::common::Span;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::{
        delete::instructions::{ConnectionInstruction, Has, Links, ThingInstruction},
        insert::{
            executable::{collect_role_type_bindings, get_thing_input_position},
            ThingPosition, TypeSource,
        },
        next_executable_id, WriteCompilationError,
    },
    VariablePosition,
};

#[derive(Debug)]
pub struct DeleteExecutable {
    pub executable_id: u64,
    pub concept_instructions: Vec<ThingInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<Variable>>,
    // pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn compile(
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: &VariableRegistry,
    constraints: &[Constraint<Variable>],
    deleted_concepts: &[Variable],
    source_span: Option<Span>,
) -> Result<DeleteExecutable, Box<WriteCompilationError>> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations, variable_registry)?;
    let mut connection_deletes = Vec::new();
    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                connection_deletes.push(ConnectionInstruction::Has(Has {
                    owner: get_thing_input_position(
                        input_variables,
                        has.owner().as_variable().unwrap(),
                        variable_registry,
                        has.source_span(),
                    )?,
                    attribute: get_thing_input_position(
                        input_variables,
                        has.attribute().as_variable().unwrap(),
                        variable_registry,
                        has.source_span(),
                    )?,
                }));
            }
            Constraint::Links(links) => {
                let relation = get_thing_input_position(
                    input_variables,
                    links.relation().as_variable().unwrap(),
                    variable_registry,
                    links.source_span(),
                )?;
                let player = get_thing_input_position(
                    input_variables,
                    links.player().as_variable().unwrap(),
                    variable_registry,
                    links.source_span(),
                )?;
                let role_type = links.role_type();
                let role = match role_type {
                    &Vertex::Variable(input) => {
                        if let Some(input) = input_variables.get(&input) {
                            TypeSource::InputVariable(*input)
                        } else if let Some(type_) = named_role_types.get(&input) {
                            TypeSource::Constant(*type_)
                        } else {
                            let annotations = type_annotations.vertex_annotations_of(role_type).unwrap();
                            if annotations.len() == 1 {
                                TypeSource::Constant(*annotations.iter().next().unwrap())
                            } else {
                                return Err(WriteCompilationError::InsertLinksAmbiguousRoleType {
                                    player_variable: variable_registry
                                        .variable_names()
                                        .get(&links.player().as_variable().unwrap())
                                        .cloned()
                                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                                    // TODO: It prints `[RoleType:[0000]], [RoleType:[0001]]`, get labels somehow
                                    role_types: annotations.iter().join(", "),
                                    source_span: links.source_span(),
                                })?;
                            }
                        }
                    }
                    Vertex::Label(_) => unreachable!("expected role name, found label in a `links` constraint"),
                    Vertex::Parameter(_) => unreachable!(),
                };
                connection_deletes.push(ConnectionInstruction::Links(Links { relation, player, role }));
            }
            Constraint::LinksDeduplication(_) | Constraint::RoleName(_) => (), // Ignore. It will have done its job during type-inference
            Constraint::Iid(_)
            | Constraint::Isa(_)
            | Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Is(_)
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::Value(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::IndexedRelation(_)
            | Constraint::OptimisedToUnsatisfiable(_) => {
                unreachable!()
            }
        }
    }

    let mut concept_deletes = Vec::new();
    for &variable in deleted_concepts {
        let Some(input_position) = input_variables.get(&variable) else {
            return Err(Box::new(WriteCompilationError::DeletedThingWasNotInInput {
                variable: variable_registry
                    .variable_names()
                    .get(&variable)
                    .cloned()
                    .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                source_span,
            }));
        };
        if type_annotations
            .vertex_annotations_of(&Vertex::Variable(variable))
            .unwrap()
            .iter()
            .any(|type_| type_.kind() == Kind::Role)
        {
            return Err(Box::new(WriteCompilationError::DeleteIllegalRoleVariable {
                variable: variable_registry
                    .variable_names()
                    .get(&variable)
                    .cloned()
                    .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                source_span,
            }));
        } else {
            concept_deletes.push(ThingInstruction { thing: ThingPosition(*input_position) });
        };
    }

    // To produce the output stream, we remove the deleted concepts from each map in the stream.
    let mut output_row_schema = Vec::new();
    for (&variable, position) in input_variables {
        if deleted_concepts.contains(&variable) {
            continue;
        }
        let pos_as_usize = position.as_usize();
        if output_row_schema.len() <= pos_as_usize {
            output_row_schema.resize(pos_as_usize + 1, None);
        }
        output_row_schema[pos_as_usize] = Some(variable);
    }

    Ok(DeleteExecutable {
        executable_id: next_executable_id(),
        connection_instructions: connection_deletes,
        concept_instructions: concept_deletes,
        output_row_schema,
    })
}
