/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::pattern::{constraint::Constraint, Vertex};

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::{
        delete::instructions::{ConnectionInstruction, Has, RolePlayer, ThingInstruction},
        insert::{
            executable::collect_role_type_bindings, get_thing_source, ThingSource, TypeSource, WriteCompilationError,
        },
    },
    VariablePosition,
};

pub struct DeleteExecutable {
    pub concept_instructions: Vec<ThingInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<Variable>>,
    // pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn compile(
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    constraints: &[Constraint<Variable>],
    deleted_concepts: &[Variable],
) -> Result<DeleteExecutable, WriteCompilationError> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    let mut connection_deletes = Vec::new();
    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                connection_deletes.push(ConnectionInstruction::Has(Has {
                    owner: get_thing_source(input_variables, has.owner().as_variable().unwrap())?,
                    attribute: get_thing_source(input_variables, has.attribute().as_variable().unwrap())?,
                }));
            }
            Constraint::Links(role_player) => {
                let relation = get_thing_source(input_variables, role_player.relation().as_variable().unwrap())?;
                let player = get_thing_source(input_variables, role_player.player().as_variable().unwrap())?;
                let role_type = role_player.role_type();
                let role = match role_type {
                    &Vertex::Variable(input) => {
                        if let Some(input) = input_variables.get(&input) {
                            TypeSource::InputVariable(*input)
                        } else if let Some(type_) = named_role_types.get(&input) {
                            TypeSource::Constant(type_.clone())
                        } else {
                            let annotations = type_annotations.vertex_annotations_of(role_type).unwrap();
                            if annotations.len() == 1 {
                                TypeSource::Constant(annotations.iter().next().unwrap().clone())
                            } else {
                                return Err(WriteCompilationError::CouldNotUniquelyDetermineRoleType {
                                    variable: input,
                                })?;
                            }
                        }
                    }
                    Vertex::Label(_) => unreachable!("expected role name, found label in a `links` constraint"),
                    Vertex::Parameter(_) => unreachable!(),
                };
                connection_deletes.push(ConnectionInstruction::RolePlayer(RolePlayer { relation, player, role }));
            }
            | Constraint::Isa(_)
            | Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Is(_)
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::As(_)
            | Constraint::Value(_)
            | Constraint::FunctionCallBinding(_) => {
                unreachable!()
            }
        }
    }

    let mut concept_deletes = Vec::new();
    for &variable in deleted_concepts {
        let Some(input_position) = input_variables.get(&variable) else {
            return Err(WriteCompilationError::DeletedThingWasNotInInput { variable });
        };
        if type_annotations
            .vertex_annotations_of(&Vertex::Variable(variable))
            .unwrap()
            .iter()
            .any(|type_| type_.kind() == Kind::Role)
        {
            return Err(WriteCompilationError::IllegalRoleDelete { variable });
        } else {
            concept_deletes.push(ThingInstruction { thing: ThingSource(*input_position) });
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
        connection_instructions: connection_deletes,
        concept_instructions: concept_deletes,
        output_row_schema,
    })
}
