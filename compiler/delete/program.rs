/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::pattern::constraint::Constraint;

use crate::{
    delete::instructions::{ConnectionInstruction, Has, RolePlayer, ThingInstruction},
    insert::{get_thing_source, program::collect_role_type_bindings, ThingSource, TypeSource, WriteCompilationError},
    match_::inference::type_annotations::TypeAnnotations,
    VariablePosition,
};

pub struct DeleteProgram {
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
) -> Result<DeleteProgram, WriteCompilationError> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    let mut connection_deletes = Vec::new();
    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                connection_deletes.push(ConnectionInstruction::Has(Has {
                    owner: get_thing_source(input_variables, has.owner())?,
                    attribute: get_thing_source(input_variables, has.attribute())?,
                }));
            }
            Constraint::Links(role_player) => {
                let relation = get_thing_source(input_variables, role_player.relation())?;
                let player = get_thing_source(input_variables, role_player.player())?;
                let role_variable = role_player.role_type();
                let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
                    (Some(input), None) => TypeSource::InputVariable(*input),
                    (None, Some(type_)) => TypeSource::Constant(type_.clone()),
                    (None, None) => {
                        let annotations = type_annotations.variable_annotations_of(role_variable).unwrap();
                        if annotations.len() == 1 {
                            TypeSource::Constant(annotations.iter().find(|_| true).unwrap().clone())
                        } else {
                            return Err(WriteCompilationError::CouldNotUniquelyDetermineRoleType {
                                variable: role_variable,
                            })?;
                        }
                    }
                    (Some(_), Some(_)) => unreachable!(),
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
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::FunctionCallBinding(_) => {
                unreachable!()
            }
        }
    }

    let mut concept_deletes = Vec::new();
    for variable in deleted_concepts {
        let Some(input_position) = input_variables.get(variable) else {
            return Err(WriteCompilationError::DeletedThingWasNotInInput { variable: *variable });
        };
        if type_annotations.variable_annotations_of(*variable).unwrap().iter().any(|type_| type_.kind() == Kind::Role) {
            Err(WriteCompilationError::IllegalRoleDelete { variable: *variable })?;
        } else {
            concept_deletes.push(ThingInstruction { thing: ThingSource(*input_position) });
        };
    }

    // To produce the output stream, we remove the deleted concepts from each map in the stream.
    let output_row_schema = input_variables
        .iter()
        .map(|(variable, position)| if deleted_concepts.contains(variable) { None } else { Some(*variable) })
        .collect::<Vec<_>>();

    Ok(DeleteProgram {
        connection_instructions: connection_deletes,
        concept_instructions: concept_deletes,
        output_row_schema,
    })
}
