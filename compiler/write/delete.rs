use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::pattern::constraint::Constraint;

use crate::{
    inference::type_annotations::TypeAnnotations,
    write::{
        determine_unique_kind, get_thing_source,
        insert::{collect_role_type_bindings, WriteCompilationError},
        write_instructions::{
            DeleteAttribute, DeleteEntity, DeleteRelation, Has, PutAttribute, PutEntity, PutRelation, RolePlayer,
        },
        ThingSource, TypeSource, VariableSource,
    },
};

#[derive(Debug)]
pub enum DeleteInstruction {
    // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
    Entity(DeleteEntity),
    Attribute(DeleteAttribute),
    Relation(DeleteRelation),
    Has(Has),               // TODO: Ordering
    RolePlayer(RolePlayer), // TODO: Ordering
}

pub struct DeletePlan {
    pub instructions: Vec<DeleteInstruction>,
    pub output_row_plan: Vec<VariableSource>,
    // pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn build_delete_plan(
    input_variables: &HashMap<Variable, usize>,
    type_annotations: &TypeAnnotations,
    constraints: &[Constraint<Variable>],
    deleted_concepts: &[Variable],
) -> Result<DeletePlan, WriteCompilationError> {
    // TODO: Maybe unify all WriteCompilation errors?
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    let mut instructions = Vec::new();
    let inserted_things = HashMap::new();
    deleted_concepts.iter().try_for_each(|variable| {
        let thing = get_thing_source(input_variables, &inserted_things, variable.clone())?;
        let annotations = type_annotations.variable_annotations_of(*variable).unwrap();
        let kind = determine_unique_kind(annotations)
            .map_err(|_| WriteCompilationError::DeleteHasMultipleKinds { variable: variable.clone() })?;
        match kind {
            Kind::Entity => instructions.push(DeleteInstruction::Entity(DeleteEntity { entity: thing })),
            Kind::Attribute => instructions.push(DeleteInstruction::Attribute(DeleteAttribute { attribute: thing })),
            Kind::Relation => instructions.push(DeleteInstruction::Relation(DeleteRelation { relation: thing })),
            Kind::Role => Err(WriteCompilationError::IllegalRoleDelete { variable: variable.clone() })?,
        }
        Ok(())
    })?;
    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                instructions.push(DeleteInstruction::Has(Has {
                    owner: get_thing_source(input_variables, &inserted_things, has.owner())?,
                    attribute: get_thing_source(input_variables, &inserted_things, has.attribute())?,
                }));
            }
            Constraint::RolePlayer(role_player) => {
                let relation = get_thing_source(input_variables, &inserted_things, role_player.relation())?;
                let player = get_thing_source(input_variables, &inserted_things, role_player.player())?;
                let role_variable = role_player.role_type();
                let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
                    (Some(input), None) => TypeSource::InputVariable(*input as u32),
                    (None, Some(type_)) => TypeSource::TypeConstant(type_.clone()),
                    (None, None) => {
                        // TODO: Do we want to support inserts with unspecified role-types?
                        let annotations = type_annotations.variable_annotations_of(role_variable).unwrap();
                        if annotations.len() == 1 {
                            TypeSource::TypeConstant(annotations.iter().find(|_| true).unwrap().clone())
                        } else {
                            return Err(WriteCompilationError::CouldNotUniquelyDetermineRoleType {
                                variable: role_variable.clone(),
                            })?;
                        }
                    }
                    (Some(_), Some(_)) => unreachable!(),
                };
                instructions.push(DeleteInstruction::RolePlayer(RolePlayer { relation, player, role }));
            }
            Constraint::Isa(_)
            | Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::FunctionCallBinding(_) => {
                unreachable!()
            }
        }
    }
    for variable in deleted_concepts {
        let source = ThingSource::InputVariable(*input_variables.get(variable).unwrap() as u32);
        let annotations = type_annotations.variable_annotations_of(variable.clone()).unwrap();
        let kind = determine_unique_kind(annotations)
            .map_err(|_| WriteCompilationError::DeleteHasMultipleKinds { variable: variable.clone() })?;
        let instruction = match kind {
            Kind::Entity => DeleteInstruction::Entity(DeleteEntity { entity: source }),
            Kind::Attribute => DeleteInstruction::Attribute(DeleteAttribute { attribute: source }),
            Kind::Relation => DeleteInstruction::Relation(DeleteRelation { relation: source }),
            Kind::Role => return Err(WriteCompilationError::IllegalRoleDelete { variable: variable.clone() }),
        };
        instructions.push(instruction);
    }
    // To produce the output stream, we remove the deleted concepts from each map in the stream.
    let output_row = input_variables
        .iter()
        .filter_map(|(variable, position)| {
            if deleted_concepts.contains(variable) {
                None
            } else {
                Some(VariableSource::InputVariable(*position as u32))
            }
        })
        .collect::<Vec<_>>();

    Ok(DeletePlan { instructions, output_row_plan: output_row })
}
