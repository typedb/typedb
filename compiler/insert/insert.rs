/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// jk there's no planning to be done here, just execution.
// There is a need to construct the executor though.

use std::{collections::HashMap, fmt::Display};

use answer::{variable::Variable, Type};
use encoding::{graph::type_::Kind, value::value::Value};
use ir::pattern::{
    constraint::{Constraint, Isa},
    expression::Expression,
};
use itertools::Itertools;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    filter_variants,
    insert::{
        get_kinds_from_annotations, get_thing_source,
        instructions::{Has, InsertEdgeInstruction, InsertVertexInstruction, PutAttribute, PutObject, RolePlayer},
        ThingSource, TypeSource, ValueSource, VariableSource, WriteCompilationError,
    },
    match_::inference::type_annotations::TypeAnnotations,
    VariablePosition,
};

pub struct InsertPlan {
    pub vertex_instructions: Vec<InsertVertexInstruction>,
    pub edge_instructions: Vec<InsertEdgeInstruction>,
    pub output_row_plan: Vec<(Variable, VariableSource)>, // Where to copy from
    pub debug_info: HashMap<VariableSource, Variable>,
}

/*
* Assumptions:
*   - Any labels have been assigned to a type variable and added as a type-annotation, though we should have a more explicit mechanism for this.
*   - Validation has already been done - An input row will not violate schema on insertion.
*/

pub fn build_insert_plan(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
) -> Result<InsertPlan, WriteCompilationError> {
    let mut vertex_instructions = Vec::with_capacity(constraints.len());
    let all_variables =
        add_inserted_concepts(constraints, input_variables, type_annotations, &mut vertex_instructions)?;

    let mut edge_instructions = Vec::with_capacity(constraints.len());
    add_has(constraints, &all_variables, &mut edge_instructions)?;
    add_role_players(constraints, type_annotations, &all_variables, &mut edge_instructions)?;

    let mut output_row_plan = Vec::with_capacity(all_variables.len()); // TODO
    all_variables.iter().map(|(v, i)| (i, v)).sorted().for_each(|(i, v)| {
        debug_assert!(i.position as usize == output_row_plan.len());
        output_row_plan.push((v.clone(), VariableSource::InputVariable(i.clone())));
    });

    let debug_info = HashMap::new(); // TODO
    Ok(InsertPlan { vertex_instructions, edge_instructions, output_row_plan, debug_info })
}

fn add_inserted_concepts(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    vertex_instructions: &mut Vec<InsertVertexInstruction>,
) -> Result<HashMap<Variable, VariablePosition>, WriteCompilationError> {
    let mut output_variables = input_variables.clone();
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;
    filter_variants!(Constraint::Isa : constraints).try_for_each(|isa| {
        if input_variables.contains_key(&isa.thing()) {
            Err(WriteCompilationError::IsaStatementForInputVariable { variable: isa.thing() })?
        }

        let type_ = match (input_variables.get(&isa.type_()), type_bindings.get(&isa.type_())) {
            (Some(input), None) => TypeSource::InputVariable(input.clone()),
            (None, Some(type_)) => TypeSource::TypeConstant(type_.clone()),
            (Some(_), Some(_)) => unreachable!("Explicit label constraints are banned in insert"),
            (None, None) => {
                Err(WriteCompilationError::CouldNotDetermineTypeOfInsertedVariable { variable: isa.thing() })?
            }
        };
        // Requires variable annotations for this stage to be available.
        let annotations = type_annotations.variable_annotations_of(isa.type_()).unwrap();
        debug_assert!(!annotations.is_empty());
        let kinds = get_kinds_from_annotations(annotations);
        let is_object = {
            let is_role = kinds.contains(&Kind::Role);
            let is_object = kinds.contains(&Kind::Relation) || kinds.contains(&Kind::Entity);
            let is_attribute = kinds.contains(&Kind::Attribute);
            if is_role {
                Err(WriteCompilationError::IllegalInsertForRole { isa: isa.clone() })?;
            } else if is_attribute && is_object {
                Err(WriteCompilationError::IsaTypeMayBeAttributeOrObject { isa: isa.clone() })?;
            }
            is_object
        };
        if is_object {
            let write_to = VariablePosition::new((input_variables.len() + vertex_instructions.len()) as u32);
            output_variables.insert(isa.thing(), write_to);
            let instruction = InsertVertexInstruction::PutObject(PutObject { type_, write_to: ThingSource(write_to) });
            vertex_instructions.push(instruction);
        } else {
            let value_variable = resolve_value_variable_for_inserted_attribute(constraints, isa.thing())?;
            let value = if let Some(constant) = value_bindings.get(&value_variable) {
                debug_assert!(!input_variables.contains_key(&value_variable));
                ValueSource::ValueConstant(constant.clone().into_owned())
            } else if let Some(position) = input_variables.get(&value_variable) {
                ValueSource::InputVariable(position.clone())
            } else {
                return Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute {
                    variable: value_variable,
                })?;
            };
            let write_to = VariablePosition::new((input_variables.len() + vertex_instructions.len()) as u32);
            output_variables.insert(isa.thing(), write_to);
            let instruction =
                InsertVertexInstruction::PutAttribute(PutAttribute { type_, value, write_to: ThingSource(write_to) });
            vertex_instructions.push(instruction);
        };
        Ok(())
    })?;
    Ok(output_variables)
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    instructions: &mut Vec<InsertEdgeInstruction>,
) -> Result<(), WriteCompilationError> {
    filter_variants!(Constraint::Has: constraints).try_for_each(|has| {
        let owner = get_thing_source(input_variables, has.owner())?;
        let attribute = get_thing_source(input_variables, has.attribute())?;
        instructions.push(InsertEdgeInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_role_players(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    instructions: &mut Vec<InsertEdgeInstruction>,
) -> Result<(), WriteCompilationError> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    filter_variants!(Constraint::Links: constraints).try_for_each(|role_player| {
        let relation = get_thing_source(input_variables, role_player.relation())?;
        let player = get_thing_source(input_variables, role_player.player())?;
        let role_variable = role_player.role_type();
        let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
            (Some(input), None) => TypeSource::InputVariable(input.clone()),
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
        instructions.push(InsertEdgeInstruction::RolePlayer(RolePlayer { relation, player, role }));
        Ok(())
    })?;
    Ok(())
}

fn resolve_value_variable_for_inserted_attribute(
    constraints: &[Constraint<Variable>],
    variable: Variable,
) -> Result<Variable, WriteCompilationError> {
    // Find the comparison linking thing to value
    let comparisons = filter_variants!(Constraint::Comparison: constraints)
        .filter_map(|cmp| {
            if cmp.lhs() == variable {
                Some(cmp.rhs())
            } else if cmp.rhs() == variable {
                Some(cmp.lhs())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if comparisons.len() == 1 {
        Ok(comparisons[0])
    } else {
        debug_assert!(comparisons.len() == 0);
        Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute { variable })
    }
}

fn collect_value_bindings(
    constraints: &[Constraint<Variable>],
) -> Result<HashMap<Variable, Value<'static>>, WriteCompilationError> {
    let mut value_sources = HashMap::new();
    filter_variants!(Constraint::ExpressionBinding : constraints).try_for_each(|expr| {
        let Expression::Constant(constant) = expr.expression().get_root() else {
            unreachable!("The grammar does not allow compound expressions")
        };
        debug_assert!(!value_sources.contains_key(&expr.left()));
        value_sources.insert(expr.left(), constant.clone().into_owned());
        Ok(())
    })?;
    Ok(value_sources)
}

fn collect_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Variable, answer::Type>, WriteCompilationError> {
    let mut type_bindings: HashMap<Variable, answer::Type> = HashMap::new();
    filter_variants!(Constraint::Label : constraints).for_each(|label| {
        let annotations = type_annotations.variable_annotations_of(label.left()).unwrap();
        debug_assert!(annotations.len() == 1);
        let type_ = annotations.iter().find(|_| true).unwrap();
        debug_assert!(!type_bindings.contains_key(&label.left()));
        type_bindings.insert(label.left(), type_.clone());
    });
    Ok(type_bindings)
}

pub(crate) fn collect_role_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Variable, answer::Type>, WriteCompilationError> {
    let mut type_bindings: HashMap<Variable, answer::Type> = HashMap::new();
    filter_variants!(Constraint::RoleName : constraints).try_for_each(|role_name| {
        let annotations = type_annotations.variable_annotations_of(role_name.left()).unwrap();
        let type_ = if annotations.len() == 1 {
            annotations.iter().find(|_| true).unwrap()
        } else {
            return Err(WriteCompilationError::CouldNotUniquelyResolveRoleTypeFromName { variable: role_name.left() });
        };
        debug_assert!(!type_bindings.contains_key(&role_name.left()));
        type_bindings.insert(role_name.left(), type_.clone());
        Ok(())
    })?;
    Ok(type_bindings)
}
