/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// jk there's no planning to be done here, just execution.
// There is a need to construct the executor though.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::{
    pattern::{constraint::Constraint, expression::Expression, ParameterID, Vertex},
    program::block::VariableRegistry,
};
use itertools::Itertools;

use crate::{
    filter_variants,
    insert::{
        get_kinds_from_annotations, get_thing_source,
        instructions::{ConceptInstruction, ConnectionInstruction, Has, PutAttribute, PutObject, RolePlayer},
        ThingSource, TypeSource, ValueSource, VariableSource, WriteCompilationError,
    },
    match_::inference::type_annotations::TypeAnnotations,
    VariablePosition,
};

pub struct InsertProgram {
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<(Variable, VariableSource)>>,
    pub variable_registry: Arc<VariableRegistry>,
}

impl InsertProgram {
    pub fn output_width(&self) -> usize {
        self.output_row_schema.len()
    }
}

/*
 * Assumptions:
 *   - Any labels have been assigned to a type variable and added as a type-annotation, though we should have a more explicit mechanism for this.
 *   - Validation has already been done - An input row will not violate schema on insertion.
 */

pub fn compile(
    variable_registry: Arc<VariableRegistry>,
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
) -> Result<InsertProgram, WriteCompilationError> {
    let mut concept_inserts = Vec::with_capacity(constraints.len());
    let variables = add_inserted_concepts(constraints, input_variables, type_annotations, &mut concept_inserts)?;

    let mut connection_inserts = Vec::with_capacity(constraints.len());
    add_has(constraints, &variables, &mut connection_inserts)?;
    add_role_players(constraints, type_annotations, &variables, &mut connection_inserts)?;

    let output_width = variables.iter().map(|(_, i)| i.position + 1).max().unwrap_or(0);
    let mut output_row_schema = vec![None; output_width as usize];
    variables.iter().map(|(v, i)| (i, v)).sorted().for_each(|(&i, &v)| {
        output_row_schema[i.position as usize] = Some((v, VariableSource::InputVariable(i)));
    });

    Ok(InsertProgram {
        concept_instructions: concept_inserts,
        connection_instructions: connection_inserts,
        output_row_schema,
        variable_registry,
    })
}

fn add_inserted_concepts(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    vertex_instructions: &mut Vec<ConceptInstruction>,
) -> Result<HashMap<Variable, VariablePosition>, WriteCompilationError> {
    let first_inserted_variable_position: usize =
        input_variables.iter().map(|(_, pos)| pos.position + 1).max().unwrap_or(0) as usize;
    let mut output_variables = input_variables.clone();
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;
    filter_variants!(Constraint::Isa : constraints).try_for_each(|isa| {
        let &Vertex::Variable(thing) = isa.thing() else { unreachable!() };

        if input_variables.contains_key(&thing) {
            return Err(WriteCompilationError::IsaStatementForInputVariable { variable: thing });
        }

        let type_ = if let Some(type_) = type_bindings.get(isa.type_()) {
            TypeSource::Constant(type_.clone())
        } else {
            match isa.type_() {
                Vertex::Variable(var) if input_variables.contains_key(var) => {
                    TypeSource::InputVariable(input_variables[var])
                }
                _ => return Err(WriteCompilationError::CouldNotDetermineTypeOfInsertedVariable { variable: thing }),
            }
        };

        // Requires variable annotations for this stage to be available.
        let annotations = type_annotations.vertex_annotations_of(isa.type_()).unwrap();
        debug_assert!(!annotations.is_empty());
        let kinds = get_kinds_from_annotations(annotations);

        if kinds.contains(&Kind::Role) {
            return Err(WriteCompilationError::IllegalInsertForRole { isa: isa.clone() });
        }

        if kinds.contains(&Kind::Relation) || kinds.contains(&Kind::Entity) {
            if kinds.contains(&Kind::Attribute) {
                return Err(WriteCompilationError::IsaTypeMayBeAttributeOrObject { isa: isa.clone() });
            }
            let write_to = VariablePosition::new((first_inserted_variable_position + vertex_instructions.len()) as u32);
            output_variables.insert(thing, write_to);
            let instruction = ConceptInstruction::PutObject(PutObject { type_, write_to: ThingSource(write_to) });
            vertex_instructions.push(instruction);
        } else {
            debug_assert!(kinds.len() == 1 && kinds.contains(&Kind::Attribute));
            let value_variable = resolve_value_variable_for_inserted_attribute(constraints, thing)?;
            let value = if let Some(&constant) = value_bindings.get(&value_variable) {
                debug_assert!(!value_variable
                    .as_variable()
                    .is_some_and(|variable| input_variables.contains_key(&variable)));
                ValueSource::Parameter(constant)
            } else if let &Vertex::Variable(variable) = value_variable {
                if let Some(&position) = input_variables.get(&variable) {
                    ValueSource::Variable(position)
                } else {
                    return Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute { variable: thing })?;
                }
            } else {
                return Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute { variable: thing })?;
            };
            let write_to = VariablePosition::new((first_inserted_variable_position + vertex_instructions.len()) as u32);
            output_variables.insert(thing, write_to);
            let instruction =
                ConceptInstruction::PutAttribute(PutAttribute { type_, value, write_to: ThingSource(write_to) });
            vertex_instructions.push(instruction);
        };
        Ok(())
    })?;
    Ok(output_variables)
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), WriteCompilationError> {
    filter_variants!(Constraint::Has: constraints).try_for_each(|has| {
        let owner = get_thing_source(input_variables, has.owner().as_variable().unwrap())?;
        let attribute = get_thing_source(input_variables, has.attribute().as_variable().unwrap())?;
        instructions.push(ConnectionInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_role_players(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), WriteCompilationError> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    filter_variants!(Constraint::Links: constraints).try_for_each(|role_player| {
        let relation = get_thing_source(input_variables, role_player.relation().as_variable().unwrap())?;
        let player = get_thing_source(input_variables, role_player.player().as_variable().unwrap())?;
        let &Vertex::Variable(role_variable) = role_player.role_type() else { unreachable!() };

        let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
            (Some(&input), None) => TypeSource::InputVariable(input),
            (None, Some(type_)) => TypeSource::Constant(type_.clone()),
            (None, None) => {
                // TODO: Do we want to support inserts with unspecified role-types?
                let annotations = type_annotations.vertex_annotations_of(&Vertex::Variable(role_variable)).unwrap();
                if annotations.len() == 1 {
                    TypeSource::Constant(annotations.iter().find(|_| true).unwrap().clone())
                } else {
                    return Err(WriteCompilationError::CouldNotUniquelyDetermineRoleType { variable: role_variable })?;
                }
            }
            (Some(_), Some(_)) => unreachable!(),
        };
        instructions.push(ConnectionInstruction::RolePlayer(RolePlayer { relation, player, role }));
        Ok(())
    })?;
    Ok(())
}

fn resolve_value_variable_for_inserted_attribute(
    constraints: &[Constraint<Variable>],
    variable: Variable,
) -> Result<&Vertex<Variable>, WriteCompilationError> {
    // Find the comparison linking thing to value
    let comparisons = filter_variants!(Constraint::Comparison: constraints)
        .filter_map(|cmp| {
            if cmp.lhs() == &Vertex::Variable(variable) {
                Some(cmp.rhs())
            } else if cmp.rhs() == &Vertex::Variable(variable) {
                Some(cmp.lhs())
            } else {
                None
            }
        })
        .collect_vec();
    if comparisons.len() == 1 {
        Ok(comparisons[0])
    } else {
        debug_assert!(comparisons.is_empty());
        Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute { variable })
    }
}

fn collect_value_bindings(
    constraints: &[Constraint<Variable>],
) -> Result<HashMap<&Vertex<Variable>, ParameterID>, WriteCompilationError> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::ExpressionBinding : constraints)
        .map(|expr| {
            let &Expression::Constant(constant) = expr.expression().get_root() else {
                unreachable!("The grammar does not allow compound expressions")
            };

            debug_assert!(!seen.contains(&expr.left()));
            #[cfg(debug_assertions)]
            seen.insert(expr.left());

            Ok((expr.left(), constant))
        })
        .chain(
            constraints
                .iter()
                .flat_map(|con| con.vertices().filter(|v| v.is_parameter()))
                .map(|param| Ok((param, param.as_parameter().unwrap()))),
        )
        .collect()
}

fn collect_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Vertex<Variable>, answer::Type>, WriteCompilationError> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::Label : constraints)
        .map(|label| {
            let annotations = type_annotations.vertex_annotations_of(label.left()).unwrap();
            debug_assert!(annotations.len() == 1);
            let type_ = annotations.first().unwrap();

            debug_assert!(!seen.contains(label.left()));
            #[cfg(debug_assertions)]
            seen.insert(label.left());

            Ok((label.left().clone(), type_.clone()))
        })
        .chain(constraints.iter().flat_map(|con| con.vertices().filter(|v| v.is_label())).map(|label| {
            let type_ = type_annotations.vertex_annotations_of(label).unwrap().iter().exactly_one().unwrap();
            Ok((label.clone(), type_.clone()))
        }))
        .collect()
}

pub(crate) fn collect_role_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Variable, answer::Type>, WriteCompilationError> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::RoleName : constraints)
        .map(|role_name| {
            let annotations = type_annotations.vertex_annotations_of(role_name.left()).unwrap();
            let variable = role_name.left().as_variable().unwrap();
            let type_ = if annotations.len() == 1 {
                annotations.iter().find(|_| true).unwrap()
            } else {
                return Err(WriteCompilationError::CouldNotUniquelyResolveRoleTypeFromName { variable });
            };

            debug_assert!(!seen.contains(&variable));
            #[cfg(debug_assertions)]
            seen.insert(variable);

            Ok((variable, type_.clone()))
        })
        .collect()
}
