/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::pattern::{
    constraint::{Comparator, Constraint},
    expression::Expression,
    ParameterID, Vertex,
};
use itertools::Itertools;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::{
        insert::{
            get_kinds_from_annotations, get_thing_source,
            instructions::{ConceptInstruction, ConnectionInstruction, Has, Links, PutAttribute, PutObject},
            ThingSource, TypeSource, ValueSource, VariableSource, WriteCompilationError,
        },
        next_executable_id,
    },
    filter_variants, VariablePosition,
};

#[derive(Debug)]
pub struct InsertExecutable {
    pub executable_id: u64,
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<(Variable, VariableSource)>>,
}

impl InsertExecutable {
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
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
) -> Result<InsertExecutable, Box<WriteCompilationError>> {
    let mut concept_inserts = Vec::with_capacity(constraints.len());
    let variables = add_inserted_concepts(constraints, input_variables, type_annotations, &mut concept_inserts)?;

    let mut connection_inserts = Vec::with_capacity(constraints.len());
    add_has(constraints, &variables, &mut connection_inserts)?;
    add_role_players(constraints, type_annotations, &variables, &mut connection_inserts)?;

    let output_width = variables.values().map(|i| i.position + 1).max().unwrap_or(0);
    let mut output_row_schema = vec![None; output_width as usize];
    variables.iter().map(|(v, i)| (i, v)).for_each(|(&i, &v)| {
        output_row_schema[i.position as usize] = Some((v, VariableSource::InputVariable(i)));
    });

    Ok(InsertExecutable {
        executable_id: next_executable_id(),
        concept_instructions: concept_inserts,
        connection_instructions: connection_inserts,
        output_row_schema,
    })
}

fn add_inserted_concepts(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    vertex_instructions: &mut Vec<ConceptInstruction>,
) -> Result<HashMap<Variable, VariablePosition>, Box<WriteCompilationError>> {
    let first_inserted_variable_position =
        input_variables.values().map(|pos| pos.position + 1).max().unwrap_or(0) as usize;
    let mut output_variables = input_variables.clone();
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;

    for isa in filter_variants!(Constraint::Isa: constraints) {
        let &Vertex::Variable(thing) = isa.thing() else { unreachable!() };

        if input_variables.contains_key(&thing) {
            return Err(Box::new(WriteCompilationError::IsaStatementForInputVariable { variable: thing }));
        }

        let type_ = if let Some(type_) = type_bindings.get(isa.type_()) {
            TypeSource::Constant(*type_)
        } else {
            match isa.type_() {
                Vertex::Variable(var) if input_variables.contains_key(var) => {
                    TypeSource::InputVariable(input_variables[var])
                }
                _ => {
                    return Err(Box::new(WriteCompilationError::CouldNotDetermineTypeOfInsertedVariable {
                        variable: thing,
                    }))
                }
            }
        };

        // Requires variable annotations for this stage to be available.
        let annotations = type_annotations.vertex_annotations_of(isa.type_()).unwrap();
        debug_assert!(!annotations.is_empty());
        let kinds = get_kinds_from_annotations(annotations);

        if kinds.contains(&Kind::Role) {
            return Err(Box::new(WriteCompilationError::IllegalInsertForRole { isa: isa.clone() }));
        }

        if kinds.contains(&Kind::Relation) || kinds.contains(&Kind::Entity) {
            if kinds.contains(&Kind::Attribute) {
                return Err(Box::new(WriteCompilationError::IsaTypeMayBeAttributeOrObject { isa: isa.clone() }));
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
                    return Err(Box::new(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute {
                        variable: thing,
                    }));
                }
            } else {
                return Err(Box::new(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute {
                    variable: thing,
                }));
            };
            let write_to = VariablePosition::new((first_inserted_variable_position + vertex_instructions.len()) as u32);
            output_variables.insert(thing, write_to);
            let instruction =
                ConceptInstruction::PutAttribute(PutAttribute { type_, value, write_to: ThingSource(write_to) });
            vertex_instructions.push(instruction);
        };
    }
    Ok(output_variables)
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
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
) -> Result<(), Box<WriteCompilationError>> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    for role_player in filter_variants!(Constraint::Links: constraints) {
        let relation = get_thing_source(input_variables, role_player.relation().as_variable().unwrap())?;
        let player = get_thing_source(input_variables, role_player.player().as_variable().unwrap())?;
        let &Vertex::Variable(role_variable) = role_player.role_type() else { unreachable!() };

        let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
            (Some(&input), None) => TypeSource::InputVariable(input),
            (None, Some(type_)) => TypeSource::Constant(*type_),
            (None, None) => {
                // TODO: Do we want to support inserts with unspecified role-types?
                let annotations = type_annotations.vertex_annotations_of(&Vertex::Variable(role_variable)).unwrap();
                if annotations.len() == 1 {
                    TypeSource::Constant(*annotations.iter().find(|_| true).unwrap())
                } else {
                    return Err(Box::new(WriteCompilationError::CouldNotUniquelyDetermineRoleType {
                        variable: role_variable,
                    }));
                }
            }
            (Some(_), Some(_)) => unreachable!(),
        };
        instructions.push(ConnectionInstruction::Links(Links { relation, player, role }));
    }
    Ok(())
}

fn resolve_value_variable_for_inserted_attribute(
    constraints: &[Constraint<Variable>],
    variable: Variable,
) -> Result<&Vertex<Variable>, Box<WriteCompilationError>> {
    // Find the comparison linking thing to value
    let (comparator, value_variable) = filter_variants!(Constraint::Comparison: constraints)
        .filter_map(|cmp| {
            if cmp.lhs() == &Vertex::Variable(variable) {
                Some((cmp.comparator(), cmp.rhs()))
            } else if cmp.rhs() == &Vertex::Variable(variable) {
                Some((cmp.comparator(), cmp.lhs()))
            } else {
                None
            }
        })
        .exactly_one()
        .map_err(|mut err| {
            debug_assert_eq!(err.next(), None);
            Box::new(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute { variable })
        })?;
    if comparator == Comparator::Equal {
        Ok(value_variable)
    } else {
        Err(Box::new(WriteCompilationError::IllegalPredicateInAttributeInsert { variable, comparator }))
    }
}

fn collect_value_bindings(
    constraints: &[Constraint<Variable>],
) -> Result<HashMap<&Vertex<Variable>, ParameterID>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::ExpressionBinding : constraints)
        .map(|expr| {
            let &Expression::Constant(constant) = expr.expression().get_root() else {
                unreachable!("The grammar does not allow compound expressions")
            };
            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(&expr.left()));
                seen.insert(expr.left());
            }

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
) -> Result<HashMap<Vertex<Variable>, answer::Type>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::Label : constraints)
        .map(|label| {
            let annotations = type_annotations.vertex_annotations_of(label.type_()).unwrap();
            #[cfg(debug_assertions)]
            debug_assert!(annotations.len() == 1);
            let type_ = annotations.first().unwrap();

            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(label.type_()));
                seen.insert(label.type_());
            }

            Ok((label.type_().clone(), *type_))
        })
        .chain(constraints.iter().flat_map(|con| con.vertices().filter(|v| v.is_label())).map(|label| {
            let type_ = type_annotations.vertex_annotations_of(label).unwrap().iter().exactly_one().unwrap();
            Ok((label.clone(), *type_))
        }))
        .collect()
}

pub(crate) fn collect_role_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Variable, answer::Type>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::RoleName : constraints)
        .map(|role_name| {
            let annotations = type_annotations.vertex_annotations_of(role_name.type_()).unwrap();
            let variable = role_name.type_().as_variable().unwrap();
            let type_ = if annotations.len() == 1 {
                annotations.iter().find(|_| true).unwrap()
            } else {
                return Err(Box::new(WriteCompilationError::CouldNotUniquelyResolveRoleTypeFromName { variable }));
            };

            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(&variable));
                seen.insert(variable);
            }

            Ok((variable, *type_))
        })
        .collect()
}
