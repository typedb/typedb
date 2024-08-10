/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// jk there's no planning to be done here, just execution.
// There is a need to construct the executor though.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
};

use answer::{variable::Variable, Type};
use encoding::{graph::type_::Kind, value::value::Value};
use ir::pattern::{
    constraint::{Constraint, Isa},
    expression::Expression,
};
use itertools::Itertools;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    inference::type_annotations::TypeAnnotations,
    write::{
        determine_unique_kind, get_thing_source,
        write_instructions::{Has, PutAttribute, PutEntity, PutRelation, RolePlayer},
        TypeSource, ValueSource, VariableSource,
    },
};

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}

#[derive(Debug)]
pub enum InsertInstruction {
    // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
    PutEntity(PutEntity),
    PutAttribute(PutAttribute),
    PutRelation(PutRelation),
    Has(Has),               // TODO: Ordering
    RolePlayer(RolePlayer), // TODO: Ordering
}

pub struct InsertPlan {
    pub instructions: Vec<InsertInstruction>,
    pub n_created_concepts: usize,
    pub output_row_plan: Vec<VariableSource>, // Where to copy from
    pub debug_info: HashMap<VariableSource, Variable>,
}

/*
* Assumptions:
*   - Any labels have been assigned to a type variable and added as a type-annotation, though we should have a more explicit mechanism for this.
*   - Validation has already been done - An input row will not violate schema on insertion.
*/

pub fn build_insert_plan(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, usize>,
    type_annotations: &TypeAnnotations,
) -> Result<InsertPlan, WriteCompilationError> {
    let mut instructions = Vec::new();
    let inserted_concepts = add_inserted_concepts(constraints, input_variables, type_annotations, &mut instructions)?;
    add_has(constraints, input_variables, &inserted_concepts, &mut instructions)?;
    add_role_players(constraints, type_annotations, input_variables, &inserted_concepts, &mut instructions)?;

    let mut output_row_plan = Vec::with_capacity(input_variables.len() + inserted_concepts.len()); // TODO
    input_variables.iter().map(|(v, i)| (i, v)).sorted().for_each(|(i, v)| {
        debug_assert!(*i == output_row_plan.len());
        output_row_plan.push(VariableSource::InputVariable(*i as u32));
    });
    inserted_concepts.iter().map(|(v, i)| (i, v)).sorted().for_each(|(i, v)| {
        debug_assert!(*i + input_variables.len() == output_row_plan.len());
        output_row_plan.push(VariableSource::InsertedThing(*i));
    });

    let debug_info = HashMap::new(); // TODO
    Ok(InsertPlan { instructions, n_created_concepts: inserted_concepts.len(), output_row_plan, debug_info })
}

fn add_inserted_concepts(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, usize>,
    type_annotations: &TypeAnnotations,
    instructions: &mut Vec<InsertInstruction>,
) -> Result<HashMap<Variable, usize>, WriteCompilationError> {
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;
    let mut inserted_concepts = HashMap::new();
    filter_variants!(Constraint::Isa : constraints).try_for_each(|isa| {
        let type_ = match (input_variables.get(&isa.type_()), type_bindings.get(&isa.type_())) {
            (Some(input), None) => TypeSource::InputVariable(*input as u32),
            (None, Some(type_)) => TypeSource::TypeConstant(type_.clone()),
            (Some(_), Some(_)) => unreachable!("Explicit label constraints are banned in insert"),
            (None, None) => {
                Err(WriteCompilationError::CouldNotDetermineTypeOfInsertedVariable { variable: isa.thing() })?
            }
        };
        let annotations = type_annotations.variable_annotations_of(isa.type_()).unwrap();
        let kind = determine_unique_kind(annotations)
            .map_err(|_| WriteCompilationError::IsaTypeHasMultipleKinds { isa: isa.clone() })?;

        let instruction = match kind {
            Kind::Entity => InsertInstruction::PutEntity(PutEntity { type_ }),
            Kind::Relation => InsertInstruction::PutRelation(PutRelation { type_ }),
            Kind::Attribute => {
                let value_variable = resolve_value_variable_for_inserted_attribute(constraints, isa.thing())?;
                let value = if let Some(constant) = value_bindings.get(&value_variable) {
                    debug_assert!(!input_variables.contains_key(&value_variable));
                    ValueSource::ValueConstant(constant.clone().into_owned())
                } else if let Some(position) = input_variables.get(&value_variable) {
                    ValueSource::InputVariable(*position as u32)
                } else {
                    return Err(WriteCompilationError::CouldNotDetermineValueOfInsertedAttribute {
                        variable: value_variable,
                    })?;
                };
                InsertInstruction::PutAttribute(PutAttribute { type_, value })
            }
            Kind::Role => {
                return Err(WriteCompilationError::IsaStatementForRoleType { isa: isa.clone() })?;
            }
        };
        inserted_concepts.insert(isa.thing(), inserted_concepts.len());
        instructions.push(instruction);
        Ok(())
    })?;
    Ok(inserted_concepts)
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, usize>,
    inserted_concepts: &HashMap<Variable, usize>,
    instructions: &mut Vec<InsertInstruction>,
) -> Result<(), WriteCompilationError> {
    filter_variants!(Constraint::Has: constraints).try_for_each(|has| {
        let owner = get_thing_source(input_variables, inserted_concepts, has.owner())?;
        let attribute = get_thing_source(input_variables, inserted_concepts, has.attribute())?;
        instructions.push(InsertInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_role_players(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, usize>,
    inserted_concepts: &HashMap<Variable, usize>,
    instructions: &mut Vec<InsertInstruction>,
) -> Result<(), WriteCompilationError> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    filter_variants!(Constraint::RolePlayer: constraints).try_for_each(|role_player| {
        let relation = get_thing_source(input_variables, inserted_concepts, role_player.relation())?;
        let player = get_thing_source(input_variables, inserted_concepts, role_player.player())?;
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
        instructions.push(InsertInstruction::RolePlayer(RolePlayer { relation, player, role }));
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

#[derive(Debug, Clone)]
pub enum WriteCompilationError {
    VariableIsBothInsertedAndInput { variable: Variable },
    IsaStatementForRoleType { isa: Isa<Variable> },
    IsaTypeHasMultipleKinds { isa: Isa<Variable> },
    CouldNotDetermineTypeOfInsertedVariable { variable: Variable },
    CouldNotDetermineValueOfInsertedAttribute { variable: Variable },
    CouldNotDetermineThingVariableSource { variable: Variable },
    CouldNotUniquelyResolveRoleTypeFromName { variable: Variable },
    CouldNotUniquelyDetermineRoleType { variable: Variable },

    IllegalRoleDelete { variable: Variable },
    DeleteHasMultipleKinds { variable: Variable },
}

impl Display for WriteCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteCompilationError {}
