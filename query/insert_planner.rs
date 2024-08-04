/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// jk there's no planning to be done here, just execution.
// There is a need to construct the executor though.


use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use answer::Type;
use answer::variable::Variable;
use answer::variable_value::VariableValue;
use concept::type_::type_manager::TypeManager;
use encoding::graph::type_::CapabilityKind::Plays;
use encoding::graph::type_::Kind;
use encoding::value::label::Label;
use encoding::value::value::Value;
use ir::inference::type_inference::TypeAnnotations;
use ir::pattern::constraint::{Constraint, Constraints, Isa};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use traversal::executor::insert_executor::{VariableSource, InsertExecutor, InsertInstruction, TypeSource, ValueSource, ThingSource};
use traversal::executor::VariablePosition;
use crate::define::filter_variants;

type AttributeValue<T> = PhantomData<T>; // TODO: Make the IR

pub fn build_insert_plan(
    // snapshot: &impl ReadableSnapshot,
    // type_manager: &TypeManager,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    constraints: &Vec<Constraint<Variable>>,
) -> Result<InsertExecutor, ()> {
    let type_constants = collect_type_from_labels(constraints, type_annotations)?;
    let value_constants = collect_values_from_attribute_value(constraints, &input_variables)?;
    let isa_kinds = collect_kind_from_isa(constraints, input_variables, type_annotations)?;

    let mut instructions = Vec::new();
    filter_variants!(Constraint::Isa : constraints).enumerate().try_for_each(|(i,isa)| {
        debug_assert!(*isa_kinds.index.get(&isa.thing()) == i);
        let type_ = get_type_source(&input_variables, &type_constants.index, isa.type_())?;
        let kind = type_annotations.get();
        let instruction = match kind {
            Kind::Entity => InsertInstruction::Entity { type_ },
            Kind::Attribute => {
                let value = get_value_source(&input_variables, &value_constants.index, isa.thing())?;
                InsertInstruction::Attribute { type_, value }
            },
            Kind::Relation => InsertInstruction::Relation { type_ },
            Kind::Role => Err(InsertCompilationError::IsaStatementForRoleType { isa: isa.clone() })?,
        };
        instructions.push(instruction);
    });


    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                instructions.push(InsertInstruction::Has {
                    owner: get_thing_source(input_variables, &isa_kinds.index, has.owner())?,
                    attribute: get_thing_source(input_variables, &isa_kinds.index, has.attribute())?,
                });
            }
            Constraint::RolePlayer(role_player) => {
                instructions.push(InsertInstruction::RolePlayer {
                    relation: get_thing_source(input_variables, &isa_kinds.index, role_player.relation())?,
                    player: get_thing_source(input_variables, &isa_kinds.index, role_player.player())?,
                    role: get_type_source(input_variables, &type_constants.index, role_player.role_type())?,
                });
            }
            Constraint::Isa(_) => {} // already handled
            Constraint::Label(_)
            | Constraint::Sub(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::Comparison(_) => unreachable!()
        }
    }
    Ok(InsertExecutor {

    })
}

fn collect_kind_from_isa(
    constraints: &Vec<Constraint<Variable>>,
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
) -> Result<Sources<Kind>, InsertCompilationError> {
    let mut inserted_things = Sources::new();
    filter_variants!(Constraint::Isa : constraints).try_for_each(|isa| {
        if input_variables.contains_key(&isa.thing()) {
            Err(InsertCompilationError::IsaConstraintForBoundVariable {  variable: isa.thing().clone() })?;
        }
        let annotations = match type_annotations.variable_annotations(isa.type_()) {
            Some(annotations) => annotations,
            None => Err(InsertCompilationError::IsaTypeHadNoAnnotations { isa: isa.clone() })?,
        };
        let kinds = annotations.iter()
            .map(|annotation| annotation.kind().clone())
            .collect::<HashSet<_>>().into_iter().collect_vec();
        let kind = match kinds.len() {
            1 => kinds[0],
            _ => Err(InsertCompilationError::IsaTypeHasMultipleKinds { isa: isa.clone() })?,
        };
        inserted_things.insert(isa.thing(), kind).map_err(|_| {
            Err(InsertCompilationError::MultipleIsaConstraintsForVariable { variable: isa.thing().clone() })
        })?;
        Ok(())
    })?;
    Ok(inserted_things)
}

fn collect_values_from_attribute_value(
    constraints: &Vec<Constraint<Variable>>,
    input_variables: &HashMap<Variable, VariablePosition>,
) -> Result<Sources<Value<'static>>, InsertCompilationError> {
    let value_constants = Sources::new();
    // filter_variants!(Constraint::AttributeValue : &constraints).try_for_each(|attribute_value| {
    //     let attribute_variable: Variable = None.unwrap();  // TODO
    //     let value_variable: Variable = None.unwrap(); // TODO
    //     todo!("remove me once the variables at the top are no longer None.unwrap()");
    //     Ok(())
    // })?;
    Ok(value_constants)
}

fn collect_type_from_labels(
    constraints: &Vec<Constraint<Variable>>,
    type_annotations: &TypeAnnotations,
) -> Result<Sources<Type>, InsertCompilationError> {
    let mut type_constants: Sources<Type> = Sources::new();
    filter_variants!(Constraint::Label : &constraints).try_for_each(|label | {
        let annotations = type_annotations.variable_annotations(label.left()).unwrap();
        debug_assert!(annotations.len() == 1);
        let inserted_type = annotations.iter().find(|_| true).unwrap().clone();
        type_constants.insert(label.left(), inserted_type).map_err(|_| {
            Err(InsertCompilationError::MultipleTypeConstraintsForVariable { variable: label.left() })
        })?;
        Ok(())
    })?;
    Ok(type_constants)
}

pub enum InsertCompilationError {
    MultipleValueConstraints { variable: Variable },
    IsaConstraintForBoundVariable {variable: Variable },
    MultipleIsaConstraintsForVariable { variable: Variable },
    IsaTypeCouldNotBeUniquelyDetemrined { isa: Isa<Variable> },
    IsaStatementForRoleType { isa: Isa<Variable> },
    IsaTypeHadNoAnnotations { isa: Isa<Variable> },
    IsaTypeHasMultipleKinds { isa: Isa<Variable> },
    CouldNotDetermineTypeVariableSource { variable: Variable },
    CouldNotDetermineValueVariableSource { variable: Variable },
    CouldNotDetermineInsertedVariableSource { variable: Variable },
    MultipleTypeConstraintsForVariable { variable: Variable },
}

struct Sources<T> {
    items: Vec<T>,
    index: HashMap<Variable, usize>
}
impl<T> Sources<T> {

    fn new() -> Self {
        Self { items: Vec::new(), index: HashMap::new() }
    }
    fn insert(&mut self, variable: Variable, item: T) -> Result<usize, ()> {
        if self.index.contains_key(&variable) {
            Err(())
        } else {
            let index = self.items.len();
            self.index.insert(variable, index);
            self.items.push(item);
            Ok(index)
        }
    }
}

fn get_type_source(input_index: &HashMap<Variable, VariablePosition>, type_index: &HashMap<Variable, usize>, variable: Variable) -> Result<TypeSource,InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(TypeSource::Input(position.clone()))
    } else if let Some(index) = type_index.get(&variable) {
        Ok(TypeSource::TypeConstant(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineTypeVariableSource { variable })
    }
}

fn get_value_source(input_index: &HashMap<Variable, VariablePosition>, value_index: &HashMap<Variable, usize>, variable: Variable) -> Result<ValueSource,InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(ValueSource::Input(position.clone()))
    } else if let Some(index) = value_index.get(&variable) {
        Ok(ValueSource::ValueConstant(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineValueVariableSource { variable })
    }
}


fn get_thing_source(input_index: &HashMap<Variable, VariablePosition>, inserted_index: &HashMap<Variable, usize>, variable: Variable) -> Result<ThingSource,InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(ThingSource::Input(position.clone()))
    } else if let Some(index) = inserted_index.get(&variable) {
        Ok(ThingSource::Inserted(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineInsertedVariableSource { variable })
    }
}
