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
    marker::PhantomData,
    sync::Arc,
};

use answer::{variable::Variable, variable_value::VariableValue, Type};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::type_::{CapabilityKind::Plays, Kind},
    value::{label::Label, value::Value},
};
use ir::pattern::constraint::{Comparison, Constraint, Constraints, Isa};
use ir::pattern::expression::Expression;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::inference::type_annotations::TypeAnnotations;

macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}

pub type VariablePosition = usize; // Why is that not in plan.

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum VariableSource {
    TypeConstant(TypeSource),
    ValueConstant(ValueSource),
    ThingSource(ThingSource),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum TypeSource {
    Input(VariablePosition),
    TypeConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueSource {
    Input(VariablePosition),
    ValueConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ThingSource {
    Input(VariablePosition),
    Inserted(usize),
}

#[derive(Debug)]
pub enum InsertInstruction {
    // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
    Entity { type_: TypeSource },
    Attribute { type_: TypeSource, value: ValueSource },
    Relation { type_: TypeSource },
    Has { owner: ThingSource, attribute: ThingSource }, // TODO: Ordering
    RolePlayer { relation: ThingSource, player: ThingSource, role: TypeSource }, // TODO: Ordering
}

pub struct InsertPlan {
    pub instructions: Vec<InsertInstruction>,
    pub type_constants: Vec<answer::Type>,
    pub value_constants: Vec<Value<'static>>,
    pub n_created_concepts: usize,
    pub output_row: Vec<VariableSource>, // Where to copy from
    pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn build_insert_plan(
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    constraints: &[Constraint<Variable>],
) -> Result<InsertPlan, InsertCompilationError> {
    let type_constants = collect_type_from_labels(constraints, type_annotations)?;
    let value_constants = collect_values_from_expression_and_comparison(constraints, &input_variables)?;
    let isa_kinds = collect_kind_from_isa(constraints, input_variables, type_annotations)?;
    // extend_isa_from_has_and_value(constraints, input_variables, &value_constants, &mut isa_kinds)?;

    let mut instructions = Vec::new();
    filter_variants!(Constraint::Isa : constraints).enumerate().try_for_each(|(i, isa)| {
        debug_assert!(*isa_kinds.index.get(&isa.thing()).unwrap() == i);
        let type_ = get_type_source(&input_variables, &type_constants.index, isa.type_())?;
        let kind = isa_kinds.items[i];
        let instruction = match kind {
            Kind::Entity => InsertInstruction::Entity { type_ },
            Kind::Attribute => {
                let value = get_value_source(&input_variables, &value_constants.index, isa.thing())?;
                InsertInstruction::Attribute { type_, value }
            }
            Kind::Relation => InsertInstruction::Relation { type_ },
            Kind::Role => Err(InsertCompilationError::IsaStatementForRoleType { isa: isa.clone() })?,
        };
        instructions.push(instruction);
        Ok(())
    })?;

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
            Constraint::Isa(_)
            | Constraint::Label(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Comparison(_)=> {} // already handled
            Constraint::Sub(_)
            | Constraint::FunctionCallBinding(_)=> unreachable!(),
        }
    }

    Ok(assemble_plan(instructions, value_constants, type_constants, isa_kinds))
}

fn assemble_plan(instructions: Vec<InsertInstruction>, value_sources: Sources<Value<'static>>, type_sources: Sources<Type>, kind_sources: Sources<Kind>) -> InsertPlan {
    let Sources { items: value_constants, index: value_index } = value_sources;
    let Sources { items: type_constants, index: type_index } = type_sources;
    let Sources { items: inserted_kinds, index: isa_index} = kind_sources;
    let mut debug_info = HashMap::new();
    isa_index.into_iter().for_each(|(v,i)| { debug_info.insert(VariableSource::ThingSource(ThingSource::Inserted(i)), v); });
    type_index.into_iter().for_each(|(v,i)| { debug_info.insert(VariableSource::TypeConstant(TypeSource::TypeConstant(i)), v); });
    value_index.into_iter().for_each(|(v,i)| { debug_info.insert(VariableSource::ValueConstant(ValueSource::ValueConstant(i)), v); });
    InsertPlan {
        instructions,
        type_constants,
        value_constants,
        n_created_concepts: inserted_kinds.len(),
        output_row: vec![], // TODO
        debug_info,
    }
}

fn collect_kind_from_isa(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
) -> Result<Sources<Kind>, InsertCompilationError> {
    let mut inserted_things = Sources::new();
    filter_variants!(Constraint::Isa : constraints).try_for_each(|isa| {
        if input_variables.contains_key(&isa.thing()) {
            Err(InsertCompilationError::IsaConstraintForBoundVariable { variable: isa.thing().clone() })?;
        }
        let annotations = match type_annotations.variable_annotations_of(isa.type_()) {
            Some(annotations) => annotations,
            None => Err(InsertCompilationError::IsaTypeHadNoAnnotations { isa: isa.clone() })?,
        };
        let kinds = annotations
            .iter()
            .map(|annotation| annotation.kind().clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let kind = match kinds.len() {
            1 => kinds[0],
            _ => Err(InsertCompilationError::IsaTypeHasMultipleKinds { isa: isa.clone() })?,
        };
        inserted_things
            .insert(isa.thing(), kind)
            .map_err(|_| InsertCompilationError::MultipleIsaConstraintsForVariable { variable: isa.thing().clone() })?;
        Ok(())
    })?;
    Ok(inserted_things)
}

fn collect_values_from_expression_and_comparison(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
) -> Result<Sources<Value<'static>>, InsertCompilationError> {
    let values = filter_variants!(Constraint::ExpressionBinding : constraints).map(|binding| {
        match binding.expression().get_root() {
            Expression::Constant(constant) => Ok((binding.left(), constant.clone())),
            _ => Err(InsertCompilationError::CompoundExpressionsNotAllowed { variable: binding.left() })
        }
    }).collect::<Result<HashMap<_,_>, _>>()?;
    let mut value_constants = Sources::new();
    filter_variants!(Constraint::Comparison : constraints).try_for_each(|cmp| {
        if let Some(value) = values.get(&cmp.lhs()) {
            value_constants.insert(cmp.rhs(), value.clone())
                .map_err(|_|  InsertCompilationError::MultipleValuesForInsertableAttributeVariable { variable: cmp.rhs() })?;
        } else if let Some(value) = values.get(&cmp.rhs()) {
            value_constants.insert(cmp.lhs(), value.clone())
                .map_err(|_|  InsertCompilationError::MultipleValuesForInsertableAttributeVariable { variable: cmp.lhs() })?;
        } else {
            Err(InsertCompilationError::TODO__IllegalComparison { comparison: cmp.clone() })?;
        }
        Ok(())
    })?;

    Ok(value_constants)
}

// TODO: Remove. Turns out we already have an isa for attributes with value
// fn extend_isa_from_has_and_value(
//     constraints: &[Constraint<Variable>],
//     input_variables: &HashMap<Variable, VariablePosition>,
//     value_constants: &Sources<Value<'static>>,
//     isa: &mut Sources<Kind>,
// ) -> Result<(), InsertCompilationError> {
//     filter_variants!(Constraint::Has : constraints).try_for_each(|has| {
//         if value_constants.index.contains_key(&has.attribute()) {
//             debug_assert!(!input_variables.contains_key(&has.attribute()));
//             isa.insert(has.attribute(), Kind::Attribute).map_err(|_| {
//                 InsertCompilationError::TODO__IllegalState { msg: "extend_isa_from_has_and_value" }
//             })?;
//         }
//         Ok(())
//     })
// }

fn collect_type_from_labels(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<Sources<Type>, InsertCompilationError> {
    let mut type_constants: Sources<Type> = Sources::new();
    filter_variants!(Constraint::Label : &constraints).try_for_each(|label| {
        let annotations = type_annotations.variable_annotations_of(label.left()).unwrap();
        debug_assert!(annotations.len() == 1);
        let inserted_type = annotations.iter().find(|_| true).unwrap().clone();
        type_constants
            .insert(label.left(), inserted_type)
            .map_err(|_| InsertCompilationError::MultipleTypeConstraintsForVariable { variable: label.left() })?;
        Ok(())
    })?;
    Ok(type_constants)
}

struct Sources<T> {
    items: Vec<T>,
    index: HashMap<Variable, usize>,
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

fn get_type_source(
    input_index: &HashMap<Variable, VariablePosition>,
    type_index: &HashMap<Variable, usize>,
    variable: Variable,
) -> Result<TypeSource, InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(TypeSource::Input(position.clone()))
    } else if let Some(index) = type_index.get(&variable) {
        Ok(TypeSource::TypeConstant(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineTypeVariableSource { variable })
    }
}

fn get_value_source(
    input_index: &HashMap<Variable, VariablePosition>,
    value_index: &HashMap<Variable, usize>,
    variable: Variable,
) -> Result<ValueSource, InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(ValueSource::Input(position.clone()))
    } else if let Some(index) = value_index.get(&variable) {
        Ok(ValueSource::ValueConstant(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineValueVariableSource { variable })
    }
}

fn get_thing_source(
    input_index: &HashMap<Variable, VariablePosition>,
    inserted_index: &HashMap<Variable, usize>,
    variable: Variable,
) -> Result<ThingSource, InsertCompilationError> {
    if let Some(position) = input_index.get(&variable) {
        Ok(ThingSource::Input(position.clone()))
    } else if let Some(index) = inserted_index.get(&variable) {
        Ok(ThingSource::Inserted(*index))
    } else {
        Err(InsertCompilationError::CouldNotDetermineInsertedVariableSource { variable })
    }
}

#[derive(Debug, Clone)]
pub enum InsertCompilationError {
    MultipleValueConstraints { variable: Variable },
    IsaConstraintForBoundVariable { variable: Variable },
    MultipleIsaConstraintsForVariable { variable: Variable },
    IsaTypeCouldNotBeUniquelyDetemrined { isa: Isa<Variable> },
    IsaStatementForRoleType { isa: Isa<Variable> },
    IsaTypeHadNoAnnotations { isa: Isa<Variable> },
    IsaTypeHasMultipleKinds { isa: Isa<Variable> },
    CouldNotDetermineTypeVariableSource { variable: Variable },
    CouldNotDetermineValueVariableSource { variable: Variable },
    CouldNotDetermineInsertedVariableSource { variable: Variable },
    MultipleTypeConstraintsForVariable { variable: Variable },
    CompoundExpressionsNotAllowed { variable: Variable },
    TODO__IllegalComparison { comparison: Comparison<Variable> },
    MultipleValuesForInsertableAttributeVariable { variable: Variable },
    TODO__IllegalState { msg: &'static str },
}

impl Display for InsertCompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for InsertCompilationError {}
