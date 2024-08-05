/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    cell::Cell,
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display, Formatter},
};

use answer::{variable::Variable, variable_value::VariableValue, Thing, Type};
use compiler::planner::insert_planner::{InsertInstruction, InsertPlan, ThingSource, TypeSource, ValueSource};
use concept::{
    error::ConceptWriteError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{type_manager::TypeManager, Ordering},
};
use encoding::value::value::Value;
use ir::pattern::constraint::{Constraint, Isa};
use storage::snapshot::WritableSnapshot;

use crate::{batch::Row, VariablePosition};

// TODO: Move to utils
macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter.filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
    };
}
macro_rules! try_unwrap_as {
    ($variant:path : $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    };
}

struct ExecutionConcepts<'a, 'row> {
    input: &'a Row<'row>,
    type_constants: &'a Vec<answer::Type>,
    value_constants: &'a Vec<Value<'static>>,
    created_things: &'a mut Vec<answer::Thing<'static>>,
}

impl<'a, 'row> ExecutionConcepts<'a, 'row> {
    fn new(
        input: &'a Row<'row>,
        type_constants: &'a Vec<answer::Type>,
        value_constants: &'a Vec<Value<'static>>,
        created_things: &'a mut Vec<answer::Thing<'static>>,
    ) -> ExecutionConcepts<'a, 'row> {
        // TODO: Should we keep an attribute cache to avoid re-creating attributes with the same value?
        ExecutionConcepts { input, type_constants, value_constants, created_things }
    }

    fn get_input(&self, position: &usize) -> &VariableValue<'static> {
        todo!()
    }

    fn get_value(&self, at: &ValueSource) -> &Value<'static> {
        match at {
            ValueSource::ValueConstant(index) => self.value_constants.get(*index).unwrap(),
            ValueSource::Input(position) => match self.get_input(position) {
                VariableValue::Value(value) => value,
                _ => unreachable!(),
            },
        }
    }

    fn get_type(&self, at: &TypeSource) -> &answer::Type {
        match at {
            TypeSource::TypeConstant(index) => self.type_constants.get(*index).unwrap(),
            TypeSource::Input(position) => match self.get_input(position) {
                VariableValue::Type(type_) => type_,
                _ => unreachable!(),
            },
        }
    }

    fn get_thing(&self, at: &ThingSource) -> &answer::Thing<'static> {
        match at {
            ThingSource::Inserted(index) => self.created_things.get(*index).unwrap(),
            ThingSource::Input(position) => match self.get_input(position) {
                VariableValue::Thing(thing) => thing,
                _ => unreachable!(),
            },
        }
    }

    fn todo__get_thing_list(&self, at: ()) -> &VariableValue<'static> {
        todo!("???")
    }
}

pub struct InsertExecutor {
    plan: InsertPlan,
    reused_created_things: Vec<answer::Thing<'static>>, // internal mutability is cleaner
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        // let output_row = Vec::new(); // TODO
        let reused_created_things = Vec::with_capacity(plan.n_created_concepts);
        Self { plan, reused_created_things }
    }
}

pub fn execute<'row>(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    insert: &mut InsertExecutor,
    input: &Row<'row>,
) -> Result<(), InsertError> {
    let InsertExecutor { plan, reused_created_things } = insert;
    let InsertPlan { type_constants, value_constants, .. } = plan;

    let context = ExecutionConcepts { input, type_constants, value_constants, created_things: reused_created_things };
    for instruction in &plan.instructions {
        match instruction {
            InsertInstruction::Entity { type_ } => {
                let entity_type = try_unwrap_as!(answer::Type::Entity: context.get_type(type_)).unwrap();
                let inserted = thing_manager
                    .create_entity(snapshot, entity_type.clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
                context.created_things.push(Thing::Entity(inserted));
            }
            InsertInstruction::Attribute { type_, value } => {
                let attribute_type = try_unwrap_as!(answer::Type::Attribute: context.get_type(type_)).unwrap();
                let inserted = thing_manager
                    .create_attribute(snapshot, attribute_type.clone(), context.get_value(value).clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
                context.created_things.push(Thing::Attribute(inserted));
            }
            InsertInstruction::Relation { type_ } => {
                let relation_type = try_unwrap_as!(answer::Type::Relation: context.get_type(type_)).unwrap();
                let inserted = thing_manager
                    .create_relation(snapshot, relation_type.clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
                context.created_things.push(Thing::Relation(inserted));
            }
            InsertInstruction::Has { attribute, owner } => {
                let owner_thing = context.get_thing(owner);
                let attribute = context.get_thing(attribute);
                owner_thing
                    .as_object()
                    .set_has_unordered(snapshot, thing_manager, attribute.as_attribute())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
            }
            InsertInstruction::RolePlayer { relation, player, role } => {
                let relation_thing = try_unwrap_as!(answer::Thing::Relation : context.get_thing(relation)).unwrap();
                let player_thing = context.get_thing(player).as_object();
                let role_type = try_unwrap_as!(answer::Type::RoleType : context.get_type(role)).unwrap();
                relation_thing
                    .add_player(snapshot, thing_manager, role_type.clone(), player_thing)
                    .map_err(|source| InsertError::ConceptWrite { source })?;
            }
        }
    }
    Ok(()) // TODO: Create output row
}

#[derive(Debug, Clone)]
pub enum InsertError {
    ConceptWrite { source: ConceptWriteError },
}

impl Display for InsertError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for InsertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptWrite { source, .. } => Some(source),
        }
    }
}
