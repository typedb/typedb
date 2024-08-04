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
use concept::{
    error::ConceptWriteError,
    thing::{object::ObjectAPI, thing_manager::ThingManager},
    type_::{type_manager::TypeManager, Ordering},
};
use encoding::value::value::Value;
use ir::pattern::constraint::{Constraint, Isa};
use storage::snapshot::WritableSnapshot;

use crate::executor::{batch::Row, VariablePosition};

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

pub enum VariableSource {
    TypeConstant(TypeSource),
    ValueConstant(ValueSource),
    ThingSource(ThingSource),
}

pub enum TypeSource {
    Input(VariablePosition),
    TypeConstant(usize),
}

pub enum ValueSource {
    Input(VariablePosition),
    ValueConstant(usize),
}

pub enum ThingSource {
    Input(VariablePosition),
    Inserted(usize),
}

pub enum InsertInstruction {
    // TODO: Just replace this with regular `Constraint`s and use a mapped-row?
    Entity { type_: TypeSource },
    Attribute { type_: TypeSource, value: ValueSource },
    Relation { type_: TypeSource },
    Has { owner: ThingSource, attribute: ThingSource }, // TODO: Ordering
    RolePlayer { relation: ThingSource, player: ThingSource, role: TypeSource }, // TODO: Ordering
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

    fn get_input(&self, position: &VariablePosition) -> &VariableValue<'static> {
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
    instructions: Vec<InsertInstruction>,
    type_constants: Vec<answer::Type>,
    value_constants: Vec<Value<'static>>,

    reused_created_things: Vec<answer::Thing<'static>>, // internal mutability is cleaner

    output_row: Vec<VariableSource>,
}

impl InsertExecutor {
    pub fn new(
        instructions: Vec<InsertInstruction>,
        type_constants: Vec<answer::Type>,
        value_constants: Vec<Value<'static>>,
        n_inserted_concepts: usize,
    ) -> Self {
        let output_row = Vec::new(); // TODO
        Self {
            instructions,
            type_constants,
            value_constants,
            output_row,
            reused_created_things: Vec::with_capacity(n_inserted_concepts),
        }
    }
}

pub(crate) fn execute<'row>(
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    insert: &mut InsertExecutor,
    input: &Row<'row>,
) -> Result<(), InsertError> {
    let InsertExecutor { type_constants, value_constants, reused_created_things, .. } = insert;
    let context = ExecutionConcepts { input, type_constants, value_constants, created_things: reused_created_things };
    for instruction in &insert.instructions {
        match instruction {
            InsertInstruction::Entity { type_ } => {
                let entity_type = try_unwrap_as!(answer::Type::Entity: context.get_type(type_)).unwrap();
                thing_manager
                    .create_entity(snapshot, entity_type.clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
            }
            InsertInstruction::Attribute { type_, value } => {
                let attribute_type = try_unwrap_as!(answer::Type::Attribute: context.get_type(type_)).unwrap();
                thing_manager
                    .create_attribute(snapshot, attribute_type.clone(), context.get_value(value).clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
            }
            InsertInstruction::Relation { type_ } => {
                let relation_type = try_unwrap_as!(answer::Type::Relation: context.get_type(type_)).unwrap();
                thing_manager
                    .create_relation(snapshot, relation_type.clone())
                    .map_err(|source| InsertError::ConceptWrite { source })?;
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
