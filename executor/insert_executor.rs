use std::cell::Cell;
use crate::executor::VariablePosition;

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::{Thing, Type};
use concept::type_::type_manager::TypeManager;
use storage::snapshot::WritableSnapshot;
use answer::variable::Variable;
use answer::variable_value::VariableValue;
use concept::error::ConceptWriteError;
use concept::thing::thing_manager::ThingManager;
use encoding::value::value::Value;
use ir::pattern::constraint::{Constraint, Isa};
use crate::executor::batch::Row;

// TODO: Move to utils
macro_rules! filter_variants {
    ($variant:path : $iterable:expr) => {
        $iterable.iter().filter_map(|item| if let $variant(inner) = item { Some(inner) } else { None })
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
    Entity { type_: TypeSource },
    Attribute { type_: TypeSource, value: ValueSource },
    Relation { type_: TypeSource },
    Has {owner: ThingSource, attribute: ThingSource },
    RolePlayer { relation: ThingSource, player: ThingSource, role: TypeSource } ,
}

struct ExecutionConcepts<'a, 'row> {
    input: &'a Row<'row>,
    type_constants: &'a Vec<answer::Type>,
    value_constants: &'a Vec<Value<'static>>,
    created_things: &'a mut Vec<answer::Thing<'static>>,
}

impl<'a, 'row> ExecutionConcepts<'a, 'row> {
    fn new(input: &'a Row<'row>, type_constants: &'a Vec<answer::Type>, value_constants: &'a Vec<Value<'static>>, created_things: &'a mut Vec<answer::Thing<'static>>) -> ExecutionConcepts<'a, 'row> {
        // TODO: Should we keep an attribute cache to avoid re-creating attributes with the same value?
        ExecutionConcepts { input, type_constants, value_constants, created_things }
    }

    fn get_input(&self, position: VariablePosition) -> &VariableValue<'static> {
        todo!()
    }

     fn get_value(&self, at: ValueSource) -> &Value<'static> {
         match at {
             ValueSource::ValueConstant(index) => self.value_constants.get(index).unwrap(),
             ValueSource::Input(position) => {
                 match self.get_input(position) {
                     VariableValue::Value(value) => value,
                     _ => unreachable!()
                 }
             }
         }
     }

    fn get_type(&self, at: TypeSource) -> &answer::Type {
        match at {
            TypeSource::TypeConstant(index) => self.type_constants.get(index).unwrap(),
            TypeSource::Input(position) => {
                match self.get_input(position) {
                    VariableValue::Type(type_) => type_,
                    _ => unreachable!()
                }
            }
        }
    }

    fn get_thing(&self, at: ThingSource) -> &answer::Thing<'static> {
        match at {
            ThingSource::Inserted(index) => self.created_things.get(index).unwrap(),
            ThingSource::Input(position) => {
                match self.get_input(position) {
                    VariableValue::Thing(thing) => thing,
                    _ => unreachable!()
                }
            }
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

    reused_created_things: Vec<VariableValue<'static>>, // internal mutability is cleaner
}


impl InsertExecutor {
    pub fn new(
        instructions: Vec<InsertInstruction>,
        type_constants: Vec<answer::Type>,
        value_constants: Vec<Value<'static>>,
        n_inserted_concepts: usize
    ) -> Self {
        Self { instructions, type_constants, value_constants, reused_created_things: Vec::with_capacity(n_inserted_concepts)}
    }
}

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    insert: &InsertExecutor,
    input: Row
) -> Result<Row, InsertError> {
    let new_concepts = create_new_concepts_for_isa_constraints(snapshot, thing_manager, insert, &input)?;
    Ok(todo!("Create new row from input & created concepts according to what's needed"))
}

fn create_new_concepts_for_isa_constraints(
    snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager,
    insert: &InsertExecutor, input: &Row
) -> Result<HashMap<Variable, VariableValue>, InsertError> {
    filter_variants!(Constraint::Isa: &insert.constraints).map(|isa| {
        create_new_concepts_for_single_isa(snapshot, type_manager, thing_manager, insert,&input, isa)
            .map(|thing| VariableValue::Thing(thing))
    }).collect()
}



fn create_new_concepts_for_single_isa(
    snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager, thing_manager: &ThingManager,
    insert: &InsertExecutor, input: &Row, isa: &Isa<Variable>,
) -> Result<Thing, InsertError> {
    debug_assert!(
        insert.input_vars_to_positions.contains_key(&isa.thing()) && !insert.input_vars_to_positions.contains_key(&isa.thing()),
        "Should be caught at pipeline construction"
    );

    let type_ = insert.types_for_isa.get(&isa.type_()).unwrap().clone();
    match type_ {
        Type::Entity(entity_type) => {
            thing_manager.create_entity(snapshot, entity_type).map(|entity| Thing::Entity(entity))
        },
        Type::Relation(relation_type) => {
            thing_manager.create_relation(snapshot, relation_type).map(|relation| Thing::Relation(relation))
        },
        Type::Attribute(attribute_type) => {
            let value = if let Some(value) = insert.constants.get(&isa.thing()) {
                value
            } else {
                insert.try_get_from_input(input, &isa.thing()).as_value().unwrap()
            };
            thing_manager.create_attribute(snapshot, attribute_type, value.clone())
                .map(|attribute| Thing::Attribute(attribute))
        },
        Type::RoleType(_) => unreachable!("Roles can't have an isa in an insert clause"),
    }.map_err(|source| InsertError::ConceptWrite { source })
}

fn create_capability() -> Result<(), InsertError>

pub enum InsertError {
    ConceptWrite { source: ConceptWriteError },
}

