/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use answer::{variable_value::VariableValue, Thing};
use compiler::planner::write_instructions::{
    Has, IsaAttribute, IsaEntity, IsaRelation, RolePlayer, ThingSource, TypeSource, ValueSource, VariableSource,
};
use concept::thing::{object::ObjectAPI, thing_manager::ThingManager};
use encoding::value::value::Value;
use storage::snapshot::WritableSnapshot;

use crate::{batch::Row, write::insert_executor::InsertError, VariablePosition};

macro_rules! try_unwrap_as {
    ($variant:path : $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    };
}

pub(crate) struct WriteExecutionContext<'a, 'row> {
    input: &'a Row<'row>,
    type_constants: &'a Vec<answer::Type>,
    value_constants: &'a Vec<Value<'static>>,
    created_things: &'a mut Vec<answer::Thing<'static>>,
}

impl<'a, 'row> WriteExecutionContext<'a, 'row> {
    pub(crate) fn populate_output_row(&self, output_row_plan: &mut Vec<VariableSource>, output: &mut Row) {
        for (i, source) in output_row_plan.iter().enumerate() {
            let value = match source {
                VariableSource::TypeSource(s) => VariableValue::Type(self.get_type(s).clone()),
                VariableSource::ValueSource(s) => VariableValue::Value(self.get_value(s).clone()),
                VariableSource::ThingSource(s) => VariableValue::Thing(self.get_thing(s).clone()),
            };
            output.set(VariablePosition::new(i as u32), value)
        }
        output.set_multiplicity(1);
    }
}

impl<'a, 'row> WriteExecutionContext<'a, 'row> {
    pub(crate) fn new(
        input: &'a Row<'row>,
        type_constants: &'a Vec<answer::Type>,
        value_constants: &'a Vec<Value<'static>>,
        created_things: &'a mut Vec<answer::Thing<'static>>,
    ) -> WriteExecutionContext<'a, 'row> {
        // TODO: Should we keep an attribute cache to avoid re-creating attributes with the same value?
        WriteExecutionContext { input, type_constants, value_constants, created_things }
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

pub trait WriteInstruction {
    // fn check(&self, snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager, context: &mut WriteExecutionContext<'_, '_>) -> Result<(), CheckError>;
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError>;
    // fn delete(&self, snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager, context: &mut WriteExecutionContext<'_, '_>) -> Result<(), DeleteError>;
}

impl WriteInstruction for IsaEntity {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError> {
        let entity_type = try_unwrap_as!(answer::Type::Entity: context.get_type(&self.type_)).unwrap();
        let inserted = thing_manager
            .create_entity(snapshot, entity_type.clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        context.created_things.push(Thing::Entity(inserted));
        Ok(())
    }
}

impl WriteInstruction for IsaAttribute {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError> {
        let attribute_type = try_unwrap_as!(answer::Type::Attribute: context.get_type(&self.type_)).unwrap();
        let inserted = thing_manager
            .create_attribute(snapshot, attribute_type.clone(), context.get_value(&self.value).clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        context.created_things.push(Thing::Attribute(inserted));
        Ok(())
    }
}

impl WriteInstruction for IsaRelation {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError> {
        let relation_type = try_unwrap_as!(answer::Type::Relation: context.get_type(&self.type_)).unwrap();
        let inserted = thing_manager
            .create_relation(snapshot, relation_type.clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        context.created_things.push(Thing::Relation(inserted));
        Ok(())
    }
}

impl WriteInstruction for Has {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError> {
        let owner_thing = context.get_thing(&self.owner);
        let attribute = context.get_thing(&self.attribute);
        owner_thing
            .as_object()
            .set_has_unordered(snapshot, thing_manager, attribute.as_attribute())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(())
    }
}

impl WriteInstruction for RolePlayer {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        context: &mut WriteExecutionContext<'_, '_>,
    ) -> Result<(), InsertError> {
        let relation_thing = try_unwrap_as!(answer::Thing::Relation : context.get_thing(&self.relation)).unwrap();
        let player_thing = context.get_thing(&self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : context.get_type(&self.role)).unwrap();
        relation_thing
            .add_player(snapshot, thing_manager, role_type.clone(), player_thing)
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(())
    }
}
