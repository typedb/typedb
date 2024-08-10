/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use answer::Thing;
use compiler::write::{ThingSource, TypeSource, ValueSource};
use compiler::write::write_instructions::{
    Has, PutAttribute, PutEntity, PutRelation, RolePlayer
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

fn get_type<'a>(input: &'a Row<'a>, source: &'a TypeSource) -> &'a answer::Type {
    match source {
        TypeSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_type(),
        TypeSource::TypeConstant(type_) => type_,
    }
}

fn get_thing<'a>(
    input: &'a Row<'a>,
    inserted_concepts: &'a Vec<answer::Thing<'static>>,
    source: &'a ThingSource,
) -> &'a answer::Thing<'static> {
    match source {
        ThingSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_thing(),
        ThingSource::InsertedThing(offset) => inserted_concepts.get(*offset).unwrap(),
    }
}

fn get_value<'a>(input: &'a Row<'a>, source: &'a ValueSource) -> &'a Value<'static> {
    match source {
        ValueSource::InputVariable(position) => input.get(VariablePosition::new(*position)).as_value(),
        ValueSource::ValueConstant(value) => value,
    }
}

//
// impl<'a, 'row> WriteExecutionContext<'a, 'row> {
//     pub(crate) fn new(
//         input: &'a Row<'row>,
//         created_things: &'a mut Vec<answer::Thing<'static>>,
//     ) -> WriteExecutionContext<'a, 'row> {
//         // TODO: Should we keep an attribute cache to avoid re-creating attributes with the same value?
//         WriteExecutionContext { input, created_things }
//     }
//
//     fn get_input(&self, position: &usize) -> &VariableValue<'static> {
//         todo!()
//     }
//
//     fn get_value(&self, at: &ValueSource) -> &Value<'static> {
//         match at {
//             ValueSource::ValueConstant(index) => self.value_constants.get(*index).unwrap(),
//             ValueSource::Input(position) => match self.get_input(position) {
//                 VariableValue::Value(value) => value,
//                 _ => unreachable!(),
//             },
//         }
//     }
//
//     fn get_type(&self, at: &TypeSource) -> &answer::Type {
//         match at {
//             TypeSource::TypeConstant(index) => self.type_constants.get(*index).unwrap(),
//             TypeSource::Input(position) => match self.get_input(position) {
//                 VariableValue::Type(type_) => type_,
//                 _ => unreachable!(),
//             },
//         }
//     }
//
//     fn get_thing(&self, at: &ThingSource) -> &answer::Thing<'static> {
//         match at {
//             ThingSource::Inserted(index) => self.created_things.get(*index).unwrap(),
//             ThingSource::Input(position) => match self.get_input(position) {
//                 VariableValue::Thing(thing) => thing,
//                 _ => unreachable!(),
//             },
//         }
//     }
//
//     fn todo__get_thing_list(&self, at: ()) -> &VariableValue<'static> {
//         todo!("???")
//     }
// }

pub trait AsInsertInstruction {
    // fn check(&self, snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager, context: &mut WriteExecutionContext<'_, '_>) -> Result<(), CheckError>;
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError>;
}

type DeleteError = (); // TODO
pub trait AsDeleteInstruction {
    fn delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &mut Vec<answer::Thing<'static>>,
    ) -> Result<(), DeleteError>;
}

type UpdateError = (); // TODO
pub trait AsUpdateInstruction: AsInsertInstruction + AsDeleteInstruction {
    fn update(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &mut Vec<answer::Thing<'static>>,
    ) -> Result<(), UpdateError>;
}

// Implementation
impl AsInsertInstruction for PutEntity {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        _inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError> {
        let entity_type = try_unwrap_as!(answer::Type::Entity: get_type(input, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_entity(snapshot, entity_type.clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(Some(Thing::Entity(inserted)))
    }
}

impl AsInsertInstruction for PutAttribute {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        _inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError> {
        let attribute_type = try_unwrap_as!(answer::Type::Attribute: get_type(input, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_attribute(snapshot, attribute_type.clone(), get_value(input, &self.value).clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(Some(Thing::Attribute(inserted)))
    }
}

impl AsInsertInstruction for PutRelation {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        _inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError> {
        let relation_type = try_unwrap_as!(answer::Type::Relation: get_type(input, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_relation(snapshot, relation_type.clone())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(Some(Thing::Relation(inserted)))
    }
}

impl AsInsertInstruction for Has {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError> {
        let owner_thing = get_thing(input, inserted_concepts, &self.owner);
        let attribute = get_thing(input, inserted_concepts, &self.attribute);
        owner_thing
            .as_object()
            .set_has_unordered(snapshot, thing_manager, attribute.as_attribute())
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(None)
    }
}

impl AsInsertInstruction for RolePlayer {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &Vec<answer::Thing<'static>>,
    ) -> Result<Option<Thing<'static>>, InsertError> {
        let relation_thing =
            try_unwrap_as!(answer::Thing::Relation : get_thing(input, inserted_concepts, &self.relation)).unwrap();
        let player_thing = get_thing(input, inserted_concepts, &self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : get_type(input, &self.role)).unwrap();
        relation_thing
            .add_player(snapshot, thing_manager, role_type.clone(), player_thing)
            .map_err(|source| InsertError::ConceptWrite { source })?;
        Ok(None)
    }
}
//
// impl AsDeleteInstruction for PutEntity {
//     fn delete(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), DeleteError> {
//         todo!()
//     }
// }
//
// impl AsDeleteInstruction for PutRelation {
//     fn delete(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), DeleteError> {
//         todo!()
//     }
// }
//
// impl AsDeleteInstruction for PutAttribute {
//     fn delete(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), DeleteError> {
//         todo!()
//     }
// }
//
// impl AsDeleteInstruction for Has {
//     fn delete(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), DeleteError> {
//         todo!()
//     }
// }
//
// impl AsDeleteInstruction for RolePlayer {
//     fn delete(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), DeleteError> {
//         todo!()
//     }
// }
//
// impl AsUpdateInstruction for Has {
//     fn update(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), UpdateError> {
//         todo!()
//     }
// }
//
// impl AsUpdateInstruction for RolePlayer {
//     fn update(
//         &self,
//         snapshot: &mut impl WritableSnapshot,
//         thing_manager: &ThingManager,
//         context: &mut WriteExecutionContext<'_, '_>,
//     ) -> Result<(), UpdateError> {
//         todo!()
//     }
// }
