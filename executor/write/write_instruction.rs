/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use answer::{Thing, Type};
use compiler::insert::{
    instructions::{PutAttribute, PutObject},
    ThingSource, TypeSource, ValueSource,
};
use concept::thing::{
    object::{Object, ObjectAPI},
    thing_manager::ThingManager,
    ThingAPI,
};
use encoding::value::value::Value;
use storage::snapshot::WritableSnapshot;

use crate::{batch::Row, write::insert_executor::WriteError, VariablePosition};

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
    inserted_concepts: &'a [answer::Thing<'static>],
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

pub trait AsInsertInstruction {
    // fn check(&self, snapshot: &mut impl WritableSnapshot, thing_manager: &ThingManager, context: &mut WriteExecutionContext<'_, '_>) -> Result<(), CheckError>;
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &[answer::Thing<'static>],
    ) -> Result<Option<Thing<'static>>, WriteError>;
}

pub trait AsDeleteInstruction {
    fn delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
    ) -> Result<(), WriteError>;
}

// Implementation
impl AsInsertInstruction for PutAttribute {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        _inserted_concepts: &[answer::Thing<'static>],
    ) -> Result<Option<Thing<'static>>, WriteError> {
        let attribute_type = try_unwrap_as!(answer::Type::Attribute: get_type(input, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_attribute(snapshot, attribute_type.clone(), get_value(input, &self.value).clone())
            .map_err(|source| WriteError::ConceptWrite { source })?;
        Ok(Some(Thing::Attribute(inserted)))
    }
}

impl AsInsertInstruction for PutObject {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        _inserted_concepts: &[answer::Thing<'static>],
    ) -> Result<Option<Thing<'static>>, WriteError> {
        match get_type(input, &self.type_) {
            Type::Entity(entity_type) => {
                let inserted = thing_manager
                    .create_entity(snapshot, entity_type.clone())
                    .map_err(|source| WriteError::ConceptWrite { source })?;
                Ok(Some(Thing::Entity(inserted)))
            }
            Type::Relation(relation_type) => {
                let inserted = thing_manager
                    .create_relation(snapshot, relation_type.clone())
                    .map_err(|source| WriteError::ConceptWrite { source })?;
                Ok(Some(Thing::Relation(inserted)))
            }
            Type::Attribute(_) | Type::RoleType(_) => unreachable!(),
        }
    }
}

impl AsInsertInstruction for compiler::insert::instructions::Has {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &[answer::Thing<'static>],
    ) -> Result<Option<Thing<'static>>, WriteError> {
        let owner_thing = get_thing(input, inserted_concepts, &self.owner);
        let attribute = get_thing(input, inserted_concepts, &self.attribute);
        owner_thing
            .as_object()
            .set_has_unordered(snapshot, thing_manager, attribute.as_attribute())
            .map_err(|source| WriteError::ConceptWrite { source })?;
        Ok(None)
    }
}

impl AsInsertInstruction for compiler::insert::instructions::RolePlayer {
    fn insert(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
        inserted_concepts: &[answer::Thing<'static>],
    ) -> Result<Option<Thing<'static>>, WriteError> {
        let relation_thing =
            try_unwrap_as!(answer::Thing::Relation : get_thing(input, inserted_concepts, &self.relation)).unwrap();
        let player_thing = get_thing(input, inserted_concepts, &self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : get_type(input, &self.role)).unwrap();
        relation_thing
            .add_player(snapshot, thing_manager, role_type.clone(), player_thing)
            .map_err(|source| WriteError::ConceptWrite { source })?;
        Ok(None)
    }
}

impl AsDeleteInstruction for compiler::delete::instructions::DeleteThing {
    fn delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
    ) -> Result<(), WriteError> {
        let thing = get_thing(input, &[], &self.thing).clone();
        match thing {
            Thing::Entity(entity) => {
                entity.delete(snapshot, thing_manager).map_err(|source| WriteError::ConceptWrite { source })
            }
            Thing::Relation(relation) => {
                relation.delete(snapshot, thing_manager).map_err(|source| WriteError::ConceptWrite { source })
            }
            Thing::Attribute(attribute) => {
                attribute.delete(snapshot, thing_manager).map_err(|source| WriteError::ConceptWrite { source })
            }
        }
    }
}

impl AsDeleteInstruction for compiler::delete::instructions::Has {
    fn delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
    ) -> Result<(), WriteError> {
        // TODO: Lists
        let attribute = get_thing(input, &[], &self.attribute).as_attribute();
        let owner = get_thing(input, &[], &self.owner).as_object();
        owner
            .unset_has_unordered(snapshot, thing_manager, attribute)
            .map_err(|source| WriteError::ConceptWrite { source })
    }
}

impl AsDeleteInstruction for compiler::delete::instructions::RolePlayer {
    fn delete(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'_>,
    ) -> Result<(), WriteError> {
        // TODO: Lists
        let Object::Relation(relation) = get_thing(input, &[], &self.relation).as_object() else { unreachable!() };
        let player = get_thing(input, &[], &self.relation).as_object();
        let answer::Type::RoleType(role_type) = get_type(input, &self.role) else { unreachable!() };
        relation
            .remove_player_single(snapshot, thing_manager, role_type.clone(), player)
            .map_err(|source| WriteError::ConceptWrite { source })
    }
}
