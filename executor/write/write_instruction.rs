/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use answer::{variable_value::VariableValue, Thing, Type};
use compiler::executable::insert::{
    instructions::{PutAttribute, PutObject},
    ThingSource, TypeSource, ValueSource,
};
use concept::thing::{
    object::{Object, ObjectAPI},
    thing_manager::ThingManager,
    ThingAPI,
};
use encoding::value::value::Value;
use ir::pipeline::ParameterRegistry;
use storage::snapshot::WritableSnapshot;

use crate::{row::Row, write::WriteError};

macro_rules! try_unwrap_as {
    ($variant:path : $item:expr) => {
        if let $variant(inner) = $item {
            Some(inner)
        } else {
            None
        }
    };
}

fn get_type<'a>(input: &'a Row<'_>, source: &'a TypeSource) -> &'a answer::Type {
    match source {
        TypeSource::InputVariable(position) => input.get(*position).as_type(),
        TypeSource::Constant(type_) => type_,
    }
}

fn get_thing<'a>(input: &'a Row<'a>, source: &ThingSource) -> &'a answer::Thing<'a> {
    let ThingSource(position) = source;
    input.get(*position).as_thing()
}

fn get_value<'a>(input: &'a Row<'_>, parameters: &'a ParameterRegistry, source: ValueSource) -> Value<'a> {
    match source {
        ValueSource::Variable(position) => input.get(position).as_value().as_reference(),
        ValueSource::Parameter(id) => parameters.value_unchecked(id).as_reference(),
    }
}

pub trait AsWriteInstruction {
    // fn check(
    //     &self,
    //     snapshot: &mut impl WritableSnapshot,
    //     thing_manager: &ThingManager,
    //     context: &mut WriteExecutionContext<'_, '_>,
    // ) -> Result<(), CheckError>;

    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>>;
}

// Implementation
impl AsWriteInstruction for PutAttribute {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        let attribute_type = try_unwrap_as!(answer::Type::Attribute: get_type(row, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_attribute(snapshot, *attribute_type, get_value(row, parameters, self.value).clone())
            .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
        let ThingSource(write_to) = &self.write_to;
        row.set(*write_to, VariableValue::Thing(Thing::Attribute(inserted)));
        Ok(())
    }
}

impl AsWriteInstruction for PutObject {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        let inserted = match get_type(row, &self.type_) {
            Type::Entity(entity_type) => {
                let inserted = thing_manager
                    .create_entity(snapshot, *entity_type)
                    .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
                Thing::Entity(inserted)
            }
            Type::Relation(relation_type) => {
                let inserted = thing_manager
                    .create_relation(snapshot, *relation_type)
                    .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
                Thing::Relation(inserted)
            }
            Type::Attribute(_) | Type::RoleType(_) => unreachable!(),
        };
        let ThingSource(write_to) = &self.write_to;
        row.set(*write_to, VariableValue::Thing(inserted));
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::insert::instructions::Has {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        let owner_thing = get_thing(row, &self.owner);
        let attribute = get_thing(row, &self.attribute);
        owner_thing
            .as_object()
            .set_has_unordered(snapshot, thing_manager, attribute.as_attribute())
            .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::insert::instructions::Links {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        let relation_thing = try_unwrap_as!(answer::Thing::Relation : get_thing(row, &self.relation)).unwrap();
        let player_thing = get_thing(row, &self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : get_type(row, &self.role)).unwrap();
        relation_thing
            .add_player(snapshot, thing_manager, *role_type, player_thing)
            .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::delete::instructions::ThingInstruction {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        let thing = get_thing(row, &self.thing).clone();
        match thing {
            Thing::Entity(entity) => {
                entity
                    .delete(snapshot, thing_manager)
                    .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
            }
            Thing::Relation(relation) => {
                relation
                    .delete(snapshot, thing_manager)
                    .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
            }
            Thing::Attribute(attribute) => {
                attribute
                    .delete(snapshot, thing_manager)
                    .map_err(|source| WriteError::ConceptWrite { typedb_source: source })?;
            }
        }
        let ThingSource(position) = &self.thing;
        row.unset(*position);
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::delete::instructions::Has {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        // TODO: Lists
        let attribute = get_thing(row, &self.attribute).as_attribute();
        let owner = get_thing(row, &self.owner).as_object();
        owner
            .unset_has_unordered(snapshot, thing_manager, attribute)
            .map_err(|source| Box::new(WriteError::ConceptWrite { typedb_source: source }))
    }
}

impl AsWriteInstruction for compiler::executable::delete::instructions::Links {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
    ) -> Result<(), Box<WriteError>> {
        // TODO: Lists
        let Object::Relation(relation) = get_thing(row, &self.relation).as_object() else { unreachable!() };
        let player = get_thing(row, &self.relation).as_object();
        let answer::Type::RoleType(role_type) = get_type(row, &self.role) else { unreachable!() };
        relation
            .remove_player_single(snapshot, thing_manager, *role_type, player)
            .map_err(|source| Box::new(WriteError::ConceptWrite { typedb_source: source }))
    }
}
