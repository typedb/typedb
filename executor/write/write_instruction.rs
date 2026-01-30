/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use answer::{variable_value::VariableValue, Thing, Type};
use compiler::executable::insert::{
    instructions::{PutAttribute, PutObject},
    ThingPosition, TypeSource, ValueSource,
};
use concept::thing::{object::ObjectAPI, thing_manager::ThingManager, ThingAPI};
use encoding::value::value::Value;
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

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

fn get_type(input: &Row<'_>, source: &TypeSource) -> answer::Type {
    match source {
        TypeSource::InputVariable(position) => input.get(*position).as_type(),
        &TypeSource::Constant(type_) => type_,
    }
}

fn get_thing<'a>(input: &'a Row<'a>, source: &ThingPosition) -> &'a answer::Thing {
    let ThingPosition(position) = source;
    input.get(*position).as_thing()
}

fn get_value<'a>(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    storage_counters: StorageCounters,
    input: &'a Row<'_>,
    parameters: &'a ParameterRegistry,
    source: &ValueSource,
) -> Result<Value<'a>, Box<WriteError>> {
    match source {
        &ValueSource::Variable(position) => match input.get(position) {
            VariableValue::Thing(Thing::Attribute(attribute)) => attribute
                .get_value(snapshot, thing_manager, storage_counters)
                .map_err(|typedb_source| Box::new(WriteError::ConceptRead { typedb_source })),
            VariableValue::Value(value) => Ok(value.as_reference()),
            _ => unreachable!("Expected value or attribute"),
        },
        ValueSource::Parameter(id) => Ok(parameters.value_unchecked(&id).as_reference()),
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
        storage_counters: StorageCounters,
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let attribute_type = try_unwrap_as!(answer::Type::Attribute: get_type(row, &self.type_)).unwrap();
        let inserted = thing_manager
            .create_attribute(
                snapshot,
                attribute_type,
                get_value(snapshot, thing_manager, storage_counters, row, parameters, &self.value)?.clone(),
            )
            .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
        let ThingPosition(write_to) = &self.write_to;
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
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let inserted = match get_type(row, &self.type_) {
            Type::Entity(entity_type) => {
                let inserted = thing_manager
                    .create_entity(snapshot, entity_type)
                    .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
                Thing::Entity(inserted)
            }
            Type::Relation(relation_type) => {
                let inserted = thing_manager
                    .create_relation(snapshot, relation_type)
                    .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
                Thing::Relation(inserted)
            }
            Type::Attribute(_) | Type::RoleType(_) => unreachable!(),
        };
        let ThingPosition(write_to) = &self.write_to;
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let owner_thing = get_thing(row, &self.owner);
        let attribute = get_thing(row, &self.attribute);
        owner_thing
            .as_object()
            .set_has_unordered(snapshot, thing_manager, attribute.as_attribute(), storage_counters)
            .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let relation_thing = try_unwrap_as!(answer::Thing::Relation : get_thing(row, &self.relation)).unwrap();
        let player_thing = get_thing(row, &self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : get_type(row, &self.role)).unwrap();
        relation_thing
            .add_player(snapshot, thing_manager, role_type, player_thing, storage_counters)
            .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::update::instructions::Has {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let owner = get_thing(row, &self.owner).as_object();
        let new_attribute = get_thing(row, &self.attribute).as_attribute();

        let mut old_attributes = owner
            .get_has_type_unordered(snapshot, thing_manager, new_attribute.type_(), &.., StorageCounters::DISABLED)
            .map_err(|err| WriteError::ConceptRead { typedb_source: err })?
            .take(2)
            .collect_vec()
            .into_iter();
        if let Some(old_attribute) = old_attributes.next() {
            match old_attribute {
                Ok((old_attribute, count)) => {
                    debug_assert_eq!(
                        count, 1,
                        "Can only update unordered has with card up to 1 (got count {count}). Update for lists!"
                    );
                    owner
                        .unset_has_unordered(snapshot, thing_manager, &old_attribute, storage_counters.clone())
                        .map_err(|typedb_source| Box::new(WriteError::ConceptWrite { typedb_source }))?;
                }
                Err(typedb_source) => return Err(Box::new(WriteError::ConceptRead { typedb_source })),
            }
        }
        debug_assert!(
            old_attributes.next().is_none(),
            "Can only update unordered has with card up to 1. Update for lists!"
        );

        owner
            .set_has_unordered(snapshot, thing_manager, new_attribute, storage_counters)
            .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
        Ok(())
    }
}

impl AsWriteInstruction for compiler::executable::update::instructions::Links {
    fn execute(
        &self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        _parameters: &ParameterRegistry,
        row: &mut Row<'_>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let relation = try_unwrap_as!(answer::Thing::Relation : get_thing(row, &self.relation)).unwrap();
        let new_player = get_thing(row, &self.player).as_object();
        let role_type = try_unwrap_as!(answer::Type::RoleType : get_type(row, &self.role)).unwrap();

        let mut old_players =
            relation.get_players_role_type(snapshot, thing_manager, role_type, StorageCounters::DISABLED);
        if let Some(old_player) = old_players.next() {
            match old_player {
                Ok(old_player) => {
                    relation
                        .remove_player_single(snapshot, thing_manager, role_type, old_player, storage_counters.clone())
                        .map_err(|typedb_source| Box::new(WriteError::ConceptWrite { typedb_source }))?;
                }
                Err(typedb_source) => return Err(Box::new(WriteError::ConceptRead { typedb_source })),
            }
        }
        debug_assert!(
            old_players.next().is_none(),
            "Can only update unordered links with card up to 1. Update for lists!"
        );

        relation
            .add_player(snapshot, thing_manager, role_type, new_player, storage_counters)
            .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let ThingPosition(position) = self.thing;
        let Some(thing) = row.get(position).get_thing().cloned() else { return Ok(()) };
        match thing {
            Thing::Entity(entity) => {
                entity
                    .delete(snapshot, thing_manager, storage_counters)
                    .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
            }
            Thing::Relation(relation) => {
                relation
                    .delete(snapshot, thing_manager, storage_counters)
                    .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
            }
            Thing::Attribute(attribute) => {
                attribute
                    .delete(snapshot, thing_manager, storage_counters)
                    .map_err(|typedb_source| WriteError::ConceptWrite { typedb_source })?;
            }
        }
        let ThingPosition(position) = &self.thing;
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        let attribute = get_thing(row, &self.attribute).as_attribute();
        let owner = get_thing(row, &self.owner).as_object();
        owner
            .unset_has_unordered(snapshot, thing_manager, attribute, storage_counters)
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<WriteError>> {
        // TODO: Lists
        let relation = get_thing(row, &self.relation).as_relation();
        let player = get_thing(row, &self.player).as_object();
        let answer::Type::RoleType(role_type) = get_type(row, &self.role) else { unreachable!() };
        relation
            .remove_player_single(snapshot, thing_manager, role_type, player, storage_counters)
            .map_err(|source| Box::new(WriteError::ConceptWrite { typedb_source: source }))
    }
}
