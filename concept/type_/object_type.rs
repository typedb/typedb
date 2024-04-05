/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::HashSet;

use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, owns::Owns, plays::Plays, relation_type::RelationType,
        role_type::RoleType, type_manager::TypeManager, OwnerAPI, PlayerAPI,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ObjectType<'a> {
    Entity(EntityType<'a>),
    Relation(RelationType<'a>),
}

impl<'a> OwnerAPI<'a> for ObjectType<'a> {
    fn set_owns(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) {
        // TODO: decide behaviour (ok or error) if already owning
        match self {
            ObjectType::Entity(entity) => entity.set_owns(type_manager, attribute_type),
            ObjectType::Relation(relation) => relation.set_owns(type_manager, attribute_type),
        }
    }

    fn delete_owns(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, attribute_type: AttributeType<'static>) {
        match self {
            ObjectType::Entity(entity) => entity.delete_owns(type_manager, attribute_type),
            ObjectType::Relation(relation) => relation.delete_owns(type_manager, attribute_type),
        }
    }

    fn get_owns<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_owns(type_manager),
            ObjectType::Relation(relation) => relation.get_owns(type_manager),
        }
    }

    fn get_owns_attribute(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_owns_attribute(type_manager, attribute_type),
            ObjectType::Relation(relation) => relation.get_owns_attribute(type_manager, attribute_type),
        }
    }
}

impl<'a> PlayerAPI<'a> for ObjectType<'a> {
    fn set_plays(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        role_type: RoleType<'static>,
    ) {
        match self {
            ObjectType::Entity(entity) => entity.set_plays(type_manager, role_type),
            ObjectType::Relation(relation) => relation.set_plays(type_manager, role_type),
        }
    }

    fn delete_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>) {
        match self {
            ObjectType::Entity(entity) => entity.delete_plays(type_manager, role_type),
            ObjectType::Relation(relation) => relation.delete_plays(type_manager, role_type),
        }
    }

    fn get_plays<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_plays(type_manager),
            ObjectType::Relation(relation) => relation.get_plays(type_manager),
        }
    }

    fn get_plays_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        match self {
            ObjectType::Entity(entity) => entity.get_plays_role(type_manager, role_type),
            ObjectType::Relation(relation) => relation.get_plays_role(type_manager, role_type),
        }
    }
}
