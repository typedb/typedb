/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    thing::{attribute::Attribute, entity::Entity, relation::Relation},
    type_::{attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType},
};
use concept::type_::object_type::ObjectType;
use concept::type_::ObjectTypeAPI;

pub mod answer_map;
pub mod variable_value;
pub mod variable;

#[derive(Debug, PartialEq)]
enum Concept<'a> {
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Type {
    Entity(EntityType<'static>),
    Relation(RelationType<'static>),
    Attribute(AttributeType<'static>),
    RoleType(RoleType<'static>),
}

impl Type {
    pub fn as_object_type(&self) -> ObjectType<'static> {
        match self {
            Type::Entity(entity) => entity.clone().into_owned_object_type(),
            Type::Relation(relation) => relation.clone().into_owned_object_type(),
            Type::Attribute(_) => panic!("Attribute Type is not an object type"),
            Type::RoleType(_) => panic!("Role Type is not an Object type"),
        }
    }
}

impl From<ObjectType<'static>> for Type {
    fn from(type_: ObjectType<'static>) -> Self {
        match type_ {
            ObjectType::Entity(entity) => Type::Entity(entity),
            ObjectType::Relation(relation) => Type::Relation(relation),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Thing<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
    Attribute(Attribute<'a>),
}
