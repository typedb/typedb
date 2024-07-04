/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};
use concept::{
    thing::{attribute::Attribute, entity::Entity, object::Object, relation::Relation},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, ObjectTypeAPI,
    },
};
use encoding::value::value::Value;

pub mod answer_map;
pub mod variable;
pub mod variable_value;

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

    pub fn as_attribute_type(&self) -> AttributeType<'static> {
        match self {
            Type::Attribute(attribute) => attribute.clone().into_owned(),
            _ => panic!("Type is not an Attribute type."),
        }
    }
}

impl From<EntityType<'static>> for Type {
    fn from(value: EntityType<'static>) -> Self {
        Self::Entity(value)
    }
}

impl From<RelationType<'static>> for Type {
    fn from(value: RelationType<'static>) -> Self {
        Self::Relation(value)
    }
}

impl From<RoleType<'static>> for Type {
    fn from(value: RoleType<'static>) -> Self {
        Self::RoleType(value)
    }
}

impl From<AttributeType<'static>> for Type {
    fn from(value: AttributeType<'static>) -> Self {
        Self::Attribute(value)
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

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Entity(entity) => write!(f, "{}", entity),
            Type::Relation(relation) => write!(f, "{}", relation),
            Type::Attribute(attribute) =>write!(f, "{}", attribute),
            Type::RoleType(role) =>write!(f, "{}", role),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Thing<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
    Attribute(Attribute<'a>),
}

impl<'a> From<Object<'a>> for Thing<'a> {
    fn from(object: Object<'a>) -> Self {
        match object {
            Object::Entity(entity) => Thing::Entity(entity),
            Object::Relation(relation) => Thing::Relation(relation),
        }
    }
}

impl<'a> Display for Thing<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Thing::Entity(entity) => write!(f, "{}", entity),
            Thing::Relation(relation) => write!(f, "{}", relation),
            Thing::Attribute(attribute) => write!(f, "{}", attribute),
        }
    }
}
