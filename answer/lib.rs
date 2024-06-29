/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    thing::{attribute::Attribute, entity::Entity, relation::Relation},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, ObjectTypeAPI,
    },
};
use concept::thing::object::Object;
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
}

impl From<ObjectType<'static>> for Type {
    fn from(type_: ObjectType<'static>) -> Self {
        match type_ {
            ObjectType::Entity(entity) => Type::Entity(entity),
            ObjectType::Relation(relation) => Type::Relation(relation),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
