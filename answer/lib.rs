/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use bytes::{byte_array::ByteArray, Bytes};
use concept::{
    thing::{attribute::Attribute, entity::Entity, object::Object, relation::Relation},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, ObjectTypeAPI, TypeAPI,
    },
};
use encoding::{
    graph::type_::{vertex::TypeVertex, Kind},
    value::value::Value,
    AsBytes,
};

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
    pub fn kind(&self) -> Kind {
        match self {
            Type::Entity(_) => Kind::Entity,
            Type::Relation(_) => Kind::Relation,
            Type::Attribute(_) => Kind::Attribute,
            Type::RoleType(_) => Kind::Role,
        }
    }

    pub fn as_object_type(&self) -> ObjectType<'static> {
        match self {
            Type::Entity(entity) => entity.clone().into_owned_object_type(),
            Type::Relation(relation) => relation.clone().into_owned_object_type(),
            Type::Attribute(_) => panic!("Attribute Type is not an object type"),
            Type::RoleType(_) => panic!("Role Type is not an Object type"),
        }
    }

    pub fn as_relation_type(&self) -> RelationType<'static> {
        match self {
            Type::Relation(relation) => relation.clone().into_owned(),
            _ => panic!("Type is not an Relation type."),
        }
    }

    pub fn as_attribute_type(&self) -> AttributeType<'static> {
        match self {
            Type::Attribute(attribute) => attribute.clone().into_owned(),
            _ => panic!("Type is not an Attribute type."),
        }
    }

    pub fn as_role_type(&self) -> RoleType<'static> {
        match self {
            Type::RoleType(role) => role.clone().into_owned(),
            _ => panic!("Type is not an Role type."),
        }
    }

    pub fn next_possible(&self) -> Self {
        match self {
            Type::Entity(entity) => {
                let mut bytes = ByteArray::from(entity.vertex().bytes());
                bytes.increment().unwrap();
                Self::Entity(EntityType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::Relation(relation) => {
                let mut bytes = ByteArray::from(relation.vertex().bytes());
                bytes.increment().unwrap();
                Self::Relation(RelationType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::Attribute(attribute) => {
                let mut bytes = ByteArray::from(attribute.vertex().bytes());
                bytes.increment().unwrap();
                Self::Attribute(AttributeType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::RoleType(role) => {
                let mut bytes = ByteArray::from(role.vertex().bytes());
                bytes.increment().unwrap();
                Self::RoleType(RoleType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
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
            Type::Attribute(attribute) => write!(f, "{}", attribute),
            Type::RoleType(role) => write!(f, "{}", role),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Thing<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
    Attribute(Attribute<'a>),
}

impl<'a> Thing<'a> {
    pub fn type_(&self) -> Type {
        match self {
            Thing::Entity(entity) => Type::Entity(entity.type_()),
            Thing::Relation(relation) => Type::Relation(relation.type_()),
            Thing::Attribute(attribute) => Type::Attribute(attribute.type_()),
        }
    }

    pub fn as_object(&self) -> Object<'_> {
        match self {
            Thing::Entity(entity) => Object::Entity(entity.as_reference()),
            Thing::Relation(relation) => Object::Relation(relation.as_reference()),
            _ => panic!("Thing is not an Attribute."),
        }
    }

    pub fn as_relation(&self) -> Relation<'_> {
        match self {
            Thing::Relation(relation) => relation.as_reference(),
            _ => panic!("Thing is not an Attribute."),
        }
    }

    pub fn as_attribute(&self) -> Attribute<'_> {
        match self {
            Thing::Attribute(attribute) => attribute.as_reference(),
            _ => panic!("Thing is not an Attribute."),
        }
    }

    pub fn into_owned(self) -> Thing<'static> {
        match self {
            Thing::Entity(entity) => Thing::Entity(entity.into_owned()),
            Thing::Relation(relation) => Thing::Relation(relation.into_owned()),
            Thing::Attribute(attribute) => Thing::Attribute(attribute.into_owned()),
        }
    }

    pub fn next_possible(&self) -> Thing<'static> {
        match self {
            Thing::Entity(entity) => Thing::Entity(entity.next_possible()),
            Thing::Relation(relation) => Thing::Relation(relation.next_possible()),
            Thing::Attribute(attribute) => Thing::Attribute(attribute.next_possible()),
        }
    }
}

impl<'a> From<Object<'a>> for Thing<'a> {
    fn from(object: Object<'a>) -> Self {
        match object {
            Object::Entity(entity) => Thing::Entity(entity),
            Object::Relation(relation) => Thing::Relation(relation),
        }
    }
}

impl<'a> From<Entity<'a>> for Thing<'a> {
    fn from(entity: Entity<'a>) -> Self {
        Thing::Entity(entity)
    }
}

impl<'a> From<Relation<'a>> for Thing<'a> {
    fn from(relation: Relation<'a>) -> Self {
        Thing::Relation(relation)
    }
}

impl<'a> From<Attribute<'a>> for Thing<'a> {
    fn from(attribute: Attribute<'a>) -> Self {
        Self::Attribute(attribute)
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
