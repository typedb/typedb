/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{collections::BTreeSet, fmt};

use bytes::{byte_array::ByteArray, Bytes};
use concept::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, object::Object, relation::Relation},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, type_manager::TypeManager, ObjectTypeAPI, TypeAPI,
    },
};
use encoding::{
    graph::type_::{
        vertex::{TypeVertex, TypeVertexEncoding},
        Kind,
    },
    value::{label::Label, value::Value},
    AsBytes,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::ReadableSnapshot;

pub mod variable;
pub mod variable_value;

#[derive(Debug, PartialEq, Clone)]
pub enum Concept<'a> {
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
}

impl<'a> fmt::Display for Concept<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Concept::Type(type_) => write!(f, "{}", type_),
            Concept::Thing(thing) => write!(f, "{}", thing),
            Concept::Value(value) => write!(f, "{}", value),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Type {
    Entity(EntityType),
    Relation(RelationType),
    Attribute(AttributeType),
    RoleType(RoleType),
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

    pub fn get_label<'a>(
        &'a self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'a TypeManager,
    ) -> Result<MaybeOwns<'a, Label<'static>>, Box<ConceptReadError>> {
        match self {
            Type::Entity(entity) => entity.get_label(snapshot, type_manager),
            Type::Relation(relation) => relation.get_label(snapshot, type_manager),
            Type::Attribute(attribute) => attribute.get_label(snapshot, type_manager),
            Type::RoleType(role) => role.get_label(snapshot, type_manager),
        }
    }

    pub fn as_object_type(&self) -> ObjectType {
        match self {
            Type::Entity(entity) => (*entity).into_owned_object_type(),
            Type::Relation(relation) => (*relation).into_owned_object_type(),
            Type::Attribute(_) => panic!("Attribute Type is not an object type"),
            Type::RoleType(_) => panic!("Role Type is not an Object type"),
        }
    }

    pub fn as_entity_type(&self) -> EntityType {
        match self {
            Type::Entity(entity) => (*entity).into_owned(),
            _ => panic!("Type is not an Relation type."),
        }
    }

    pub fn as_relation_type(&self) -> RelationType {
        match self {
            Type::Relation(relation) => (*relation).into_owned(),
            _ => panic!("Type is not an Relation type."),
        }
    }

    pub fn as_attribute_type(&self) -> AttributeType {
        match self {
            Type::Attribute(attribute) => (*attribute).into_owned(),
            _ => panic!("Type is not an Attribute type."),
        }
    }

    pub fn as_role_type(&self) -> RoleType {
        match self {
            Type::RoleType(role) => (*role).into_owned(),
            _ => panic!("Type is not an Role type."),
        }
    }

    pub fn next_possible(&self) -> Self {
        match self {
            Type::Entity(entity) => {
                let mut bytes = ByteArray::from(&*entity.vertex().into_bytes());
                bytes.increment().unwrap();
                Self::Entity(EntityType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::Relation(relation) => {
                let mut bytes = ByteArray::from(&*relation.vertex().into_bytes());
                bytes.increment().unwrap();
                Self::Relation(RelationType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::Attribute(attribute) => {
                let mut bytes = ByteArray::from(&*attribute.vertex().into_bytes());
                bytes.increment().unwrap();
                Self::Attribute(AttributeType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
            Type::RoleType(role) => {
                let mut bytes = ByteArray::from(&*role.vertex().into_bytes());
                bytes.increment().unwrap();
                Self::RoleType(RoleType::new(TypeVertex::new(Bytes::Array(bytes))))
            }
        }
    }

    pub fn is_direct_subtype_of(
        &self,
        supertype: &Self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        if self.kind() != supertype.kind() || self == supertype {
            return Ok(false);
        }
        match supertype {
            Type::Entity(entity) => Ok(entity.get_subtypes(snapshot, type_manager)?.contains(&self.as_entity_type())),
            Type::Relation(relation) => {
                Ok(relation.get_subtypes(snapshot, type_manager)?.contains(&self.as_relation_type()))
            }
            Type::Attribute(attribute) => {
                Ok(attribute.get_subtypes(snapshot, type_manager)?.contains(&self.as_attribute_type()))
            }
            Type::RoleType(role) => Ok(role.get_subtypes(snapshot, type_manager)?.contains(&self.as_role_type())),
        }
    }

    pub fn is_transitive_subtype_of(
        &self,
        supertype: &Self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        if self.kind() != supertype.kind() {
            return Ok(false);
        }
        if self == supertype {
            return Ok(true);
        }
        match supertype {
            Type::Entity(entity) => {
                Ok(entity.get_subtypes_transitive(snapshot, type_manager)?.contains(&self.as_entity_type()))
            }
            Type::Relation(relation) => {
                Ok(relation.get_subtypes_transitive(snapshot, type_manager)?.contains(&self.as_relation_type()))
            }
            Type::Attribute(attribute) => {
                Ok(attribute.get_subtypes_transitive(snapshot, type_manager)?.contains(&self.as_attribute_type()))
            }
            Type::RoleType(role) => {
                Ok(role.get_subtypes_transitive(snapshot, type_manager)?.contains(&self.as_role_type()))
            }
        }
    }

    pub fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        match self {
            Type::Entity(entity) => entity.is_abstract(snapshot, type_manager),
            Type::Relation(relation) => relation.is_abstract(snapshot, type_manager),
            Type::Attribute(attribute) => attribute.is_abstract(snapshot, type_manager),
            Type::RoleType(role) => role.is_abstract(snapshot, type_manager),
        }
    }

    pub fn try_retain(
        annotations: &mut BTreeSet<Self>,
        predicate: impl Fn(&Self) -> Result<bool, Box<ConceptReadError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut to_be_removed = Vec::new();
        for annotation in annotations.iter() {
            if !predicate(annotation)? {
                to_be_removed.push(annotation.clone());
            }
        }
        for annotation in to_be_removed.iter() {
            annotations.remove(annotation);
        }
        Ok(())
    }
}

impl Hkt for Type {
    type HktSelf<'a> = Self;
}

impl From<EntityType> for Type {
    fn from(value: EntityType) -> Self {
        Self::Entity(value)
    }
}

impl From<RelationType> for Type {
    fn from(value: RelationType) -> Self {
        Self::Relation(value)
    }
}

impl From<RoleType> for Type {
    fn from(value: RoleType) -> Self {
        Self::RoleType(value)
    }
}

impl From<AttributeType> for Type {
    fn from(value: AttributeType) -> Self {
        Self::Attribute(value)
    }
}

impl From<ObjectType> for Type {
    fn from(type_: ObjectType) -> Self {
        match type_ {
            ObjectType::Entity(entity) => Type::Entity(entity),
            ObjectType::Relation(relation) => Type::Relation(relation),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            _ => panic!("Thing is not an Object."),
        }
    }

    pub fn as_relation(&self) -> Relation<'_> {
        match self {
            Thing::Relation(relation) => relation.as_reference(),
            _ => panic!("Thing is not an relation."),
        }
    }

    pub fn as_attribute(&self) -> Attribute<'_> {
        match self {
            Thing::Attribute(attribute) => attribute.as_reference(),
            _ => panic!("Thing is not an Attribute."),
        }
    }

    pub fn as_reference(&self) -> Thing<'_> {
        match self {
            Thing::Entity(entity) => Thing::Entity(entity.as_reference()),
            Thing::Relation(relation) => Thing::Relation(relation.as_reference()),
            Thing::Attribute(attribute) => Thing::Attribute(attribute.as_reference()),
        }
    }

    pub fn to_owned(&self) -> Thing<'static> {
        match self {
            Thing::Entity(entity) => Thing::Entity(entity.as_reference().into_owned()),
            Thing::Relation(relation) => Thing::Relation(relation.as_reference().into_owned()),
            Thing::Attribute(attribute) => Thing::Attribute(attribute.as_reference().into_owned()),
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

impl Hkt for Thing<'static> {
    type HktSelf<'a> = Thing<'a>;
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

impl<'a> fmt::Display for Thing<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Thing::Entity(entity) => write!(f, "{}", entity),
            Thing::Relation(relation) => write!(f, "{}", relation),
            Thing::Attribute(attribute) => write!(f, "{}", attribute),
        }
    }
}
