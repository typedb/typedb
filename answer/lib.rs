/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::{
    thing::{attribute::Attribute, entity::Entity, relation::Relation},
    type_::{attribute_type::AttributeType, entity_type::EntityType, relation_type::RelationType, role_type::RoleType},
};
use encoding::value::value::Value;

pub mod answer_map;
pub mod variable_value;

#[derive(Debug, PartialEq)]
enum Concept<'a> {
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Entity(EntityType<'static>),
    Relation(RelationType<'static>),
    Attribute(AttributeType<'static>),
    RoleType(RoleType<'static>),
}

#[derive(Debug, Clone, PartialEq)]
enum Thing<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
    Attribute(Attribute<'a>),
}
