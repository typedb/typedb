/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::thing::attribute::Attribute;
use concept::thing::entity::Entity;
use concept::thing::relation::Relation;
use concept::thing::value::Value;
use concept::type_::attribute_type::AttributeType;
use concept::type_::entity_type::EntityType;
use concept::type_::relation_type::RelationType;
use concept::type_::role_type::RoleType;

pub mod answer_map;
pub mod variable_value;

enum Concept<'a> {
    Type(Type),
    Thing(Thing<'a>),
    Value(Value<'a>),
}

pub enum Type {
    Entity(EntityType<'static>),
    Relation(RelationType<'static>),
    Attribute(AttributeType<'static>),
    RoleType(RoleType<'static>),
}

enum Thing<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
    Attribute(Attribute<'a>),
}
