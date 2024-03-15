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

use encoding::graph::thing::vertex_attribute::AttributeVertex;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::value::value_type::ValueType;

use crate::ConceptAPI;
use crate::thing::attribute::Attribute;
use crate::thing::entity::Entity;
use crate::thing::relation::Relation;
use crate::thing::thing_manager::ThingManager;
use crate::thing::value::Value;

pub mod attribute;
pub mod entity;
mod relation;
pub mod value;
pub mod thing_manager;

pub trait ThingAPI<'a>: ConceptAPI<'a> {}

pub trait ObjectAPI<'a>: ThingAPI<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a>;
}

pub trait EntityAPI<'a>: ObjectAPI<'a> {
    fn into_owned(self) -> Entity<'static>;
}

pub trait RelationAPI<'a>: ObjectAPI<'a> {
    fn into_owned(self) -> Relation<'static>;
}

pub trait AttributeAPI<'a>: ThingAPI<'a> {
    fn vertex(&self) -> &AttributeVertex<'a>;

    fn into_owned(self) -> Attribute<'static>;

    fn value_type(&self) -> ValueType;

    fn value(&self, thing_manager: &ThingManager) -> Value;
}
