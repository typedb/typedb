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

use bytes::Bytes;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::layout::prefix::Prefix;
use encoding::Prefixed;
use storage::key_value::StorageKeyReference;
use crate::concept_iterator;
use crate::error::ConceptWriteError;
use crate::thing::attribute::Attribute;
use crate::thing::entity::Entity;
use crate::thing::relation::Relation;
use crate::thing::thing_manager::ThingManager;

#[derive(Debug, Eq, PartialEq)]
pub enum Object<'a> {
    Entity(Entity<'a>),
    Relation(Relation<'a>),
}

impl<'a> Object<'a> {

    pub(crate) fn new(object_vertex: ObjectVertex<'a>) -> Self {
        match object_vertex.prefix() {
            Prefix::VertexEntity => Object::Entity(Entity::new(object_vertex)),
            Prefix::VertexRelation => Object::Relation(Relation::new(object_vertex)),
            _ => unreachable!("Object creation requires either Entity or Relation vertex."),
        }
    }

    pub(crate) fn as_reference<'this>(&'this self) -> Object<'this> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.as_reference()),
            Object::Relation(relation) => Object::Relation(relation.as_reference()),
        }
    }

    fn set_has<D>(&self, thing_manager: &ThingManager<'_, '_, D>, attribute: &Attribute<'_>)
                  -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => entity.set_has(thing_manager, attribute),
            Object::Relation(relation) => relation.set_has(thing_manager, attribute),
        }
    }

    pub fn vertex(&self) -> ObjectVertex<'_> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }
}
