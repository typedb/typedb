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

use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::graph::thing::vertex::ObjectVertex;
use storage::key_value::StorageKeyReference;
use storage::snapshot::iterator::SnapshotPrefixIterator;

use crate::{Concept, concept_iterator, Object, Thing, Type};
use crate::error::{ConceptError, ConceptErrorKind};

#[derive(Debug, PartialEq, Eq)]
pub struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        Entity { vertex: vertex }
    }

    pub fn to_owned(&self) -> Entity<'static> {
        Entity { vertex: self.vertex.to_owned() }
    }
}

impl<'a> Concept<'a> for Entity<'a> {}

impl<'a> Thing<'a> for Entity<'a> {}

impl<'a> Object<'a> for Entity<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a> {
        &self.vertex
    }
}


// TODO: can we inline this into the macro invocation?
fn create_entity<'a>(storage_key_ref: StorageKeyReference<'a>) -> Entity<'a> {
    Entity::new(ObjectVertex::new(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(EntityIterator, Entity, create_entity);

