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
use encoding::graph::thing::vertex_object::ObjectVertex;
use storage::{key_value::StorageKeyReference, snapshot::iterator::SnapshotPrefixIterator};

use crate::{
    concept_iterator,
    error::{ConceptError, ConceptErrorKind},
    thing::{EntityAPI, ObjectAPI, ThingAPI},
    ConceptAPI,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        Entity { vertex }
    }
}

impl<'a> ConceptAPI<'a> for Entity<'a> {}

impl<'a> ThingAPI<'a> for Entity<'a> {}

impl<'a> ObjectAPI<'a> for Entity<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a> {
        &self.vertex
    }
}

impl<'a> EntityAPI<'a> for Entity<'a> {
    fn into_owned(self) -> Entity<'static> {
        Entity { vertex: self.vertex.into_owned() }
    }
}

fn storage_key_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Entity<'_> {
    Entity::new(ObjectVertex::new(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(EntityIterator, Entity, storage_key_to_entity);
