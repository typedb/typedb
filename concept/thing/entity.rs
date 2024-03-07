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

use crate::{ConceptAPI, concept_iterator};
use crate::error::{ConceptError, ConceptErrorKind};
use crate::thing::{ObjectAPI, ThingAPI};
use crate::type_::TypeAPI;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        Entity { vertex: vertex }
    }

    pub fn into_owned(self) -> Entity<'static> {
        Entity { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Entity<'a> {}

impl<'a> ThingAPI<'a> for Entity<'a> {}

impl<'a> ObjectAPI<'a> for Entity<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a> {
        &self.vertex
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_entity<'a>(storage_key_ref: StorageKeyReference<'a>) -> Entity<'a> {
    Entity::new(ObjectVertex::new(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(EntityIterator, Entity, storage_key_to_entity);
