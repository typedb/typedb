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
use storage::{key_value::StorageKeyReference, snapshot::iterator::SnapshotRangeIterator};
use storage::snapshot::error::SnapshotError;

use crate::{
    concept_iterator,
    ConceptAPI,
    error::{ConceptError, ConceptErrorKind},
    thing::{ObjectAPI, RelationAPI, ThingAPI},
};
use crate::ByteReference;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Relation<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        Relation { vertex }
    }
}

impl<'a> ConceptAPI<'a> for Relation<'a> {}

impl<'a> ThingAPI<'a> for Relation<'a> {}

impl<'a> ObjectAPI<'a> for Relation<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a> {
        &self.vertex
    }
}

impl<'a> RelationAPI<'a> for Relation<'a> {
    fn into_owned(self) -> Relation<'static> {
        Relation { vertex: self.vertex.into_owned() }
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Relation<'_> {
    Relation::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(RelationIterator, Relation, storage_key_ref_to_entity);
