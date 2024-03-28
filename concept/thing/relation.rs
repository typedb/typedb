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
use encoding::graph::thing::edge::ThingEdgeHas;
use encoding::graph::thing::vertex_object::ObjectVertex;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{iterator::SnapshotRangeIterator, SnapshotError},
};

use crate::{
    concept_iterator,
    error::{ConceptError, ConceptErrorKind},
    ByteReference, ConceptAPI,
};
use crate::error::ConceptWriteError;
use crate::thing::attribute::{Attribute, AttributeIterator};
use crate::thing::thing_manager::ThingManager;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Relation<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Relation<'a> {
    pub fn new(vertex: ObjectVertex<'a>) -> Self {
        Relation { vertex }
    }

    pub fn set_has<D>(&self, thing_manager: &ThingManager<'_, '_, D>, attribute: &Attribute<'_>)
                      -> Result<(), ConceptWriteError> {
        thing_manager.set_storage_has(self.vertex(), attribute.vertex())
    }

    pub fn get_has<'m, D>(&self, thing_manager: &'m ThingManager<'_, '_, D>) -> AttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.get_storage_has(self.vertex())
    }

    pub(crate) fn vertex<'this: 'a>(&'this self) -> ObjectVertex<'this> {
        self.vertex.as_reference()
    }

    pub(crate) fn into_owned(self) -> Relation<'static> {
        Relation { vertex: self.vertex.into_owned() }
    }
}

impl<'a> ConceptAPI<'a> for Relation<'a> {}

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity(storage_key_ref: StorageKeyReference<'_>) -> Relation<'_> {
    Relation::new(ObjectVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}
concept_iterator!(RelationIterator, Relation, storage_key_ref_to_entity);
