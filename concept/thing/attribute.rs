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

use std::cmp::Ordering;

use bytes::Bytes;
use encoding::{
    graph::{thing::vertex_attribute::AttributeVertex, type_::vertex::build_vertex_attribute_type, Typed},
    value::value_type::ValueType,
    AsBytes,
};
use storage::key_value::StorageKeyReference;
use storage::snapshot::ReadableSnapshot;

use crate::{
    concept_iterator,
    error::ConceptReadError,
    thing::{thing_manager::ThingManager, value::Value},
    type_::attribute_type::AttributeType,
    ByteReference, ConceptAPI,
};

#[derive(Clone, Debug)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
    value: Option<Value>, // TODO: if we end up doing traversals over Vertex instead of Concept, we could embed the Value cache into the AttributeVertex
}

impl<'a> Attribute<'a> {
    pub(crate) fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex, value: None }
    }

    pub(crate) fn value_type(&self) -> ValueType {
        self.vertex.value_type()
    }

    pub fn type_(&self) -> AttributeType<'static> {
        AttributeType::new(build_vertex_attribute_type(self.vertex.type_id()))
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn value(&self, thing_manager: &ThingManager<'_, impl ReadableSnapshot>) -> Result<Value, ConceptReadError> {
        thing_manager.get_attribute_value(self)
    }

    pub fn get_owners<'m>(&self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>) {
        // -> ObjectIterator<'m, 1>
        todo!()
    }

    pub(crate) fn vertex<'this: 'a>(&'this self) -> AttributeVertex<'this> {
        self.vertex.as_reference()
    }

    fn into_owned(self) -> Attribute<'static> {
        Attribute::new(self.vertex.into_owned())
    }
}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> PartialEq<Self> for Attribute<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.vertex().eq(&other.vertex())
    }
}

impl<'a> Eq for Attribute<'a> {}

impl<'a> PartialOrd<Self> for Attribute<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Attribute<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.vertex.cmp(&other.vertex())
    }
}

fn storage_key_to_attribute<'a>(storage_key_ref: StorageKeyReference<'a>) -> Attribute<'a> {
    Attribute::new(AttributeVertex::new(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(AttributeIterator, Attribute, storage_key_to_attribute);
