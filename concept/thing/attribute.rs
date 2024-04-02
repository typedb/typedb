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
use encoding::{AsBytes, graph::thing::vertex_attribute::AttributeVertex, value::value_type::ValueType};
use encoding::graph::type_::vertex::{build_vertex_attribute_type, TypeVertex};
use encoding::graph::Typed;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{iterator::SnapshotRangeIterator, SnapshotError},
};

use crate::{
    ByteReference,
    concept_iterator,
    ConceptAPI,
    error::{ConceptError, ConceptErrorKind, ConceptReadError}, thing::{thing_manager::ThingManager, value::Value},
};
use crate::type_::attribute_type::AttributeType;

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

    pub fn value<D>(&self, thing_manager: &ThingManager<'_, '_, D>) -> Result<Value, ConceptReadError> {
        thing_manager.get_attribute_value(self)
    }

    pub fn get_owners<'m, D>(&self, thing_manager: &'m ThingManager<'_, '_, D>)  {
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
