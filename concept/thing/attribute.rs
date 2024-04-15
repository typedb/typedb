/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::Bytes;
use encoding::{
    graph::{thing::vertex_attribute::AttributeVertex, type_::vertex::build_vertex_attribute_type, Typed},
    value::value_type::ValueType,
    AsBytes, Keyable,
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{thing_manager::ThingManager, value::Value, ThingAPI},
    type_::attribute_type::AttributeType,
    ByteReference, ConceptAPI, ConceptStatus, GetStatus,
};

#[derive(Debug)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
    value: Option<Value<'a>>, // TODO: if we end up doing traversals over Vertex instead of Concept, we could embed the Value cache into the AttributeVertex
}

impl<'a> Attribute<'a> {
    pub(crate) fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex, value: None }
    }

    pub(crate) fn value_type(&self) -> ValueType {
        self.vertex.value_type()
    }

    pub fn type_(&self) -> AttributeType<'static> {
        AttributeType::new(build_vertex_attribute_type(self.vertex.type_id_()))
    }

    pub fn iid(&self) -> ByteReference<'_> {
        self.vertex.bytes()
    }

    pub fn value(
        &mut self,
        thing_manager: &ThingManager<impl ReadableSnapshot>,
    ) -> Result<Value<'_>, ConceptReadError> {
        if self.value.is_none() {
            let value = thing_manager.get_attribute_value(self)?;
            self.value = Some(value);
        }
        Ok(self.value.as_ref().unwrap().as_reference())
    }

    pub fn get_owners<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) {
        // -> ObjectIterator<'m, 1>
        todo!()
    }

    pub fn as_reference(&self) -> Attribute<'_> {
        Attribute { vertex: self.vertex.as_reference(), value: self.value.as_ref().map(|value| value.as_reference()) }
    }

    pub(crate) fn vertex<'this: 'a>(&'this self) -> AttributeVertex<'this> {
        self.vertex.as_reference()
    }

    pub(crate) fn into_vertex(self) -> AttributeVertex<'a> {
        self.vertex
    }

    pub(crate) fn into_owned(self) -> Attribute<'static> {
        Attribute::new(self.vertex.into_owned())
    }
}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> ThingAPI<'a> for Attribute<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<'_, impl WritableSnapshot>) {
        // Attributes are always PUT, so we don't have to record a lock on modification
    }

    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus {
        thing_manager.get_status(self.vertex().as_storage_key())
    }

    fn delete<'m>(self, thing_manager: &'m ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError> {
        todo!()
    }
}

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
