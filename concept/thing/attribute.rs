/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;

use bytes::Bytes;
use encoding::{
    AsBytes,
    graph::{thing::vertex_attribute::AttributeVertex, type_::vertex::build_vertex_attribute_type, Typed},
    Keyable, value::value_type::ValueType,
};
use encoding::graph::thing::edge::ThingEdgeHasReverse;
use encoding::value::decode_value_u64;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{ByteReference, concept_iterator, ConceptAPI, ConceptStatus, edge_iterator, error::{ConceptReadError, ConceptWriteError}, GetStatus, thing::{thing_manager::ThingManager, ThingAPI, value::Value}, type_::attribute_type::AttributeType};
use crate::thing::object::Object;

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

    pub fn has_owners<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> bool {
        match self.get_status(thing_manager) {
            ConceptStatus::Put => thing_manager.has_owners(self.as_reference(), false),
            ConceptStatus::Inserted | ConceptStatus::Persisted | ConceptStatus::Deleted => {
                unreachable!("Attributes are expected to always have a PUT status.")
            }
        }
    }

    pub fn get_owners<'m>(
        &self, thing_manager: &'m ThingManager<impl ReadableSnapshot>,
    ) -> AttributeOwnerIterator<'m, { ThingEdgeHasReverse::LENGTH_BOUND_PREFIX_FROM }> {
        thing_manager.get_owners_of(self.as_reference())
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
    fn set_modified(&self, thing_manager: &ThingManager<impl WritableSnapshot>) {
        // Attributes are always PUT, so we don't have to record a lock on modification
    }

    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus {
        debug_assert_eq!(thing_manager.get_status(self.vertex().as_storage_key()), ConceptStatus::Put);
        ConceptStatus::Put
    }

    fn delete<'m>(self, thing_manager: &'m ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError> {
        let mut owner_iter = self.get_owners(thing_manager);
        let mut owner = owner_iter.next().transpose()
            .map_err(|err| ConceptWriteError::ConceptRead { source : err })?;
        while let Some((object, count)) = owner {
            object.delete_has_many(thing_manager, self.as_reference(), count)?;
            owner = owner_iter.next().transpose()
                .map_err(|err|  ConceptWriteError::ConceptRead { source : err })?;
        }

        thing_manager.delete_attribute(self);
        Ok(())
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

fn storage_key_to_owner<'a>(
    storage_key_reference: StorageKeyReference<'a>,
    value: ByteReference<'a>,
) -> (Object<'a>, u64) {
    let edge = ThingEdgeHasReverse::new(Bytes::Reference(storage_key_reference.byte_ref()));
    (Object::new(edge.into_to()), decode_value_u64(value))
}

edge_iterator!(
    AttributeOwnerIterator;
    (Object<'_>, u64);
    storage_key_to_owner
);