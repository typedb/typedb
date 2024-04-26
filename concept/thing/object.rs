/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_reference::ByteReference, Bytes};
use encoding::{
    graph::thing::{
        edge::ThingEdgeHas
        ,
        vertex_object::ObjectVertex,
    },
    layout::prefix::Prefix,
    Prefixed,
    value::decode_value_u64,
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::WritableSnapshot,
};
use storage::snapshot::ReadableSnapshot;

use crate::{ConceptStatus, edge_iterator, thing::{attribute::Attribute, entity::Entity, ObjectAPI, relation::Relation, thing_manager::ThingManager}};
use crate::error::{ConceptReadError, ConceptWriteError};
use crate::thing::ThingAPI;

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

    fn set_has(
        &self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>
    ) -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => entity.set_has_unordered(thing_manager, attribute),
            Object::Relation(relation) => relation.set_has_unordered(thing_manager, attribute),
        }
    }

    pub(crate) fn delete_has_many(
        &self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>, count: u64
    ) -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => {
                todo!()
                // entity.delete_has_many(thing_manager, attribute, count)
            },
            Object::Relation(relation) => {
                todo!()
                // relation.delete_has_many(thing_manager, attribute, count)
            }
        }
    }

    pub fn vertex(&self) -> ObjectVertex<'_> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    pub(crate) fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }
}

impl<'a> ThingAPI<'a> for Object<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<impl WritableSnapshot>) {
        match self {
            Object::Entity(entity) => entity.set_modified(thing_manager),
            Object::Relation(relation) => relation.set_modified(thing_manager),
        }
    }

    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus {
        match self {
            Object::Entity(entity) => entity.get_status(thing_manager),
            Object::Relation(relation) => relation.get_status(thing_manager),
        }
    }

    fn errors(&self, thing_manager: &ThingManager<impl WritableSnapshot>) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        match self {
            Object::Entity(entity) => entity.errors(thing_manager),
            Object::Relation(relation) => relation.errors(thing_manager),
        }
    }

    fn delete(self, thing_manager: &ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => entity.delete(thing_manager),
            Object::Relation(relation) => relation.delete(thing_manager),
        }
    }
}

impl<'a> ObjectAPI<'a> for Object<'a> {
    fn vertex<'this>(&'this self) -> ObjectVertex<'this> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    fn into_vertex(self) -> ObjectVertex<'a> {
        match self {
            Object::Entity(entity) => entity.into_vertex(),
            Object::Relation(relation) => relation.into_vertex(),
        }
    }
}

fn storage_key_to_has_attribute<'a>(
    storage_key_ref: StorageKeyReference<'a>,
    value: ByteReference<'a>,
) -> (Attribute<'a>, u64) {
    let edge = ThingEdgeHas::new(Bytes::Reference(storage_key_ref.byte_ref()));
    (Attribute::new(edge.into_to()), decode_value_u64(value))
}

edge_iterator!(
    HasAttributeIterator;
    (Attribute<'_>, u64);
    storage_key_to_has_attribute
);
