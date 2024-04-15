/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use bytes::{byte_reference::ByteReference, Bytes};
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeRolePlayer},
            vertex_attribute::AttributeVertex,
            vertex_object::ObjectVertex,
        },
        type_::vertex::build_vertex_role_type,
    },
    layout::prefix::Prefix,
    value::decode_value_u64,
    Prefixed,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{
        iterator::{SnapshotIteratorError, SnapshotRangeIterator},
        WritableSnapshot,
    },
};

use crate::{
    error::ConceptReadError,
    thing::{attribute::Attribute, entity::Entity, relation::Relation, thing_manager::ThingManager, ObjectAPI},
    type_::role_type::RoleType,
};

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

    fn set_has(&self, thing_manager: &ThingManager<impl WritableSnapshot>, attribute: Attribute<'_>) {
        match self {
            Object::Entity(entity) => entity.set_has(thing_manager, attribute),
            Object::Relation(relation) => relation.set_has(thing_manager, attribute),
        }
    }

    pub fn vertex(&self) -> ObjectVertex<'_> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
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

pub struct HasAttributeIterator<'a, const S: usize> {
    snapshot_iterator: Option<SnapshotRangeIterator<'a, S>>,
}

impl<'a, const S: usize> HasAttributeIterator<'a, S> {
    pub(crate) fn new(snapshot_iterator: SnapshotRangeIterator<'a, S>) -> Self {
        Self { snapshot_iterator: Some(snapshot_iterator) }
    }

    pub(crate) fn new_empty() -> Self {
        Self { snapshot_iterator: None }
    }

    pub fn peek(&mut self) -> Option<Result<Attribute<'_>, ConceptReadError>> {
        self.iter_peek().map(|result| {
            result
                .map(|(storage_key, value)| {
                    let edge = ThingEdgeHas::new(Bytes::Reference(storage_key.byte_ref()));
                    Attribute::new(edge.into_to())
                })
                .map_err(|snapshot_error| ConceptReadError::SnapshotIterate { source: snapshot_error })
        })
    }

    // a lending iterator trait is infeasible with the current borrow checker
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Attribute<'_>, ConceptReadError>> {
        self.iter_next().map(|result| {
            result
                .map(|(storage_key, value)| {
                    let edge = ThingEdgeHas::new(Bytes::Reference(storage_key.byte_ref()));
                    Attribute::new(edge.into_to())
                })
                .map_err(|snapshot_error| ConceptReadError::SnapshotIterate { source: snapshot_error })
        })
    }

    pub fn seek(&mut self) {
        todo!()
    }

    fn iter_peek(
        &mut self,
    ) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.peek()
        } else {
            None
        }
    }

    fn iter_next(
        &mut self,
    ) -> Option<Result<(StorageKeyReference<'_>, ByteReference<'_>), Arc<SnapshotIteratorError>>> {
        if let Some(iter) = self.snapshot_iterator.as_mut() {
            iter.next()
        } else {
            None
        }
    }

    pub fn collect_cloned(mut self) -> Vec<Attribute<'static>> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            if item.is_none() {
                break;
            }
            let key = item.unwrap().unwrap().into_owned();
            vec.push(key);
        }
        vec
    }

    pub fn count(mut self) -> usize {
        let mut count = 0;
        let mut next = self.next();
        while next.is_some() {
            next = self.next();
            count += 1;
        }
        count
    }

    pub fn collect_cloned(mut self) -> Vec<Attribute<'static>> {
        let mut vec = Vec::new();
        loop {
            let item = self.next();
            if item.is_none() {
                break;
            }
            let key = item.unwrap().unwrap().into_owned();
            vec.push(key);
        }
        vec
    }
}
