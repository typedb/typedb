/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::{byte_reference::ByteReference, Bytes};
use encoding::{
    graph::thing::{edge::ThingEdgeHas, vertex_object::ObjectVertex},
    layout::prefix::Prefix,
    value::decode_value_u64,
    Prefixed,
};
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    edge_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{
        attribute::Attribute, entity::Entity, relation::Relation, thing_manager::ThingManager, value::Value, ObjectAPI,
        ThingAPI,
    },
    type_::{attribute_type::AttributeType, object_type::ObjectType, owns::Owns, type_manager::TypeManager, ObjectTypeAPI},
    ConceptStatus,
};

#[derive(Debug, Clone, Eq, PartialEq)]
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

    pub(crate) fn as_reference(&self) -> Object<'_> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.as_reference()),
            Object::Relation(relation) => Object::Relation(relation.as_reference()),
        }
    }

    pub fn unwrap_entity(self) -> Entity<'a> {
        match self {
            Self::Entity(entity) => entity,
            Self::Relation(relation) => panic!("called `Object::unwrap_entity()` on a `Relation` value: {relation:?}"),
        }
    }

    pub fn unwrap_relation(self) -> Relation<'a> {
        match self {
            Self::Relation(relation) => relation,
            Self::Entity(entity) => panic!("called `Object::unwrap_relation()` on an `Entity` value: {entity:?}"),
        }
    }

    pub fn type_(&self) -> ObjectType<'static> {
        match self {
            Object::Entity(entity) => ObjectType::Entity(entity.type_()),
            Object::Relation(relation) => ObjectType::Relation(relation.type_()),
        }
    }

    pub fn vertex(&self) -> ObjectVertex<'_> {
        match self {
            Object::Entity(entity) => entity.vertex(),
            Object::Relation(relation) => relation.vertex(),
        }
    }

    pub fn into_owned(self) -> Object<'static> {
        match self {
            Object::Entity(entity) => Object::Entity(entity.into_owned()),
            Object::Relation(relation) => Object::Relation(relation.into_owned()),
        }
    }
}

impl<'a> ThingAPI<'a> for Object<'a> {
    fn set_modified<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) {
        match self {
            Object::Entity(entity) => entity.set_modified(snapshot, thing_manager),
            Object::Relation(relation) => relation.set_modified(snapshot, thing_manager),
        }
    }

    fn get_status<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &'m ThingManager<Snapshot>,
    ) -> ConceptStatus {
        match self {
            Object::Entity(entity) => entity.get_status(snapshot, thing_manager),
            Object::Relation(relation) => relation.get_status(snapshot, thing_manager),
        }
    }

    fn errors<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Vec<ConceptWriteError>, ConceptReadError> {
        match self {
            Object::Entity(entity) => entity.errors(snapshot, thing_manager),
            Object::Relation(relation) => relation.errors(snapshot, thing_manager),
        }
    }

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<(), ConceptWriteError> {
        match self {
            Object::Entity(entity) => entity.delete(snapshot, thing_manager),
            Object::Relation(relation) => relation.delete(snapshot, thing_manager),
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

    fn type_(&self) -> impl ObjectTypeAPI<'static> {
        self.type_()
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
