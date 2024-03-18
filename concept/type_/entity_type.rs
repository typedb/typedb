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

use std::collections::HashSet;

use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{
    graph::type_::vertex::{new_vertex_entity_type, TypeVertex},
    layout::prefix::PrefixType,
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use storage::{key_value::StorageKeyReference, snapshot::iterator::SnapshotPrefixIterator};

use crate::{
    concept_iterator,
    error::{ConceptError, ConceptErrorKind},
    type_::{
        annotation::AnnotationAbstract, attribute_type::AttributeType, object_type::ObjectType, owns::Owns,
        type_manager::TypeManager, EntityTypeAPI, OwnerAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> EntityType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> EntityType {
        if vertex.prefix() != PrefixType::VertexEntityType {
            panic!(
                "Type IID prefix was expected to be Prefix::EntityType ({:?}) but was {:?}",
                PrefixType::VertexEntityType,
                vertex.prefix()
            )
        }
        EntityType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for EntityType<'a> {}

impl<'a> TypeAPI<'a> for EntityType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> EntityTypeAPI<'a> for EntityType<'a> {
    fn into_owned(self) -> EntityType<'static> {
        EntityType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for EntityType<'a> {
    fn _construct_owns(&self, attribute_type: AttributeType<'static>) -> Owns<'static> {
        Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type)
    }

    fn get_owns<'m>(&self, type_manager: &'m TypeManager) -> MaybeOwns<'m, HashSet<Owns<'static>>> {
        type_manager.get_entity_type_owns(self.clone().into_owned())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum EntityTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<AnnotationAbstract> for EntityTypeAnnotation {
    fn from(annotation: AnnotationAbstract) -> Self {
        EntityTypeAnnotation::Abstract(annotation)
    }
}

// impl<'a> IIDAPI<'a> for EntityType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_ref_to_entity_type(storage_key_ref: StorageKeyReference<'_>) -> EntityType<'_> {
    EntityType::new(new_vertex_entity_type(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_ref_to_entity_type);
