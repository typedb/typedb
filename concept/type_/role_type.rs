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


use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::graph::type_::vertex::{new_vertex_role_type, TypeVertex};
use encoding::layout::prefix::PrefixType;
use storage::{key_value::StorageKeyReference, snapshot::iterator::SnapshotRangeIterator};
use storage::snapshot::error::SnapshotError;

use crate::{
    concept_iterator,
    ConceptAPI,
    error::{ConceptError, ConceptErrorKind},
    type_::{
        annotation::{Annotation, AnnotationAbstract},
        RelationTypeAPI, TypeAPI,
    },
};
use bytes::byte_reference::ByteReference;
use encoding::Prefixed;
use crate::type_::RoleTypeAPI;


#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RoleType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RoleType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RoleType<'_> {
        if vertex.prefix() != PrefixType::VertexRoleType {
            panic!(
                "Type IID prefix was expected to be Prefix::RoleType ({:?}) but was {:?}",
                PrefixType::VertexRoleType,
                vertex.prefix()
            )
        }
        RoleType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for RoleType<'a> {}

impl<'a> TypeAPI<'a> for RoleType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> RoleTypeAPI<'a> for RoleType<'a> {
    fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}


#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<Annotation> for RoleTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => {
                RoleTypeAnnotation::Abstract(annotation)
            }
        }
    }
}

// impl<'a> IIDAPI<'a> for RoleType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_role_type(storage_key_ref: StorageKeyReference<'_>) -> RoleType<'_> {
    RoleType::new(new_vertex_role_type(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RoleTypeIterator, RoleType, storage_key_to_role_type);
