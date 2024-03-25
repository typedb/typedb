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

use bytes::byte_reference::ByteReference;
use bytes::Bytes;
use encoding::graph::type_::vertex::{new_vertex_role_type, TypeVertex};
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use encoding::value::label::Label;
use primitive::maybe_owns::MaybeOwns;
use storage::{key_value::StorageKeyReference, snapshot::iterator::SnapshotRangeIterator};
use storage::snapshot::error::SnapshotError;

use crate::{
    concept_iterator,
    ConceptAPI,
    error::{ConceptError, ConceptErrorKind},
    type_::{annotation::{Annotation, AnnotationAbstract}, TypeAPI},
};
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::type_manager::TypeManager;

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

impl<'a> RoleType<'a> {
    pub fn is_root<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> bool {
        type_manager.get_role_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, Label<'static>> {
        type_manager.get_role_type_label(self.clone().into_owned())
    }

    fn set_name<D>(&self, type_manager: &TypeManager<'_, '_, D>, name: &str) {
        // // TODO: setLabel should fail is setting label on Root type
        // type_manager.set_storage_label(self.vertex().clone().into_owned(), label);

        todo!()
    }

    pub fn get_supertype<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> Option<RoleType<'static>> {
        type_manager.get_role_type_supertype(self.clone().into_owned())
    }

    fn set_supertype<D>(&self, type_manager: &TypeManager<'_, '_, D>, supertype: RoleType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, Vec<RoleType<'static>>> {
        type_manager.get_role_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<RoleType<'static>>>;

    pub fn get_annotations<'m, D>(
        &self,
        type_manager: &'m TypeManager<'_, '_, D>,
    ) -> MaybeOwns<'m, HashSet<RoleTypeAnnotation>> {
        type_manager.get_role_type_annotations(self.clone().into_owned())
    }

    pub(crate) fn set_annotation<D>(&self, type_manager: &TypeManager<'_, '_, D>, annotation: RoleTypeAnnotation) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation<D>(&self, type_manager: &TypeManager<'_, '_, D>, annotation: RoleTypeAnnotation) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
        }
    }

    fn get_relates<D>(&self, type_manager: &TypeManager<'_, '_, D>) -> Relates<'static> {
        todo!()
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    fn get_plays<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Plays<'static>>> {
        todo!()
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
    RoleType::new(new_vertex_role_type(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RoleTypeIterator, RoleType, storage_key_to_role_type);
