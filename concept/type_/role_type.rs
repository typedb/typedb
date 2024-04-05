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

use bytes::Bytes;
use encoding::{
    graph::type_::vertex::{new_vertex_role_type, TypeVertex},
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationDuplicate},
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager,
        TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RoleType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RoleType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RoleType<'_> {
        debug_assert_eq!(vertex.prefix(), Prefix::VertexRoleType);
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
    pub fn is_root(&self, type_manager: &TypeManager<'_, impl ReadableSnapshot>) -> Result<bool, ConceptReadError> {
        type_manager.get_role_type_is_root(self.clone().into_owned())
    }

    pub fn get_label<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label(self.clone().into_owned())
    }

    fn set_name(&self, _type_manager: &TypeManager<'_, impl WritableSnapshot>, _name: &str) {
        // // TODO: setLabel should fail is setting label on Root type
        // type_manager.set_storage_label(self.vertex().clone().into_owned(), label);

        todo!()
    }

    pub fn get_supertype(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<Option<RoleType<'_>>, ConceptReadError> {
        type_manager.get_role_type_supertype(self.clone().into_owned())
    }

    fn set_supertype(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, supertype: RoleType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    pub fn get_supertypes<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<RoleType<'static>>>;

    pub fn get_annotations<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<RoleTypeAnnotation>>, ConceptReadError> {
        type_manager.get_role_type_annotations(self.clone().into_owned())
    }

    pub(crate) fn set_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: RoleTypeAnnotation) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
            RoleTypeAnnotation::Duplicate(_) => {
                type_manager.set_storage_annotation_duplicate(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: RoleTypeAnnotation) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
            RoleTypeAnnotation::Duplicate(_) => {
                type_manager.delete_storage_annotation_duplicate(self.vertex().clone().into_owned())
            }
        }
    }

    fn get_relates(&self, _type_manager: &TypeManager<'_, impl ReadableSnapshot>) -> Relates<'static> {
        todo!()
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    fn get_plays<'m>(&self, _type_manager: &'m TypeManager<'_, impl ReadableSnapshot>) -> MaybeOwns<'m, HashSet<Plays<'static>>> {
        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {
    Abstract(AnnotationAbstract),
    Duplicate(AnnotationDuplicate),
}

impl From<Annotation> for RoleTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => RoleTypeAnnotation::Abstract(annotation),
            Annotation::Duplicate(annotation) => RoleTypeAnnotation::Duplicate(annotation),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Role type."),
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
