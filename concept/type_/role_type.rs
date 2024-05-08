/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationDistinct},
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
    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }

    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&RoleTypeAnnotation::Abstract(AnnotationAbstract::new())))
    }

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<(), ConceptWriteError> {
        todo!()
    }
}

impl<'a> RoleType<'a> {
    pub fn is_root<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_role_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn get_label<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label(snapshot, self.clone().into_owned())
    }

    fn set_name<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        _type_manager: &TypeManager<Snapshot>,
        _name: &str,
    ) {
        // // TODO: setLabel should fail is setting label on Root type
        // type_manager.set_storage_label(self.clone().into_owned(), label);

        todo!()
    }

    pub fn get_supertype<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<RoleType<'_>>, ConceptReadError> {
        type_manager.get_role_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        supertype: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.storage_set_supertype(snapshot, self.clone().into_owned(), supertype);
        Ok(())
    }

    pub fn get_supertypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_cardinality<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        let card: AnnotationCardinality = annotations
            .iter()
            .filter_map(|annotation| match annotation {
                RoleTypeAnnotation::Cardinality(card) => Some(card.clone()),
                _ => None,
            })
            .next()
            .unwrap_or_else(|| type_manager.role_default_cardinality());
        Ok(card)
    }

    pub fn get_annotations<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<RoleTypeAnnotation>>, ConceptReadError> {
        type_manager.get_role_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: RoleTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.storage_set_annotation_abstract(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Distinct(_) => {
                type_manager.storage_set_annotation_distinct(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Cardinality(cardinality) => {
                type_manager.storage_set_annotation_cardinality(snapshot, self.clone().into_owned(), cardinality)
            }
        };
        Ok(())
    }

    fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: RoleTypeAnnotation,
    ) {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.storage_delete_annotation_abstract(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Distinct(_) => {
                type_manager.storage_delete_annotation_distinct(snapshot, self.clone().into_owned())
            }
            RoleTypeAnnotation::Cardinality(_) => {
                type_manager.storage_delete_annotation_cardinality(snapshot, self.clone().into_owned())
            }
        }
    }

    fn get_relates<Snapshot: ReadableSnapshot>(&self, _type_manager: &TypeManager<Snapshot>) -> Relates<'static> {
        todo!()
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    fn get_plays<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        _type_manager: &'m TypeManager<Snapshot>,
    ) -> MaybeOwns<'m, HashSet<Plays<'static>>> {
        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for RoleTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => RoleTypeAnnotation::Abstract(annotation),
            Annotation::Distinct(annotation) => RoleTypeAnnotation::Distinct(annotation),
            Annotation::Cardinality(annotation) => RoleTypeAnnotation::Cardinality(annotation),
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
