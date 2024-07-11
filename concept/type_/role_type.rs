/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::{
        type_::{
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
            Kind,
        },
        Typed,
    },
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use super::Ordering;
use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationCategory, AnnotationError, DefaultFrom},
        object_type::ObjectType,
        plays::Plays,
        relates::Relates,
        type_manager::TypeManager,
        KindAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RoleType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RoleType<'a> {
    pub fn get_players_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_players<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type(snapshot, self.clone().into_owned())
    }

    pub fn get_relation<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Relates<'static>>, ConceptReadError> {
        type_manager.get_relates_for_role_type(snapshot, self.clone().into_owned())
    }

    pub fn get_relations_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relates_for_role_type_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_role_ordering(snapshot, self.clone().into_owned())
    }

    pub fn set_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_role_ordering(snapshot, self.clone(), ordering)
    }
}

impl Hkt for RoleType<'static> {
    type HktSelf<'a> = Self;
}

impl<'a> ConceptAPI<'a> for RoleType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for RoleType<'a> {
    const PREFIX: Prefix = Prefix::VertexRoleType;
}

impl<'a> TypeVertexEncoding<'a> for RoleType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == Prefix::VertexRoleType);
        if vertex.prefix() != Prefix::VertexRoleType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexRoleType, actual_prefix: vertex.prefix() })
        } else {
            Ok(RoleType { vertex })
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for RoleType<'a> {
    type SelfStatic = RoleType<'static>;

    fn new(vertex: TypeVertex<'a>) -> RoleType<'_> {
        Self::from_vertex(vertex).unwrap()
    }

    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        self.vertex.as_reference()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains_key(&RoleTypeAnnotation::Abstract(AnnotationAbstract)))
    }

    fn delete(self, snapshot: &mut impl WritableSnapshot, type_manager: &TypeManager) -> Result<(), ConceptWriteError> {
        type_manager.delete_role_type(snapshot, self.clone().into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_role_type_label(snapshot, self.clone().into_owned())
    }
}

impl<'a> RoleType<'a> {
    pub fn is_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_role_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn set_name(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        name: &str,
    ) -> Result<(), ConceptWriteError> {
        if self.is_root(snapshot, type_manager)? {
            Err(ConceptWriteError::RootModification) // TODO: Move into TypeManager?
        } else {
            type_manager.set_role_type_name(snapshot, self.clone().into_owned(), name)
        }
    }

    pub fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<RoleType<'_>>, ConceptReadError> {
        type_manager.get_role_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn get_supertypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<RoleTypeAnnotation, RoleType<'static>>>, ConceptReadError> {
        type_manager.get_role_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RoleTypeAnnotation>>, ConceptReadError> {
        type_manager.get_role_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: RoleTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.set_annotation_abstract(snapshot, self.clone().into_owned())?
            }
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        let role_type_annotation = RoleTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match role_type_annotation {
            RoleTypeAnnotation::Abstract(_) => {
                type_manager.unset_annotation_abstract(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub(crate) fn get_relates<'m>(
        &self,
        snapshot: &impl WritableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Relates<'static>>, ConceptReadError> {
        type_manager.get_role_type_relates(snapshot, self.clone().into_owned())
    }

    pub fn into_owned(self) -> RoleType<'static> {
        RoleType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> KindAPI<'a> for RoleType<'a> {
    type AnnotationType = RoleTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Role;
}

impl<'a> Display for RoleType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[RoleType:{}]", self.vertex.type_id_())
    }
}

// --- Played API ---
impl<'a> RoleType<'a> {
    pub fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<ObjectType<'static>, Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_for_role_type(snapshot, self.clone().into_owned())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RoleTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<Annotation> for Result<RoleTypeAnnotation, AnnotationError> {
    fn from(annotation: Annotation) -> Result<RoleTypeAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(RoleTypeAnnotation::Abstract(annotation)),

            | Annotation::Independent(_)
            | Annotation::Distinct(_)
            | Annotation::Cardinality(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Regex(_)
            | Annotation::Cascade(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => Err(AnnotationError::UnsupportedAnnotationForRoleType(annotation.category())),
        }
    }
}

impl From<Annotation> for RoleTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        let into_annotation: Result<RoleTypeAnnotation, AnnotationError> = annotation.into();
        match into_annotation {
            Ok(into_annotation) => into_annotation,
            Err(_) => unreachable!("Do not call this conversion from user-exposed code!"),
        }
    }
}

impl Into<Annotation> for RoleTypeAnnotation {
    fn into(self) -> Annotation {
        match self {
            RoleTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
        }
    }
}

// impl<'a> IIDAPI<'a> for RoleType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_role_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> RoleType<'_> {
    RoleType::read_from(storage_key.into_bytes())
}

concept_iterator!(RoleTypeIterator, RoleType, storage_key_to_role_type);
