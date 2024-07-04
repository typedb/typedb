/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::{graph::type_::edge::TypeEdgeEncoding, layout::prefix::Prefix};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationCategory, AnnotationDistinct, DefaultFrom},
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{validation::AnnotationError, TypeManager},
        InterfaceImplementation,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Relates<'a> {
    relation: RelationType<'a>,
    role: RoleType<'a>,
}

impl<'a> Relates<'a> {
    pub(crate) fn new(relation: RelationType<'a>, role: RoleType<'a>) -> Self {
        Relates { relation, role }
    }

    pub fn relation(&self) -> RelationType<'a> {
        self.relation.clone()
    }

    pub fn role(&self) -> RoleType<'a> {
        self.role.clone()
    }

    pub fn set_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        overridden: Relates<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_relates_overridden(snapshot, self.clone().into_owned(), overridden)
    }

    pub fn unset_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_relates_overridden(snapshot, self.clone().into_owned())
    }

    pub fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        let ordering = self.role.get_ordering(snapshot, type_manager)?;
        let card = annotations
            .iter()
            .filter_map(|(annotation, _)| match annotation {
                RelatesAnnotation::Cardinality(card) => Some(*card),
                _ => None,
            })
            .next()
            .unwrap_or_else(|| type_manager.role_default_cardinality(ordering));
        Ok(card)
    }

    pub fn get_annotations_declared<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelatesAnnotation>>, ConceptReadError> {
        type_manager.get_relates_annotations_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<RelatesAnnotation, Relates<'static>>>, ConceptReadError> {
        type_manager.get_relates_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: RelatesAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RelatesAnnotation::Distinct(_) => {
                type_manager.set_relates_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            RelatesAnnotation::Cardinality(cardinality) => {
                type_manager.set_edge_annotation_cardinality(snapshot, self.clone().into_owned(), cardinality)?
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
        let relates_annotation = RelatesAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Conversion { source })?;
        match relates_annotation {
            RelatesAnnotation::Distinct(_) => {
                type_manager.unset_edge_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            RelatesAnnotation::Cardinality(_) => {
                type_manager.unset_edge_annotation_cardinality(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    fn into_owned(self) -> Relates<'static> {
        Relates { relation: self.relation.into_owned(), role: self.role.into_owned() }
    }
}

impl<'a> TypeEdgeEncoding<'a> for Relates<'a> {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeRelates;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeRelatesReverse;
    type From = RelationType<'a>;
    type To = RoleType<'a>;

    fn from_vertices(from: RelationType<'a>, to: RoleType<'a>) -> Self {
        Self::new(from, to)
    }

    fn canonical_from(&self) -> Self::From {
        self.relation()
    }

    fn canonical_to(&self) -> Self::To {
        self.role()
    }
}

impl<'a> InterfaceImplementation<'a> for Relates<'a> {
    type AnnotationType = RelatesAnnotation;
    type ObjectType = RelationType<'a>;
    type InterfaceType = RoleType<'a>;

    fn object(&self) -> RelationType<'a> {
        self.relation.clone()
    }

    fn interface(&self) -> RoleType<'a> {
        self.role.clone()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RelatesAnnotation {
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for Result<RelatesAnnotation, AnnotationError> {
    fn from(annotation: Annotation) -> Result<RelatesAnnotation, AnnotationError> {
        match annotation {
            Annotation::Distinct(annotation) => Ok(RelatesAnnotation::Distinct(annotation)),
            Annotation::Cardinality(annotation) => Ok(RelatesAnnotation::Cardinality(annotation)),

            Annotation::Abstract(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
            Annotation::Independent(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
            Annotation::Unique(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
            Annotation::Key(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
            Annotation::Regex(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
            Annotation::Cascade(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
        }
    }
}

impl From<Annotation> for RelatesAnnotation {
    fn from(annotation: Annotation) -> Self {
        let into_annotation: Result<RelatesAnnotation, AnnotationError> = annotation.into();
        match into_annotation {
            Ok(into_annotation) => into_annotation,
            Err(_) => unreachable!("Do not call this conversion from user-exposed code!"),
        }
    }
}

impl Into<Annotation> for RelatesAnnotation {
    fn into(self) -> Annotation {
        match self {
            RelatesAnnotation::Distinct(annotation) => Annotation::Distinct(annotation),
            RelatesAnnotation::Cardinality(annotation) => Annotation::Cardinality(annotation),
        }
    }
}
