/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::{
    graph::type_::{edge::TypeEdgeEncoding, CapabilityKind},
    layout::prefix::Prefix,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationDistinct,
            AnnotationError, DefaultFrom,
        },
        constraint::CapabilityConstraint,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, Ordering,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Relates {
    relation: RelationType,
    role: RoleType,
}

impl Hkt for Relates {
    type HktSelf<'a> = Relates;
}

impl Relates {
    pub const DEFAULT_UNORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, Some(1));
    pub const DEFAULT_ORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, None);

    pub fn relation(&self) -> RelationType {
        self.relation
    }

    pub fn role(&self) -> RoleType {
        self.role
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_capability_abstract_constraint(snapshot, self.clone().into_owned())
    }

    pub fn get_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relates_distinct_constraints(snapshot, self.clone().into_owned())
    }

    pub fn is_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(!self.get_constraints_distinct(snapshot, type_manager)?.is_empty())
    }

    pub fn set_specialise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        specialised: Relates,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_relates_specialise(snapshot, thing_manager, self.clone().into_owned(), specialised)
    }

    pub fn unset_specialise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_relates_specialise(snapshot, thing_manager, self.clone().into_owned())
    }

    pub fn is_specialising(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        type_manager.get_relates_is_specialising(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: RelatesAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            RelatesAnnotation::Abstract(_) => type_manager.set_relates_annotation_abstract(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
                true,
            )?,
            RelatesAnnotation::Distinct(_) => {
                type_manager.set_relates_annotation_distinct(snapshot, thing_manager, self.clone().into_owned())?
            }
            RelatesAnnotation::Cardinality(cardinality) => type_manager.set_relates_annotation_cardinality(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
                cardinality,
            )?,
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<ConceptWriteError>> {
        let relates_annotation = RelatesAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match relates_annotation {
            RelatesAnnotation::Abstract(_) => {
                type_manager.unset_relates_annotation_abstract(snapshot, self.clone().into_owned())?
            }
            RelatesAnnotation::Distinct(_) => {
                type_manager.unset_relates_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            RelatesAnnotation::Cardinality(_) => {
                type_manager.unset_relates_annotation_cardinality(snapshot, thing_manager, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn get_default_cardinality_for_non_specialising(role_ordering: Ordering) -> AnnotationCardinality {
        match role_ordering {
            Ordering::Unordered => Self::DEFAULT_UNORDERED_CARDINALITY,
            Ordering::Ordered => Self::DEFAULT_ORDERED_CARDINALITY,
        }
    }

    pub fn get_default_distinct(role_ordering: Ordering) -> Option<AnnotationDistinct> {
        match role_ordering {
            Ordering::Ordered => None,
            Ordering::Unordered => Some(AnnotationDistinct),
        }
    }

    pub(crate) fn into_owned(self) -> Relates {
        Relates { relation: self.relation.into_owned(), role: self.role.into_owned() }
    }
}

impl<'a> TypeEdgeEncoding<'a> for Relates {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeRelates;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeRelatesReverse;
    type From = RelationType;
    type To = RoleType;

    fn from_vertices(from: RelationType, to: RoleType) -> Self {
        Self::new(from, to)
    }

    fn canonical_from(&self) -> Self::From {
        self.relation()
    }

    fn canonical_to(&self) -> Self::To {
        self.role()
    }
}

impl<'a> Capability<'a> for Relates {
    type AnnotationType = RelatesAnnotation;
    type ObjectType = RelationType;
    type InterfaceType = RoleType;
    const KIND: CapabilityKind = CapabilityKind::Relates;

    fn new(relation: RelationType, role: RoleType) -> Self {
        Relates { relation, role }
    }

    fn object(&self) -> RelationType {
        self.relation
    }

    fn interface(&self) -> RoleType {
        self.role
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_constraint_abstract(snapshot, type_manager)?.is_some())
    }

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelatesAnnotation>>, Box<ConceptReadError>> {
        type_manager.get_relates_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Relates>>>, Box<ConceptReadError>>
    where
        'a: 'static,
    {
        type_manager.get_relates_constraints(snapshot, self.clone().into_owned())
    }

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relates_cardinality_constraints(snapshot, self.clone().into_owned())
    }

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        type_manager.get_relates_cardinality(snapshot, self.clone().into_owned())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RelatesAnnotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
}

impl TryFrom<Annotation> for RelatesAnnotation {
    type Error = AnnotationError;

    fn try_from(annotation: Annotation) -> Result<RelatesAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(RelatesAnnotation::Abstract(annotation)),
            Annotation::Distinct(annotation) => Ok(RelatesAnnotation::Distinct(annotation)),
            Annotation::Cardinality(annotation) => Ok(RelatesAnnotation::Cardinality(annotation)),

            | Annotation::Independent(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Regex(_)
            | Annotation::Cascade(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => Err(AnnotationError::UnsupportedAnnotationForRelates(annotation.category())),
        }
    }
}

impl From<RelatesAnnotation> for Annotation {
    fn from(anno: RelatesAnnotation) -> Self {
        match anno {
            RelatesAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
            RelatesAnnotation::Distinct(annotation) => Annotation::Distinct(annotation),
            RelatesAnnotation::Cardinality(annotation) => Annotation::Cardinality(annotation),
        }
    }
}
