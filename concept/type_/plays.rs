/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::{
    graph::type_::{edge::TypeEdgeEncoding, vertex::TypeVertexEncoding, CapabilityKind},
    layout::prefix::Prefix,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationCategory, AnnotationError, DefaultFrom},
        constraint::CapabilityConstraint,
        object_type::ObjectType,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, TypeAPI,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Plays {
    player: ObjectType,
    role: RoleType,
}

impl Hkt for Plays {
    type HktSelf<'a> = Plays;
}

impl Plays {
    pub const DEFAULT_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, None);

    pub fn player(&self) -> ObjectType {
        self.player
    }

    pub fn role(&self) -> RoleType {
        self.role
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: PlaysAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            PlaysAnnotation::Cardinality(cardinality) => type_manager.set_plays_annotation_cardinality(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
                cardinality,
            )?,
        }
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<ConceptWriteError>> {
        let plays_annotation = PlaysAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match plays_annotation {
            PlaysAnnotation::Cardinality(_) => {
                type_manager.unset_plays_annotation_cardinality(snapshot, thing_manager, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_capability_abstract_constraint(snapshot, self.clone().into_owned())
    }

    pub fn get_default_cardinality() -> AnnotationCardinality {
        Self::DEFAULT_CARDINALITY
    }

    pub(crate) fn into_owned(self) -> Plays {
        Plays { player: ObjectType::new(self.player.vertex()), role: self.role.into_owned() }
    }
}

impl<'a> TypeEdgeEncoding<'a> for Plays {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgePlays;
    const REVERSE_PREFIX: Prefix = Prefix::EdgePlaysReverse;
    type From = ObjectType;
    type To = RoleType;

    fn from_vertices(player: ObjectType, role: RoleType) -> Self {
        Plays { player, role }
    }

    fn canonical_from(&self) -> Self::From {
        self.player()
    }

    fn canonical_to(&self) -> Self::To {
        self.role()
    }
}

impl<'a> Capability<'a> for Plays {
    type AnnotationType = PlaysAnnotation;
    type ObjectType = ObjectType;
    type InterfaceType = RoleType;
    const KIND: CapabilityKind = CapabilityKind::Plays;

    fn new(player: ObjectType, role: RoleType) -> Self {
        Self { player, role }
    }

    fn object(&self) -> ObjectType {
        self.player
    }

    fn interface(&self) -> RoleType {
        self.role
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        let is_abstract = self.get_constraint_abstract(snapshot, type_manager)?.is_some();
        debug_assert!(!is_abstract, "Abstractness of plays is not implemented! Take care of validation");
        Ok(is_abstract)
    }

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<PlaysAnnotation>>, Box<ConceptReadError>> {
        type_manager.get_plays_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<CapabilityConstraint<Plays>>>, Box<ConceptReadError>>
    where
        'a: 'static,
    {
        type_manager.get_plays_constraints(snapshot, self.clone().into_owned())
    }

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_plays_cardinality_constraints(snapshot, self.clone().into_owned())
    }

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        type_manager.get_capability_cardinality(snapshot, self.clone().into_owned())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum PlaysAnnotation {
    Cardinality(AnnotationCardinality),
}

impl TryFrom<Annotation> for PlaysAnnotation {
    type Error = AnnotationError;
    fn try_from(annotation: Annotation) -> Result<PlaysAnnotation, AnnotationError> {
        match annotation {
            Annotation::Cardinality(annotation) => Ok(PlaysAnnotation::Cardinality(annotation)),

            | Annotation::Abstract(_)
            | Annotation::Independent(_)
            | Annotation::Distinct(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Regex(_)
            | Annotation::Cascade(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => Err(AnnotationError::UnsupportedAnnotationForPlays(annotation.category())),
        }
    }
}

impl From<PlaysAnnotation> for Annotation {
    fn from(val: PlaysAnnotation) -> Self {
        match val {
            PlaysAnnotation::Cardinality(annotation) => Annotation::Cardinality(annotation),
        }
    }
}

impl PartialEq<Annotation> for PlaysAnnotation {
    fn eq(&self, annotation: &Annotation) -> bool {
        match annotation {
            Annotation::Cardinality(other_cardinality) =>
            {
                #[allow(irrefutable_let_patterns, reason = "for consistency & extensibility")]
                if let Self::Cardinality(cardinality) = self {
                    cardinality == other_cardinality
                } else {
                    false
                }
            }
            Annotation::Abstract(_) => false,
            Annotation::Independent(_) => false,
            Annotation::Distinct(_) => false,
            Annotation::Unique(_) => false,
            Annotation::Key(_) => false,
            Annotation::Regex(_) => false,
            Annotation::Cascade(_) => false,
            Annotation::Range(_) => false,
            Annotation::Values(_) => false,
        }
    }
}
