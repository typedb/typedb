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
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationDistinct, AnnotationError, AnnotationKey,
            AnnotationRange, AnnotationRegex, AnnotationUnique, AnnotationValues, DefaultFrom,
        },
        attribute_type::AttributeType,
        constraint::CapabilityConstraint,
        object_type::ObjectType,
        type_manager::TypeManager,
        Capability, Ordering, TypeAPI,
    },
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Owns {
    owner: ObjectType,
    attribute: AttributeType,
}

impl Hkt for Owns {
    type HktSelf<'a> = Owns;
}

impl<'a> Owns {
    pub const DEFAULT_UNORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, Some(1));
    pub const DEFAULT_ORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, None);

    pub fn owner(&self) -> ObjectType {
        self.owner
    }

    pub fn attribute(&self) -> AttributeType {
        self.attribute.into_owned()
    }

    pub fn is_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        type_manager.get_is_key(snapshot, (*self).into_owned())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_capability_abstract_constraint(snapshot, (*self).into_owned())
    }

    pub fn get_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_owns_distinct_constraints(snapshot, (*self).into_owned())
    }

    pub fn is_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(!self.get_constraints_distinct(snapshot, type_manager)?.is_empty())
    }

    pub fn get_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_unique_constraint(snapshot, (*self).into_owned())
    }

    pub fn get_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_owns_regex_constraints(snapshot, (*self).into_owned())
    }

    pub fn get_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_owns_range_constraints(snapshot, (*self).into_owned())
    }

    pub fn get_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_owns_values_constraints(snapshot, (*self).into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: OwnsAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.set_owns_annotation_distinct(snapshot, thing_manager, (*self).into_owned())?
            }
            OwnsAnnotation::Key(_) => {
                type_manager.set_owns_annotation_key(snapshot, thing_manager, (*self).into_owned())?
            }
            OwnsAnnotation::Cardinality(cardinality) => type_manager.set_owns_annotation_cardinality(
                snapshot,
                thing_manager,
                (*self).into_owned(),
                cardinality,
            )?,
            OwnsAnnotation::Unique(_) => {
                type_manager.set_owns_annotation_unique(snapshot, thing_manager, (*self).into_owned())?
            }
            OwnsAnnotation::Regex(regex) => {
                type_manager.set_owns_annotation_regex(snapshot, thing_manager, (*self).into_owned(), regex)?
            }
            OwnsAnnotation::Range(range) => {
                type_manager.set_owns_annotation_range(snapshot, thing_manager, (*self).into_owned(), range)?
            }
            OwnsAnnotation::Values(values) => {
                type_manager.set_owns_annotation_values(snapshot, thing_manager, (*self).into_owned(), values)?
            }
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
        let owns_annotation = OwnsAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match owns_annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.unset_owns_annotation_distinct(snapshot, (*self).into_owned())?
            }
            OwnsAnnotation::Key(_) => {
                type_manager.unset_owns_annotation_key(snapshot, thing_manager, (*self).into_owned())?
            }
            OwnsAnnotation::Cardinality(_) => {
                type_manager.unset_owns_annotation_cardinality(snapshot, thing_manager, (*self).into_owned())?
            }
            OwnsAnnotation::Unique(_) => type_manager.unset_owns_annotation_unique(snapshot, (*self).into_owned())?,
            OwnsAnnotation::Regex(_) => type_manager.unset_owns_annotation_regex(snapshot, (*self).into_owned())?,
            OwnsAnnotation::Range(_) => type_manager.unset_owns_annotation_range(snapshot, (*self).into_owned())?,
            OwnsAnnotation::Values(_) => type_manager.unset_owns_annotation_values(snapshot, (*self).into_owned())?,
        }
        Ok(())
    }

    pub fn get_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Ordering, Box<ConceptReadError>> {
        type_manager.get_owns_ordering(snapshot, (*self).into_owned())
    }

    pub fn set_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        ordering: Ordering,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_owns_ordering(snapshot, thing_manager, (*self).into_owned(), ordering)
    }

    pub(crate) fn into_owned(self) -> Owns {
        self
    }

    pub fn get_default_cardinality(ordering: Ordering) -> AnnotationCardinality {
        match ordering {
            Ordering::Unordered => Self::DEFAULT_UNORDERED_CARDINALITY,
            Ordering::Ordered => Self::DEFAULT_ORDERED_CARDINALITY,
        }
    }

    pub fn get_default_distinct(ordering: Ordering) -> Option<AnnotationDistinct> {
        match ordering {
            Ordering::Ordered => None,
            Ordering::Unordered => Some(AnnotationDistinct),
        }
    }
}

impl<'a> TypeEdgeEncoding<'a> for Owns {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeOwns;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeOwnsReverse;
    type From = ObjectType;
    type To = AttributeType;

    fn from_vertices(from: ObjectType, to: AttributeType) -> Self {
        Owns::new(from, to)
    }

    fn canonical_from(&self) -> Self::From {
        self.owner()
    }

    fn canonical_to(&self) -> Self::To {
        self.attribute()
    }
}

impl<'a> Capability<'a> for Owns {
    type AnnotationType = OwnsAnnotation;
    type ObjectType = ObjectType;
    type InterfaceType = AttributeType;
    const KIND: CapabilityKind = CapabilityKind::Owns;

    fn new(owner_type: ObjectType, attribute_type: AttributeType) -> Self {
        Owns { owner: owner_type, attribute: attribute_type }
    }

    fn object(&self) -> ObjectType {
        self.owner
    }

    fn interface(&self) -> AttributeType {
        self.attribute
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        let is_abstract = self.get_constraint_abstract(snapshot, type_manager)?.is_some();
        debug_assert!(!is_abstract, "Abstractness of owns is not implemented! Take care of validation");
        Ok(is_abstract)
    }

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, Box<ConceptReadError>> {
        type_manager.get_owns_annotations_declared(snapshot, (*self).into_owned())
    }

    fn get_constraints<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<CapabilityConstraint<Owns>>>, Box<ConceptReadError>>
    where
        'a: 'static,
    {
        type_manager.get_owns_constraints(snapshot, (*self).into_owned())
    }

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_owns_cardinality_constraints(snapshot, (*self).into_owned())
    }

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        type_manager.get_capability_cardinality(snapshot, (*self).into_owned().into_owned())
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum OwnsAnnotation {
    Distinct(AnnotationDistinct),
    Unique(AnnotationUnique),
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
    Regex(AnnotationRegex),
    Range(AnnotationRange),
    Values(AnnotationValues),
}

impl TryFrom<Annotation> for OwnsAnnotation {
    type Error = AnnotationError;

    fn try_from(annotation: Annotation) -> Result<OwnsAnnotation, AnnotationError> {
        match annotation {
            Annotation::Distinct(annotation) => Ok(OwnsAnnotation::Distinct(annotation)),
            Annotation::Unique(annotation) => Ok(OwnsAnnotation::Unique(annotation)),
            Annotation::Key(annotation) => Ok(OwnsAnnotation::Key(annotation)),
            Annotation::Cardinality(annotation) => Ok(OwnsAnnotation::Cardinality(annotation)),
            Annotation::Regex(annotation) => Ok(OwnsAnnotation::Regex(annotation)),
            Annotation::Range(annotation) => Ok(OwnsAnnotation::Range(annotation)),
            Annotation::Values(annotation) => Ok(OwnsAnnotation::Values(annotation)),

            | Annotation::Abstract(_) | Annotation::Independent(_) | Annotation::Cascade(_) => {
                Err(AnnotationError::UnsupportedAnnotationForOwns(annotation.category()))
            }
        }
    }
}

impl From<OwnsAnnotation> for Annotation {
    fn from(anno: OwnsAnnotation) -> Self {
        match anno {
            OwnsAnnotation::Distinct(annotation) => Annotation::Distinct(annotation),
            OwnsAnnotation::Unique(annotation) => Annotation::Unique(annotation),
            OwnsAnnotation::Key(annotation) => Annotation::Key(annotation),
            OwnsAnnotation::Cardinality(annotation) => Annotation::Cardinality(annotation),
            OwnsAnnotation::Regex(annotation) => Annotation::Regex(annotation),
            OwnsAnnotation::Range(annotation) => Annotation::Range(annotation),
            OwnsAnnotation::Values(annotation) => Annotation::Values(annotation),
        }
    }
}

impl PartialEq<Annotation> for OwnsAnnotation {
    fn eq(&self, annotation: &Annotation) -> bool {
        match annotation {
            Annotation::Distinct(_) => matches!(self, Self::Distinct(_)),
            Annotation::Unique(_) => matches!(self, Self::Unique(_)),
            Annotation::Key(_) => matches!(self, Self::Key(_)),
            Annotation::Cardinality(other_cardinality) => {
                if let Self::Cardinality(cardinality) = self {
                    cardinality == other_cardinality
                } else {
                    false
                }
            }
            Annotation::Range(other_range) => {
                if let Self::Range(range) = self {
                    range == other_range
                } else {
                    false
                }
            }
            Annotation::Values(other_values) => {
                if let Self::Values(values) = self {
                    values == other_values
                } else {
                    false
                }
            }
            Annotation::Abstract(_) => false,
            Annotation::Independent(_) => false,
            Annotation::Regex(_) => false,
            Annotation::Cascade(_) => false,
        }
    }
}
