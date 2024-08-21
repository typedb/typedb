/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use encoding::{
    graph::type_::{edge::TypeEdgeEncoding, vertex::TypeVertexEncoding, CapabilityKind},
    layout::prefix::Prefix,
};
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
        object_type::ObjectType,
        type_manager::TypeManager,
        Capability, Ordering, TypeAPI,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Owns<'a> {
    owner: ObjectType<'a>,
    attribute: AttributeType<'a>,
}

impl<'a> Owns<'a> {
    pub const DEFAULT_UNORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, Some(1));
    pub const DEFAULT_ORDERED_CARDINALITY: AnnotationCardinality = AnnotationCardinality::new(0, None);

    pub fn owner(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    pub fn attribute(&self) -> AttributeType<'static> {
        self.attribute.clone().into_owned()
    }

    pub fn is_key(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_owns_is_key(snapshot, self.clone())
    }

    pub fn is_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_owns_is_unique(snapshot, self.clone())
    }

    pub fn is_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_owns_is_distinct(snapshot, self.clone())
    }

    pub fn get_constraint_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationRegex>, ConceptReadError> {
        type_manager.get_owns_regex(snapshot, self.clone())
    }

    pub fn get_constraint_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationRange>, ConceptReadError> {
        type_manager.get_owns_range(snapshot, self.clone())
    }

    pub fn get_constraint_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationValues>, ConceptReadError> {
        type_manager.get_owns_values(snapshot, self.clone())
    }

    pub fn set_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_owns_override(snapshot, thing_manager, self.clone().into_owned(), overridden)
    }

    pub fn unset_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_owns_override(snapshot, thing_manager, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: OwnsAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.set_owns_annotation_distinct(snapshot, thing_manager, self.clone().into_owned())?
            }
            OwnsAnnotation::Key(_) => {
                type_manager.set_owns_annotation_key(snapshot, thing_manager, self.clone().into_owned())?
            }
            OwnsAnnotation::Cardinality(cardinality) => type_manager.set_owns_annotation_cardinality(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
                cardinality,
            )?,
            OwnsAnnotation::Unique(_) => {
                type_manager.set_owns_annotation_unique(snapshot, thing_manager, self.clone().into_owned())?
            }
            OwnsAnnotation::Regex(regex) => {
                type_manager.set_owns_annotation_regex(snapshot, thing_manager, self.clone().into_owned(), regex)?
            }
            OwnsAnnotation::Range(range) => {
                type_manager.set_owns_annotation_range(snapshot, thing_manager, self.clone().into_owned(), range)?
            }
            OwnsAnnotation::Values(values) => {
                type_manager.set_owns_annotation_values(snapshot, thing_manager, self.clone().into_owned(), values)?
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
    ) -> Result<(), ConceptWriteError> {
        let owns_annotation = OwnsAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match owns_annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.unset_capability_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Key(_) => {
                type_manager.unset_owns_annotation_key(snapshot, thing_manager, self.clone().into_owned())?
            }
            OwnsAnnotation::Cardinality(_) => {
                type_manager.unset_owns_annotation_cardinality(snapshot, thing_manager, self.clone().into_owned())?
            }
            OwnsAnnotation::Unique(_) => {
                type_manager.unset_owns_annotation_unique(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Regex(_) => {
                type_manager.unset_owns_annotation_regex(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Range(_) => {
                type_manager.unset_owns_annotation_range(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Values(_) => {
                type_manager.unset_owns_annotation_values(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn get_ordering<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_owns_ordering(snapshot, self.clone().into_owned())
    }

    pub fn set_ordering(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_owns_ordering(snapshot, thing_manager, self.clone().into_owned(), ordering)
    }

    pub(crate) fn into_owned(self) -> Owns<'static> {
        Owns { owner: ObjectType::new(self.owner.vertex().into_owned()), attribute: self.attribute.into_owned() }
    }

    pub(crate) fn get_uniqueness_source(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        debug_assert!(
            !AnnotationCategory::Unique.declarable_below(AnnotationCategory::Key)
                && AnnotationCategory::Key.declarable_below(AnnotationCategory::Unique),
            "This function uses the fact that @key is always below @unique. Revalidate the logic!"
        );

        let owns_owned = self.clone().into_owned();

        let unique_source = type_manager.get_capability_annotation_source(
            snapshot,
            owns_owned.clone(),
            OwnsAnnotation::Unique(AnnotationUnique),
        )?;
        Ok(match unique_source {
            Some(_) => unique_source,
            None => type_manager.get_capability_annotation_source(
                snapshot,
                owns_owned,
                OwnsAnnotation::Key(AnnotationKey),
            )?,
        })
    }
}

impl<'a> TypeEdgeEncoding<'a> for Owns<'a> {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeOwns;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeOwnsReverse;
    type From = ObjectType<'a>;
    type To = AttributeType<'a>;

    fn from_vertices(from: ObjectType<'a>, to: AttributeType<'a>) -> Self {
        Owns::new(from, to)
    }

    fn canonical_from(&self) -> Self::From {
        self.owner()
    }

    fn canonical_to(&self) -> Self::To {
        self.attribute()
    }
}

impl<'a> Capability<'a> for Owns<'a> {
    type AnnotationType = OwnsAnnotation;
    type ObjectType = ObjectType<'a>;
    type InterfaceType = AttributeType<'a>;
    const KIND: CapabilityKind = CapabilityKind::Owns;

    fn new(owner_type: ObjectType<'a>, attribute_type: AttributeType<'a>) -> Self {
        Owns { owner: owner_type, attribute: attribute_type }
    }

    fn object(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    fn interface(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }

    fn get_override<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, Option<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_override(snapshot, self.clone().into_owned())
    }

    fn get_overriding<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_overriding(snapshot, self.clone().into_owned())
    }

    fn get_overriding_transitive<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_overriding_transitive(snapshot, self.clone().into_owned())
    }

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        type_manager.get_owns_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashMap<OwnsAnnotation, Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_annotations(snapshot, self.clone().into_owned())
    }

    fn get_default_cardinality<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        let ordering = self.get_ordering(snapshot, type_manager)?;
        Ok(type_manager.get_owns_default_cardinality(ordering))
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

impl Into<Annotation> for OwnsAnnotation {
    fn into(self) -> Annotation {
        match self {
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
