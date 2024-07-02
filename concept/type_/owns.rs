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
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationDistinct, AnnotationKey, AnnotationRegex,
            AnnotationUnique,
        },
        attribute_type::AttributeType,
        object_type::ObjectType,
        type_manager::{validation::ConversionError, TypeManager},
        InterfaceImplementation, Ordering, TypeAPI,
    },
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Owns<'a> {
    owner: ObjectType<'a>,
    attribute: AttributeType<'a>,
}

impl<'a> Owns<'a> {
    pub fn new(owner_type: ObjectType<'a>, attribute_type: AttributeType<'a>) -> Self {
        Owns { owner: owner_type, attribute: attribute_type }
    }

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
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains_key(&OwnsAnnotation::Key(AnnotationKey)))
    }

    pub fn is_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains_key(&OwnsAnnotation::Unique(AnnotationUnique))
            || annotations.contains_key(&OwnsAnnotation::Key(AnnotationKey)))
    }

    pub fn is_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        match self.get_ordering(snapshot, type_manager)? {
            Ordering::Ordered => {
                let annotations = self.get_annotations(snapshot, type_manager)?;
                Ok(annotations.contains_key(&OwnsAnnotation::Distinct(AnnotationDistinct)))
            }
            Ordering::Unordered => Ok(true),
        }
    }

    pub fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationCardinality>, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        for annotation in annotations.keys() {
            match annotation {
                OwnsAnnotation::Cardinality(cardinality) => return Ok(Some(*cardinality)),
                OwnsAnnotation::Key(_) => return Ok(Some(AnnotationCardinality::new(1, Some(1)))),
                _ => (),
            }
        }
        Ok(None)
    }

    // TODO: Should it be 'this or just 'tm on type_manager?
    pub fn get_override<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, Option<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_overridden(snapshot, self.clone().into_owned())
    }

    pub fn set_override(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_owns_overridden(snapshot, self.clone().into_owned(), overridden)
    }

    pub fn unset_override<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_owns_overridden(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        type_manager.get_owns_annotations_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashMap<OwnsAnnotation, Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation: OwnsAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.set_owns_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Key(_) => type_manager.set_owns_annotation_key(snapshot, self.clone().into_owned())?,
            OwnsAnnotation::Cardinality(cardinality) => {
                type_manager.set_edge_annotation_cardinality(snapshot, self.clone().into_owned(), cardinality)?
            }
            OwnsAnnotation::Unique(_) => {
                type_manager.set_owns_annotation_unique(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Regex(regex) => {
                type_manager.set_owns_annotation_regex(snapshot, self.clone().into_owned(), regex)?
            }
        }
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        let owns_annotation = OwnsAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Conversion { source })?;
        match owns_annotation {
            OwnsAnnotation::Distinct(_) => {
                type_manager.unset_edge_annotation_distinct(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Key(_) => type_manager.unset_edge_annotation_key(snapshot, self.clone().into_owned())?,
            OwnsAnnotation::Cardinality(_) => {
                type_manager.unset_edge_annotation_cardinality(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Unique(_) => {
                type_manager.unset_owns_annotation_unique(snapshot, self.clone().into_owned())?
            }
            OwnsAnnotation::Regex(_) => {
                type_manager.unset_edge_annotation_regex(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn set_ordering<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        ordering: Ordering,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_owns_ordering(snapshot, self.clone().into_owned(), ordering)
    }

    pub fn get_ordering(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_owns_ordering(snapshot, self.clone().into_owned())
    }

    fn into_owned(self) -> Owns<'static> {
        Owns { owner: ObjectType::new(self.owner.vertex().into_owned()), attribute: self.attribute.into_owned() }
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

impl<'a> InterfaceImplementation<'a> for Owns<'a> {
    type AnnotationType = OwnsAnnotation;
    type ObjectType = ObjectType<'a>;
    type InterfaceType = AttributeType<'a>;

    fn object(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    fn interface(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum OwnsAnnotation {
    Distinct(AnnotationDistinct),
    Unique(AnnotationUnique),
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
    Regex(AnnotationRegex),
}

impl OwnsAnnotation {
    pub fn try_getting_default(annotation_category: AnnotationCategory) -> Result<OwnsAnnotation, ConversionError> {
        annotation_category.to_default_annotation().into()
    }
}

impl From<Annotation> for Result<OwnsAnnotation, ConversionError> {
    fn from(annotation: Annotation) -> Result<OwnsAnnotation, ConversionError> {
        match annotation {
            Annotation::Distinct(annotation) => Ok(OwnsAnnotation::Distinct(annotation)),
            Annotation::Unique(annotation) => Ok(OwnsAnnotation::Unique(annotation)),
            Annotation::Key(annotation) => Ok(OwnsAnnotation::Key(annotation)),
            Annotation::Cardinality(annotation) => Ok(OwnsAnnotation::Cardinality(annotation)),
            Annotation::Regex(annotation) => Ok(OwnsAnnotation::Regex(annotation)),

            Annotation::Abstract(_) => Err(ConversionError::UnsupportedAnnotationForTypeOrEdge(annotation.category())),
            Annotation::Independent(_) => {
                Err(ConversionError::UnsupportedAnnotationForTypeOrEdge(annotation.category()))
            }
            Annotation::Cascade(_) => Err(ConversionError::UnsupportedAnnotationForTypeOrEdge(annotation.category())),
        }
    }
}

impl From<Annotation> for OwnsAnnotation {
    fn from(annotation: Annotation) -> Self {
        let into_annotation: Result<OwnsAnnotation, ConversionError> = annotation.into();
        match into_annotation {
            Ok(into_annotation) => into_annotation,
            Err(_) => unreachable!("Do not call this conversion from user-exposed code!"),
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

            Annotation::Abstract(_) => false,
            Annotation::Independent(_) => false,
            Annotation::Regex(_) => false,
            Annotation::Cascade(_) => false,
        }
    }
}
