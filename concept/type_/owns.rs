/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::graph::type_::edge::{build_edge_owns, build_edge_owns_reverse, TypeEdge};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationDistinct, AnnotationKey, AnnotationUnique},
        attribute_type::AttributeType,
        object_type::ObjectType,
        type_manager::TypeManager,
        IntoCanonicalTypeEdge, Ordering, TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::type_manager::encoding_helper::OwnsEncoder;
use crate::type_::InterfaceEdge;

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

    pub fn is_key<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&OwnsAnnotation::Key(AnnotationKey)))
    }

    pub fn is_unique<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&OwnsAnnotation::Unique(AnnotationUnique))
            || annotations.contains(&OwnsAnnotation::Key(AnnotationKey)))
    }

    pub fn is_distinct<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let is_ordered = false; // TODO
        if is_ordered {
            let annotations = self.get_annotations(snapshot, type_manager)?;
            Ok(annotations.contains(&OwnsAnnotation::Distinct(AnnotationDistinct)))
        } else {
            Ok(true)
        }
    }

    pub fn get_cardinality<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<AnnotationCardinality>, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        for annotation in &annotations {
            match annotation {
                OwnsAnnotation::Cardinality(cardinality) => return Ok(Some(*cardinality)),
                OwnsAnnotation::Key(_) => return Ok(Some(AnnotationCardinality::new(1, Some(1)))),
                _ => (),
            }
        }
        Ok(None)
    }

    // TODO: Should it be 'this or just 'tm on type_manager?
    pub fn get_override<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, Option<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_overridden(snapshot, self.clone().into_owned())
    }

    pub fn set_override<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        type_manager.set_owns_overridden(snapshot, self.clone().into_owned(), overridden)
    }

    pub fn get_annotations<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        type_manager.get_owns_annotations(snapshot, self.clone())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: OwnsAnnotation,
    ) {
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.storage_set_edge_annotation_distinct(snapshot, self.clone()),
            OwnsAnnotation::Unique(_) => type_manager.storage_set_edge_annotation_unique(snapshot, self.clone()),
            OwnsAnnotation::Key(_) => type_manager.storage_set_edge_annotation_key(snapshot, self.clone()),
            OwnsAnnotation::Cardinality(cardinality) => {
                type_manager.storage_set_edge_annotation_cardinality(snapshot, self.clone(), cardinality)
            }
        }
    }

    pub fn unset_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: OwnsAnnotation,
    ) {
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.storage_unset_edge_annotation_distinct(snapshot, self.clone()),
            OwnsAnnotation::Unique(_) => type_manager.storage_unset_edge_annotation_unique(snapshot, self.clone()),
            OwnsAnnotation::Key(_) => type_manager.storage_unset_edge_annotation_key(snapshot, self.clone()),
            OwnsAnnotation::Cardinality(_) => {
                type_manager.storage_unset_edge_annotation_cardinality(snapshot, self.clone())
            }
        }
    }

    pub fn set_ordering<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        ordering: Ordering,
    ) {
        type_manager.set_owns_ordering(snapshot, self.clone().into_type_edge(), ordering)
    }

    pub fn get_ordering<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_owns_ordering(snapshot, self.clone().into_owned())
    }

    fn into_owned(self) -> Owns<'static> {
        Owns { owner: ObjectType::new(self.owner.vertex().into_owned()), attribute: self.attribute.into_owned() }
    }
}

impl<'a> IntoCanonicalTypeEdge<'a> for Owns<'a> {
    fn as_type_edge(&self) -> TypeEdge<'static> {
        build_edge_owns(self.owner.vertex().clone().into_owned(), self.attribute.vertex().clone().into_owned())
    }

    fn into_type_edge(self) -> TypeEdge<'static> {
        build_edge_owns(self.owner.vertex().clone().into_owned(), self.attribute.vertex().clone().into_owned())
    }
}

impl<'a> InterfaceEdge<'a> for Owns<'a> {
    type AnnotationType = OwnsAnnotation;
    type ObjectType = ObjectType<'a>;
    type InterfaceType = AttributeType<'a>;
    type Encoder = OwnsEncoder;

    fn new(owner: ObjectType<'a>, attribute: AttributeType<'a>) -> Self {
        Owns::new(owner, attribute)
    }

    fn object(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    fn interface(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum OwnsAnnotation {
    Distinct(AnnotationDistinct),
    Unique(AnnotationUnique),
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for OwnsAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Distinct(annotation) => OwnsAnnotation::Distinct(annotation),
            Annotation::Unique(annotation) => OwnsAnnotation::Unique(annotation),
            Annotation::Key(annotation) => OwnsAnnotation::Key(annotation),
            Annotation::Cardinality(annotation) => OwnsAnnotation::Cardinality(annotation),

            Annotation::Abstract(_) => unreachable!("Independent annotation not available for Owns."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Owns."),
            Annotation::Regex(_) => unreachable!("Regex annotation not available for Owns."),
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
        }
    }
}
