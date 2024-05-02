/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::graph::type_::edge::{build_edge_owns, TypeEdge};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::error::ConceptReadError;
use crate::type_::{attribute_type::AttributeType, IntoCanonicalTypeEdge, object_type::ObjectType, Ordering, TypeAPI};
use crate::type_::annotation::{Annotation, AnnotationCardinality, AnnotationDistinct};
use crate::type_::type_manager::TypeManager;

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

    pub fn attribute(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }

    pub fn is_distinct<'this, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>
    ) -> Result<bool, ConceptReadError> {
        let is_ordered = false; // TODO
        if is_ordered {
            let annotations = self.get_annotations(snapshot, type_manager)?;
            Ok(annotations.contains(&OwnsAnnotation::Distinct(AnnotationDistinct::new())))
        } else {
            Ok(true)
        }
    }

    pub(crate) fn get_annotations<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        type_manager.get_owns_annotations(snapshot, self.clone())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>, annotation: OwnsAnnotation
    ) {
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.storage_set_edge_annotation_distinct(
                snapshot,
                self.clone()
            ),
            OwnsAnnotation::Cardinality(cardinality) => {
                type_manager.storage_set_edge_annotation_cardinality(snapshot, self.clone(), cardinality)
            }
        }
    }

    pub fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: OwnsAnnotation
    ) {
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.storage_delete_edge_annotation_distinct(snapshot, self.clone()),
            OwnsAnnotation::Cardinality(_) => {
                type_manager.storage_delete_edge_annotation_cardinality(snapshot, self.clone())
            }
        }
    }

    pub fn set_ordering<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        ordering: Ordering
    ) {
        type_manager.storage_set_owns_ordering(snapshot, self.clone().into_type_edge(), ordering)
    }

    pub fn get_ordering<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum OwnsAnnotation {
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for OwnsAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Distinct(annotation) => OwnsAnnotation::Distinct(annotation),
            Annotation::Cardinality(annotation) => OwnsAnnotation::Cardinality(annotation),
            Annotation::Abstract(_) => unreachable!("Independent annotation not available for Owns."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Owns."),
        }
    }
}