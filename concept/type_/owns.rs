/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use encoding::graph::type_::edge::{build_edge_owns, TypeEdge};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::ReadableSnapshot;
use crate::error::ConceptReadError;
use crate::type_::{attribute_type::AttributeType, IntoCanonicalTypeEdge, object_type::ObjectType, TypeAPI};
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

    pub(crate) fn owner(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    fn attribute(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }

    pub(crate) fn get_annotations<'m, 'this>(
        &'this self, type_manager: &'m TypeManager<impl ReadableSnapshot>
    ) -> Result<MaybeOwns<'m, HashSet<OwnsAnnotation>>, ConceptReadError> {
        // type_manager.get_owns_annotations(self.clone())
        todo!()
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