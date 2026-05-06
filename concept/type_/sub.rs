/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::hash::Hash;

use encoding::{
    graph::type_::{CapabilityKind, edge::TypeEdgeEncoding},
    layout::prefix::Prefix,
};
use macro_rules_attribute::derive;

use crate::type_::{
    Capability, KindAPI, TypeAPI,
    annotation::{
        Annotation, AnnotationDoc, AnnotationError, AnnotationMeta, FromAnnotation,
        HasAnnotationCategory, has_annotation_category,
    },
};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct Sub<T> {
    subtype: T,
    supertype: T,
}

impl<T: TypeAPI> Sub<T> {
    pub(crate) fn subtype(self) -> T {
        self.subtype
    }

    pub(crate) fn supertype(self) -> T {
        self.supertype
    }
}

impl<T: TypeAPI + KindAPI + Eq + Hash> Capability for Sub<T> {
    type AnnotationType = SubAnnotation;

    type ObjectType = T;

    type InterfaceType = T;

    const KIND: CapabilityKind = CapabilityKind::Sub;

    fn new(object_type: Self::ObjectType, attribute_type: Self::InterfaceType) -> Self {
        todo!()
    }

    fn object(&self) -> Self::ObjectType {
        todo!()
    }

    fn interface(&self) -> Self::InterfaceType {
        todo!()
    }

    fn is_abstract(
        &self,
        snapshot: &impl storage::snapshot::ReadableSnapshot,
        type_manager: &super::type_manager::TypeManager,
    ) -> Result<bool, Box<crate::error::ConceptReadError>> {
        todo!()
    }

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl storage::snapshot::ReadableSnapshot,
        type_manager: &'this super::type_manager::TypeManager,
    ) -> Result<
        primitive::maybe_owns::MaybeOwns<'this, std::collections::HashSet<Self::AnnotationType>>,
        Box<crate::error::ConceptReadError>,
    > {
        todo!()
    }

    fn get_constraints<'a>(
        self,
        snapshot: &impl storage::snapshot::ReadableSnapshot,
        type_manager: &'a super::type_manager::TypeManager,
    ) -> Result<
        primitive::maybe_owns::MaybeOwns<'a, std::collections::HashSet<super::constraint::CapabilityConstraint<Self>>>,
        Box<crate::error::ConceptReadError>,
    > {
        todo!()
    }

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl storage::snapshot::ReadableSnapshot,
        type_manager: &super::type_manager::TypeManager,
    ) -> Result<
        std::collections::HashSet<super::constraint::CapabilityConstraint<Self>>,
        Box<crate::error::ConceptReadError>,
    > {
        todo!()
    }

    fn get_cardinality(
        &self,
        snapshot: &impl storage::snapshot::ReadableSnapshot,
        type_manager: &super::type_manager::TypeManager,
    ) -> Result<super::annotation::AnnotationCardinality, Box<crate::error::ConceptReadError>> {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, FromAnnotation!, has_annotation_category!)]
pub enum SubAnnotation {
    Doc(AnnotationDoc),
    Meta(AnnotationMeta),
}

impl<T: TypeAPI> TypeEdgeEncoding for Sub<T> {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeSub;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeSubReverse;
    type From = T;
    type To = T;

    fn from_vertices(from: T, to: T) -> Self {
        Sub { subtype: from, supertype: to }
    }

    fn canonical_from(&self) -> Self::From {
        self.subtype()
    }

    fn canonical_to(&self) -> Self::To {
        self.supertype()
    }
}
