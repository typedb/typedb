/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, hash::Hash};

use encoding::{
    graph::type_::{CapabilityKind, edge::TypeEdgeEncoding},
    layout::prefix::Prefix,
};
use macro_rules_attribute::derive;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        Capability, TypeAPI,
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationDoc, AnnotationError, AnnotationMeta,
            FromAnnotation, HasAnnotationCategory, has_annotation_category,
        },
        attribute_type::AttributeType,
        constraint::CapabilityConstraint,
        entity_type::EntityType,
        relation_type::RelationType,
        type_manager::TypeManager,
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

macro_rules! impl_capability_for_sub {
    ($t:ident) => {
        impl Sub<$t> {
            pub fn set_annotation(
                &self,
                snapshot: &mut impl WritableSnapshot,
                type_manager: &TypeManager,
                thing_manager: &ThingManager,
                annotation: SubAnnotation,
            ) -> Result<(), Box<ConceptWriteError>> {
                match annotation {
                    SubAnnotation::Doc(doc) => paste::paste!(type_manager.[<set_sub_ $t:snake _annotation_doc>](snapshot, *self, doc)),
                    SubAnnotation::Meta(meta) => paste::paste!(type_manager.[<set_sub_ $t:snake _annotation_meta>](snapshot, *self, meta)),
                }
            }

            pub fn unset_annotation(
                &self,
                snapshot: &mut impl WritableSnapshot,
                type_manager: &TypeManager,
                thing_manager: &ThingManager,
                annotation_category: AnnotationCategory,
            ) -> Result<(), Box<ConceptWriteError>> {
                let sub_annotation = SubAnnotationCategory::try_from(annotation_category)
                    .map_err(|typedb_source| ConceptWriteError::Annotation { typedb_source })?;
                match sub_annotation {
                    SubAnnotationCategory::Doc => paste::paste!(type_manager.[<unset_sub_ $t:snake _annotation_doc>](snapshot, *self)),
                    SubAnnotationCategory::Meta(meta) => paste::paste!(type_manager.[<unset_sub_ $t:snake _annotation_meta>](snapshot, *self, meta)),
                }
            }
        }

        impl Capability for Sub<$t> {
            type AnnotationType = SubAnnotation;

            type ObjectType = $t;

            type InterfaceType = $t;

            const KIND: CapabilityKind = CapabilityKind::Sub;

            fn new(subtype: Self::ObjectType, supertype: Self::InterfaceType) -> Self {
                Self { subtype, supertype }
            }

            fn object(&self) -> Self::ObjectType {
                self.subtype()
            }

            fn interface(&self) -> Self::InterfaceType {
                self.supertype()
            }

            fn is_abstract(
                &self,
                _snapshot: &impl ReadableSnapshot,
                _type_manager: &TypeManager,
            ) -> Result<bool, Box<ConceptReadError>> {
                Ok(false)
            }

            fn get_annotations_declared<'this>(
                &'this self,
                snapshot: &impl ReadableSnapshot,
                type_manager: &'this TypeManager,
            ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, Box<ConceptReadError>> {
                paste::paste!(type_manager.[<get_sub_ $t:snake _annotations_declared>](snapshot, *self))
            }

            fn get_constraints<'a>(
                self,
                _snapshot: &impl ReadableSnapshot,
                _type_manager: &'a TypeManager,
            ) -> Result<MaybeOwns<'a, HashSet<CapabilityConstraint<Self>>>, Box<ConceptReadError>> {
                Ok(MaybeOwns::Owned(HashSet::new()))
            }

            fn get_cardinality_constraints(
                &self,
                _snapshot: &impl ReadableSnapshot,
                _type_manager: &TypeManager,
            ) -> Result<HashSet<CapabilityConstraint<Self>>, Box<ConceptReadError>> {
                Ok(HashSet::new())
            }

            fn get_cardinality(
                &self,
                _snapshot: &impl ReadableSnapshot,
                _type_manager: &TypeManager,
            ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
                Ok(AnnotationCardinality::unchecked())
            }
        }
    };
}

impl_capability_for_sub!(EntityType);
impl_capability_for_sub!(RelationType);
impl_capability_for_sub!(AttributeType);

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
