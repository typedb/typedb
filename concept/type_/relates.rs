/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::{
    graph::type_::{CapabilityKind, edge::TypeEdgeEncoding},
    layout::prefix::Prefix,
};
use lending_iterator::higher_order::Hkt;
use macro_rules_attribute::derive;
use primitive::maybe_owns::MaybeOwns;
use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
    type_::{
        Capability, Ordering,
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationDistinct,
            AnnotationDoc, AnnotationError, AnnotationMeta, FromAnnotation, HasAnnotationCategory,
            has_annotation_category,
        },
        constraint::CapabilityConstraint,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::TypeManager,
    },
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
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
        type_manager.get_capability_abstract_constraint(snapshot, *self)
    }

    pub fn get_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relates_distinct_constraints(snapshot, *self)
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
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_relates_specialise(snapshot, thing_manager, *self, specialised, storage_counters)
    }

    pub fn unset_specialise(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_relates_specialise(snapshot, thing_manager, *self)
    }

    pub fn is_implicit(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        type_manager.get_relates_is_implicit(snapshot, *self)
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: RelatesAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            RelatesAnnotation::Abstract(_) => {
                type_manager.set_relates_annotation_abstract(snapshot, thing_manager, *self, true)?
            }
            RelatesAnnotation::Distinct(_) => {
                type_manager.set_relates_annotation_distinct(snapshot, thing_manager, *self)?
            }
            RelatesAnnotation::Cardinality(cardinality) => {
                type_manager.set_relates_annotation_cardinality(snapshot, thing_manager, *self, cardinality)?
            }
            RelatesAnnotation::Doc(doc) => type_manager.set_relates_annotation_doc(snapshot, *self, doc)?,
            RelatesAnnotation::Meta(meta) => type_manager.set_relates_annotation_meta(snapshot, *self, meta)?,
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
        let relates_annotation = RelatesAnnotationCategory::try_from(annotation_category)
            .map_err(|typedb_source| ConceptWriteError::Annotation { typedb_source })?;
        match relates_annotation {
            RelatesAnnotationCategory::Abstract => type_manager.unset_relates_annotation_abstract(snapshot, *self)?,
            RelatesAnnotationCategory::Distinct => type_manager.unset_relates_annotation_distinct(snapshot, *self)?,
            RelatesAnnotationCategory::Cardinality => {
                type_manager.unset_relates_annotation_cardinality(snapshot, thing_manager, *self)?
            }
            RelatesAnnotationCategory::Doc => type_manager.unset_relates_annotation_doc(snapshot, *self)?,
            RelatesAnnotationCategory::Meta(meta) => {
                type_manager.unset_relates_annotation_meta(snapshot, *self, meta)?
            }
        }
        Ok(())
    }

    pub fn get_default_cardinality_for_explicit(role_ordering: Ordering) -> AnnotationCardinality {
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
}

impl TypeEdgeEncoding for Relates {
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

impl Capability for Relates {
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
        type_manager.get_relates_annotations_declared(snapshot, *self)
    }

    fn get_constraints<'m>(
        self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Relates>>>, Box<ConceptReadError>> {
        type_manager.get_relates_constraints(snapshot, self)
    }

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relates_cardinality_constraints(snapshot, *self)
    }

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>> {
        type_manager.get_relates_cardinality(snapshot, *self)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, FromAnnotation!, has_annotation_category!)]
pub enum RelatesAnnotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Cardinality(AnnotationCardinality),
    Doc(AnnotationDoc),
    Meta(AnnotationMeta),
}
