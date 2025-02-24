/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt, sync::Arc};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::{
        type_::{
            vertex::{PrefixedTypeVertexEncoding, TypeVertex, TypeVertexEncoding},
            Kind,
        },
        Typed,
    },
    layout::prefix::{Prefix, Prefix::VertexRelationType},
    value::label::Label,
    Prefixed,
};
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::StorageKey,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    concept_iterator,
    error::{ConceptReadError, ConceptWriteError},
    thing::{relation::Relation, thing_manager::ThingManager},
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCascade, AnnotationCategory, AnnotationError, DefaultFrom,
        },
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint, TypeConstraint},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, ThingTypeAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationType {
    vertex: TypeVertex,
}

impl fmt::Debug for RelationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Relation[{:?}]", self.vertex.type_id_())
    }
}

impl RelationType {}

impl Hkt for RelationType {
    type HktSelf<'a> = RelationType;
}

impl ConceptAPI for RelationType {}

impl TypeVertexEncoding for RelationType {
    fn from_vertex(vertex: TypeVertex) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexRelationType);
        if vertex.prefix() != Prefix::VertexRelationType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexRelationType, actual_prefix: vertex.prefix() })
        } else {
            Ok(RelationType { vertex })
        }
    }

    fn vertex(&self) -> TypeVertex {
        self.vertex
    }

    fn into_vertex(self) -> TypeVertex {
        self.vertex
    }
}

impl PrefixedTypeVertexEncoding for RelationType {
    const PREFIX: Prefix = VertexRelationType;
}

impl TypeAPI for RelationType {
    fn new(vertex: TypeVertex) -> RelationType {
        Self::from_vertex(vertex).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_constraint_abstract(snapshot, type_manager)?.is_some())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.delete_relation_type(snapshot, thing_manager, self)
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label>, Box<ConceptReadError>> {
        type_manager.get_relation_type_label(snapshot, *self)
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label>, Box<ConceptReadError>> {
        type_manager.get_relation_type_label_arc(snapshot, *self)
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<RelationType>, Box<ConceptReadError>> {
        type_manager.get_relation_type_supertype(snapshot, *self)
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RelationType>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_supertypes(snapshot, *self)
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelationType>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_subtypes(snapshot, *self)
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RelationType>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_subtypes_transitive(snapshot, *self)
    }
}

impl KindAPI for RelationType {
    type AnnotationType = RelationTypeAnnotation;
    const KIND: Kind = Kind::Relation;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelationTypeAnnotation>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_annotations_declared(snapshot, *self)
    }

    fn get_constraints<'m>(
        self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<RelationType>>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_constraints(snapshot, self)
    }
}

impl ThingTypeAPI for RelationType {
    type InstanceType = Relation;
}

impl ObjectTypeAPI for RelationType {
    fn into_object_type(self) -> ObjectType {
        ObjectType::Relation(self)
    }
}

impl RelationType {
    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_relation_type_label(snapshot, *self, label)
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        supertype: RelationType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_relation_type_supertype(snapshot, thing_manager, *self, supertype)
    }

    pub fn unset_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_relation_type_supertype(snapshot, thing_manager, *self)
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: RelationTypeAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.set_relation_type_annotation_abstract(snapshot, thing_manager, *self)?
            }
            RelationTypeAnnotation::Cascade(_) => {
                type_manager.set_annotation_cascade(snapshot, thing_manager, *self)?
            }
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), Box<ConceptWriteError>> {
        let relation_type_annotation = RelationTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|typedb_source| ConceptWriteError::Annotation { typedb_source })?;
        match relation_type_annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.unset_relation_type_annotation_abstract(snapshot, *self)?
            }
            RelationTypeAnnotation::Cascade(_) => type_manager.unset_annotation_cascade(snapshot, *self)?,
        }
        Ok(())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<TypeConstraint<RelationType>>, Box<ConceptReadError>> {
        type_manager.get_type_abstract_constraint(snapshot, *self)
    }

    pub fn create_relates(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        name: &str,
        ordering: Ordering,
    ) -> Result<Relates, Box<ConceptWriteError>> {
        let label = Label::build_scoped(name, self.get_label(snapshot, type_manager).unwrap().name().as_str(), None);
        let role_type = type_manager.create_role_type(snapshot, thing_manager, &label, *self, ordering)?;
        Ok(Relates::new(*self, role_type))
    }

    pub(crate) fn get_relates_root<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_relates_root(snapshot, *self)
    }

    pub fn get_relates_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_relates_declared(snapshot, *self)
    }

    pub fn get_relates<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_relates(snapshot, *self)
    }

    pub fn get_relates_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_relates_with_specialised(snapshot, *self)
    }

    pub fn get_related_role_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType>, Box<ConceptReadError>> {
        Ok(self.get_relates_declared(snapshot, type_manager)?.iter().map(|relates| relates.role()).collect())
    }

    pub fn get_related_role_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType>, Box<ConceptReadError>> {
        Ok(self.get_relates(snapshot, type_manager)?.iter().map(|relates| relates.role()).collect())
    }

    pub fn get_related_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Relates>>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_related_role_type_constraints(snapshot, *self, role_type)
    }

    pub fn get_related_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_type_relates_abstract_constraint(snapshot, *self, role_type)
    }

    pub fn get_related_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_type_relates_cardinality_constraints(snapshot, *self, role_type)
    }

    fn is_related_role_type_bounded_to_one(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self
            .get_related_role_type_constraints_cardinality(snapshot, type_manager, role_type)?
            .into_iter()
            .map(|constraint| constraint.description().unwrap_cardinality().expect("Only Cardinality constraints"))
            .any(|cardinality| cardinality.is_bounded_to_one()))
    }

    pub(crate) fn get_related_role_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Relates>>, Box<ConceptReadError>> {
        type_manager.get_type_relates_distinct_constraints(snapshot, *self, role_type)
    }

    pub fn is_related_role_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_related_role_type_constraint_abstract(snapshot, type_manager, role_type)?.is_some())
    }

    pub(crate) fn is_related_role_type_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(!self.get_related_role_type_constraints_distinct(snapshot, type_manager, role_type)?.is_empty())
    }

    pub fn get_relates_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        Ok(self
            .get_relates_declared(snapshot, type_manager)?
            .iter()
            .find(|relates| relates.role() == role_type)
            .cloned())
    }

    pub fn get_relates_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        Ok(self.get_relates(snapshot, type_manager)?.iter().find(|relates| relates.role() == role_type).cloned())
    }

    pub fn try_get_relates_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Relates, Box<ConceptReadError>> {
        let relates = self.get_relates_role(snapshot, type_manager, role_type)?;
        match relates {
            None => Err(Box::new(ConceptReadError::CannotGetRelatesDoesntExist {
                type_: self.get_label(snapshot, type_manager)?.clone(),
                relates: role_type.get_label(snapshot, type_manager)?.clone(),
            })),
            Some(relates) => Ok(relates),
        }
    }

    pub fn get_relates_role_name_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        for relates in self.get_relates_declared(snapshot, type_manager)?.into_iter() {
            let role_label = relates.role().get_label(snapshot, type_manager)?;
            if role_label.name.as_str() == role_name {
                return Ok(Some(relates.to_owned()));
            }
        }
        Ok(None)
    }

    pub fn get_relates_role_name(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        for relates in self.get_relates(snapshot, type_manager)?.into_iter() {
            let role_label = relates.role().get_label(snapshot, type_manager)?;
            if role_label.name.as_str() == role_name {
                return Ok(Some(relates.to_owned()));
            }
        }
        Ok(None)
    }

    pub fn get_relates_role_explicit_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        if let Some(relates) = self.get_relates_role_declared(snapshot, type_manager, role_type.clone())? {
            if !relates.is_implicit(snapshot, type_manager)? {
                return Ok(Some(relates));
            }
        }
        Ok(None)
    }

    pub fn get_relates_role_explicit(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        if let Some(relates) = self.get_relates_role(snapshot, type_manager, role_type)? {
            if relates.is_implicit(snapshot, type_manager)? {
                Ok(Some(relates.role().get_relates_explicit(snapshot, type_manager)?))
            } else {
                Ok(Some(relates))
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_relates_role_name_explicit_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        if let Some(relates) = self.get_relates_role_name_declared(snapshot, type_manager, role_name)? {
            if !relates.is_implicit(snapshot, type_manager)? {
                return Ok(Some(relates));
            }
        }
        Ok(None)
    }

    pub fn get_relates_role_name_explicit(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Relates>, Box<ConceptReadError>> {
        if let Some(relates) = self.get_relates_role_name(snapshot, type_manager, role_name)? {
            if relates.is_implicit(snapshot, type_manager)? {
                Ok(Some(relates.role().get_relates_explicit(snapshot, type_manager)?))
            } else {
                Ok(Some(relates))
            }
        } else {
            Ok(None)
        }
    }
}

impl fmt::Display for RelationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[RelationType:{}]", self.vertex.type_id_())
    }
}

impl primitive::prefix::Prefix for RelationType {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().into_starts_with(other.vertex())
    }
}

impl OwnerAPI for RelationType {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        ordering: Ordering,
    ) -> Result<Owns, Box<ConceptWriteError>> {
        type_manager.set_owns(snapshot, thing_manager, (*self).into_object_type(), attribute_type, ordering)?;
        Ok(Owns::new(ObjectType::Relation(*self), attribute_type))
    }

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_owns(snapshot, thing_manager, (*self).into_object_type(), attribute_type)?;
        Ok(())
    }

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_owns_declared(snapshot, *self)
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_owns(snapshot, *self)
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_owns_with_specialised(snapshot, *self)
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns>>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_owned_attribute_type_constraints(snapshot, *self, attribute_type)
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_abstract_constraint(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_cardinality_constraints(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_distinct_constraints(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn is_owned_attribute_type_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(!self.get_owned_attribute_type_constraints_distinct(snapshot, type_manager, attribute_type)?.is_empty())
    }

    fn get_owned_attribute_type_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_regex_constraints(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_range_constraints(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_values_constraints(snapshot, (*self).into_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_unique_constraint(snapshot, (*self).into_object_type(), attribute_type)
    }
}

impl PlayerAPI for RelationType {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<Plays, Box<ConceptWriteError>> {
        type_manager.set_plays(snapshot, thing_manager, (*self).into_object_type(), role_type)
    }

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_plays(snapshot, thing_manager, (*self).into_object_type(), role_type)
    }

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_plays_declared(snapshot, *self)
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_plays(snapshot, *self)
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_plays_with_specialised(snapshot, *self)
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays>>>, Box<ConceptReadError>> {
        type_manager.get_relation_type_played_role_type_constraints(snapshot, *self, role_type)
    }

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_type_plays_abstract_constraint(snapshot, (*self).into_object_type(), role_type)
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_type_plays_cardinality_constraints(snapshot, (*self).into_object_type(), role_type)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RelationTypeAnnotation {
    Abstract(AnnotationAbstract),
    Cascade(AnnotationCascade),
}

impl TryFrom<Annotation> for RelationTypeAnnotation {
    type Error = AnnotationError;
    fn try_from(annotation: Annotation) -> Result<RelationTypeAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(RelationTypeAnnotation::Abstract(annotation)),
            Annotation::Cascade(annotation) => Ok(RelationTypeAnnotation::Cascade(annotation)),

            | Annotation::Distinct(_)
            | Annotation::Independent(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Cardinality(_)
            | Annotation::Regex(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => {
                Err(AnnotationError::UnsupportedAnnotationForRelationType { category: annotation.category() })
            }
        }
    }
}

impl From<RelationTypeAnnotation> for Annotation {
    fn from(anno: RelationTypeAnnotation) -> Self {
        match anno {
            RelationTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
            RelationTypeAnnotation::Cascade(annotation) => Annotation::Cascade(annotation),
        }
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_relation_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> RelationType {
    RelationType::read_from(storage_key.into_bytes())
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
