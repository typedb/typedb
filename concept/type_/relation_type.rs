/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    sync::Arc,
};

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
        constraint::{CapabilityConstraint, TypeConstraint},
        get_with_specialised,
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RelationType<'a> {}

impl Hkt for RelationType<'static> {
    type HktSelf<'a> = RelationType<'a>;
}

impl<'a> ConceptAPI<'a> for RelationType<'a> {}

impl<'a> TypeVertexEncoding<'a> for RelationType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexRelationType);
        if vertex.prefix() != Prefix::VertexRelationType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexRelationType, actual_prefix: vertex.prefix() })
        } else {
            Ok(RelationType { vertex })
        }
    }

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> PrefixedTypeVertexEncoding<'a> for RelationType<'a> {
    const PREFIX: Prefix = VertexRelationType;
}

impl<'a> TypeAPI<'a> for RelationType<'a> {
    type SelfStatic = RelationType<'static>;

    fn new(vertex: TypeVertex<'a>) -> RelationType<'_> {
        Self::from_vertex(vertex).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_constraint_abstract(snapshot, type_manager)?.is_some())
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.delete_relation_type(snapshot, thing_manager, self.clone().into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_relation_type_label(snapshot, self.clone().into_owned())
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, ConceptReadError> {
        type_manager.get_relation_type_label_arc(snapshot, self.clone().into_owned())
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<RelationType<'static>>, ConceptReadError> {
        type_manager.get_relation_type_supertype(snapshot, self.clone().into_owned())
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_supertypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_subtypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }
}

impl<'a> KindAPI<'a> for RelationType<'a> {
    type AnnotationType = RelationTypeAnnotation;
    const KIND: Kind = Kind::Relation;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<RelationTypeAnnotation>>, ConceptReadError> {
        type_manager.get_relation_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<RelationType<'static>>>>, ConceptReadError>
    where
        'a: 'static,
    {
        type_manager.get_relation_type_constraints(snapshot, self.clone().into_owned())
    }
}

impl<'a> ThingTypeAPI<'a> for RelationType<'a> {
    type InstanceType<'b> = Relation<'b>;
}

impl<'a> ObjectTypeAPI<'a> for RelationType<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static> {
        ObjectType::Relation(self.into_owned())
    }
}

impl<'a> RelationType<'a> {
    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_relation_type_label(snapshot, self.clone().into_owned(), label)
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        supertype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_relation_type_supertype(snapshot, thing_manager, self.clone().into_owned(), supertype)
    }

    pub fn unset_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_relation_type_supertype(snapshot, thing_manager, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: RelationTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => type_manager.set_relation_type_annotation_abstract(
                snapshot,
                thing_manager,
                self.clone().into_owned(),
            )?,
            RelationTypeAnnotation::Cascade(_) => {
                type_manager.set_annotation_cascade(snapshot, thing_manager, self.clone().into_owned())?
            }
        };
        Ok(())
    }

    pub fn unset_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        annotation_category: AnnotationCategory,
    ) -> Result<(), ConceptWriteError> {
        let relation_type_annotation = RelationTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match relation_type_annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_manager.unset_relation_type_annotation_abstract(snapshot, self.clone().into_owned())?
            }
            RelationTypeAnnotation::Cascade(_) => {
                type_manager.unset_annotation_cascade(snapshot, self.clone().into_owned())?
            }
        }
        Ok(())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<TypeConstraint<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_type_abstract_constraint(snapshot, self.clone().into_owned())
    }

    pub fn create_relates(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        name: &str,
        ordering: Ordering,
    ) -> Result<Relates<'static>, ConceptWriteError> {
        let label = Label::build_scoped(name, self.get_label(snapshot, type_manager).unwrap().name().as_str());
        let role_type =
            type_manager.create_role_type(snapshot, thing_manager, &label, self.clone().into_owned(), ordering)?;
        Ok(Relates::new(self.clone().into_owned(), role_type))
    }

    pub(crate) fn get_relates_root<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates_root(snapshot, self.clone().into_owned())
    }

    pub fn get_relates_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates_declared(snapshot, self.clone().into_owned())
    }

    pub fn get_relates<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates(snapshot, self.clone().into_owned())
    }

    pub fn get_relates_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates_with_specialised(snapshot, self.clone().into_owned())
    }

    pub fn get_related_role_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType<'static>>, ConceptReadError> {
        Ok(self.get_relates_declared(snapshot, type_manager)?.iter().map(|relates| relates.role().clone()).collect())
    }

    pub fn get_related_role_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType<'static>>, ConceptReadError> {
        Ok(self.get_relates(snapshot, type_manager)?.iter().map(|relates| relates.role().clone()).collect())
    }

    pub fn get_related_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Relates<'static>>>>, ConceptReadError> {
        type_manager.get_relation_type_related_role_type_constraints(snapshot, self.clone().into_owned(), role_type)
    }

    pub(crate) fn get_related_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<CapabilityConstraint<Relates<'static>>>, ConceptReadError> {
        type_manager.get_type_relates_abstract_constraint(snapshot, self.clone().into_owned(), role_type)
    }

    pub(crate) fn get_related_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Relates<'static>>>, ConceptReadError> {
        type_manager.get_type_relates_cardinality_constraints(snapshot, self.clone().into_owned(), role_type)
    }

    pub(crate) fn get_related_role_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Relates<'static>>>, ConceptReadError> {
        type_manager.get_type_relates_distinct_constraints(snapshot, self.clone().into_owned(), role_type)
    }

    pub(crate) fn is_related_role_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_related_role_type_constraint_abstract(snapshot, type_manager, role_type)?.is_some())
    }

    pub(crate) fn is_related_role_type_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(!self.get_related_role_type_constraints_distinct(snapshot, type_manager, role_type)?.is_empty())
    }

    pub fn get_relates_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
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
        role_type: RoleType<'static>,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        Ok(self.get_relates(snapshot, type_manager)?.iter().find(|relates| relates.role() == role_type).cloned())
    }

    pub fn try_get_relates_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Relates<'static>, ConceptReadError> {
        let relates = self.get_relates_role(snapshot, type_manager, role_type.clone())?;
        match relates {
            None => Err(ConceptReadError::CannotGetRelatesDoesntExist(
                self.get_label(snapshot, type_manager)?.clone(),
                role_type.get_label(snapshot, type_manager)?.clone(),
            )),
            Some(relates) => Ok(relates),
        }
    }

    pub fn get_relates_role_name_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
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
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        for relates in self.get_relates(snapshot, type_manager)?.into_iter() {
            let role_label = relates.role().get_label(snapshot, type_manager)?;
            if role_label.name.as_str() == role_name {
                return Ok(Some(relates.to_owned()));
            }
        }
        Ok(None)
    }

    get_with_specialised! {
        pub fn get_relates_role_with_specialised() -> Relates = RoleType<'static> | get_relates_role;
        pub fn get_relates_role_name_with_specialised() -> Relates = &str | get_relates_role_name;
    }

    pub fn into_owned(self) -> RelationType<'static> {
        RelationType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> Display for RelationType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[RelationType:{}]", self.vertex.type_id_())
    }
}

impl<'a> primitive::prefix::Prefix for RelationType<'a> {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().as_reference().into_starts_with(other.vertex().as_reference())
    }
}

impl<'a> OwnerAPI<'a> for RelationType<'a> {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, ConceptWriteError> {
        type_manager.set_owns(
            snapshot,
            thing_manager,
            self.clone().into_owned_object_type(),
            attribute_type.clone(),
            ordering,
        )?;
        Ok(Owns::new(ObjectType::Relation(self.clone().into_owned()), attribute_type))
    }

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_owns(snapshot, thing_manager, self.clone().into_owned_object_type(), attribute_type)?;
        Ok(())
    }

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_owns_declared(snapshot, self.clone().into_owned())
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_owns(snapshot, self.clone().into_owned())
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_owns_with_specialised(snapshot, self.clone().into_owned())
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns<'static>>>>, ConceptReadError> {
        type_manager.get_relation_type_owned_attribute_type_constraints(
            snapshot,
            self.clone().into_owned(),
            attribute_type,
        )
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_abstract_constraint(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_cardinality_constraints(
            snapshot,
            self.clone().into_owned_object_type(),
            attribute_type,
        )
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_distinct_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn is_owned_attribute_type_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(!self.get_owned_attribute_type_constraints_distinct(snapshot, type_manager, attribute_type)?.is_empty())
    }

    fn get_owned_attribute_type_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_regex_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_range_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_values_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_unique_constraint(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }
}

impl<'a> PlayerAPI<'a> for RelationType<'a> {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError> {
        type_manager.set_plays(snapshot, thing_manager, self.clone().into_owned_object_type(), role_type.clone())
    }

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_plays(snapshot, thing_manager, self.clone().into_owned_object_type(), role_type)
    }

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_plays_declared(snapshot, self.clone().into_owned())
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_plays(snapshot, self.clone().into_owned())
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_plays_with_specialised(snapshot, self.clone().into_owned())
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays<'static>>>>, ConceptReadError> {
        type_manager.get_relation_type_played_role_type_constraints(snapshot, self.clone().into_owned(), role_type)
    }

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<CapabilityConstraint<Plays<'static>>>, ConceptReadError> {
        type_manager.get_type_plays_abstract_constraint(snapshot, self.clone().into_owned_object_type(), role_type)
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Plays<'static>>>, ConceptReadError> {
        type_manager.get_type_plays_cardinality_constraints(snapshot, self.clone().into_owned_object_type(), role_type)
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
                Err(AnnotationError::UnsupportedAnnotationForRelationType(annotation.category()))
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
fn storage_key_to_relation_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> RelationType<'_> {
    RelationType::read_from(storage_key.into_bytes())
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
