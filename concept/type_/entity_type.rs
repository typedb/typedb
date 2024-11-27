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
    layout::prefix::{Prefix, Prefix::VertexEntityType},
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
    thing::{entity::Entity, thing_manager::ThingManager},
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationCategory, AnnotationError, DefaultFrom},
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, TypeConstraint},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        role_type::RoleType,
        type_manager::TypeManager,
        Capability, KindAPI, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI, ThingTypeAPI, TypeAPI,
    },
    ConceptAPI,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EntityType {
    vertex: TypeVertex,
}

impl Hkt for EntityType {
    type HktSelf<'a> = EntityType;
}

impl<'a> EntityType {}

impl<'a> ConceptAPI<'a> for EntityType {}

impl<'a> PrefixedTypeVertexEncoding<'a> for EntityType {
    const PREFIX: Prefix = VertexEntityType;
}

impl<'a> TypeVertexEncoding<'a> for EntityType {
    fn from_vertex(vertex: TypeVertex) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexEntityType);
        if vertex.prefix() != Prefix::VertexEntityType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexEntityType, actual_prefix: vertex.prefix() })
        } else {
            Ok(EntityType { vertex })
        }
    }

    fn vertex(&self) -> TypeVertex {
        self.vertex
    }

    fn into_vertex(self) -> TypeVertex {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for EntityType {
    type SelfStatic = EntityType;
    fn new(vertex: TypeVertex) -> EntityType {
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
        type_manager.delete_entity_type(snapshot, thing_manager, self.into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_label(snapshot, (*self).into_owned())
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_label_arc(snapshot, (*self).into_owned())
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<EntityType>, Box<ConceptReadError>> {
        type_manager.get_entity_type_supertype(snapshot, (*self).into_owned())
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_supertypes(snapshot, (*self).into_owned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<EntityType>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_subtypes(snapshot, (*self).into_owned())
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_subtypes_transitive(snapshot, (*self).into_owned())
    }
}

impl<'a> ObjectTypeAPI<'a> for EntityType {
    fn into_owned_object_type(self) -> ObjectType {
        ObjectType::Entity(self.into_owned())
    }
}

impl<'a> KindAPI<'a> for EntityType {
    type AnnotationType = EntityTypeAnnotation;
    const KIND: Kind = Kind::Entity;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<EntityTypeAnnotation>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_annotations_declared(snapshot, (*self).into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<EntityType>>>, Box<ConceptReadError>>
    where
        'a: 'static,
    {
        type_manager.get_entity_type_constraints(snapshot, (*self).into_owned())
    }
}

impl<'a> ThingTypeAPI<'a> for EntityType {
    type InstanceType<'b> = Entity<'b>;
}

impl<'a> EntityType {
    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_label(snapshot, (*self).into_owned(), label)
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        supertype: EntityType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.set_entity_type_supertype(snapshot, thing_manager, (*self).into_owned(), supertype)
    }

    pub fn unset_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_entity_type_supertype(snapshot, thing_manager, (*self).into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: EntityTypeAnnotation,
    ) -> Result<(), Box<ConceptWriteError>> {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.set_entity_type_annotation_abstract(snapshot, thing_manager, (*self).into_owned())?
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
        let entity_annotation = EntityTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match entity_annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.unset_entity_type_annotation_abstract(snapshot, (*self).into_owned())?
            }
        }

        Ok(())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<TypeConstraint<EntityType>>, Box<ConceptReadError>> {
        type_manager.get_type_abstract_constraint(snapshot, (*self).into_owned())
    }

    pub fn into_owned(self) -> EntityType {
        self
    }
}

impl<'a> OwnerAPI<'a> for EntityType {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        ordering: Ordering,
    ) -> Result<Owns, Box<ConceptWriteError>> {
        type_manager.set_owns(snapshot, thing_manager, (*self).into_owned_object_type(), attribute_type, ordering)?;
        Ok(Owns::new(ObjectType::Entity((*self).into_owned()), attribute_type))
    }

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_owns(snapshot, thing_manager, (*self).into_owned_object_type(), attribute_type)?;
        Ok(())
    }

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_owns_declared(snapshot, (*self).into_owned())
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_owns(snapshot, (*self).into_owned())
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_owns_with_specialised(snapshot, (*self).into_owned())
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns>>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_owned_attribute_type_constraints(snapshot, (*self).into_owned(), attribute_type)
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_abstract_constraint(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_cardinality_constraints(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_distinct_constraints(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_regex_constraints(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_range_constraints(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_values_constraints(snapshot, (*self).into_owned_object_type(), attribute_type)
    }

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        type_manager.get_type_owns_unique_constraint(snapshot, (*self).into_owned_object_type(), attribute_type)
    }
}

impl<'a> PlayerAPI<'a> for EntityType {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<Plays, Box<ConceptWriteError>> {
        type_manager.set_plays(snapshot, thing_manager, (*self).into_owned_object_type(), role_type)
    }

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        type_manager.unset_plays(snapshot, thing_manager, (*self).into_owned_object_type(), role_type)
    }

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_plays_declared(snapshot, (*self).into_owned())
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_plays(snapshot, (*self).into_owned())
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_plays_with_specialised(snapshot, (*self).into_owned())
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays>>>, Box<ConceptReadError>> {
        type_manager.get_entity_type_played_role_type_constraints(snapshot, (*self).into_owned(), role_type)
    }

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_type_plays_abstract_constraint(snapshot, (*self).into_owned_object_type(), role_type)
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        type_manager.get_type_plays_cardinality_constraints(snapshot, (*self).into_owned_object_type(), role_type)
    }
}

impl<'a> fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[EntityType:{}]", self.vertex.type_id_())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum EntityTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl TryFrom<Annotation> for EntityTypeAnnotation {
    type Error = AnnotationError;
    fn try_from(annotation: Annotation) -> Result<EntityTypeAnnotation, AnnotationError> {
        match annotation {
            Annotation::Abstract(annotation) => Ok(EntityTypeAnnotation::Abstract(annotation)),

            | Annotation::Distinct(_)
            | Annotation::Independent(_)
            | Annotation::Unique(_)
            | Annotation::Key(_)
            | Annotation::Cardinality(_)
            | Annotation::Regex(_)
            | Annotation::Cascade(_)
            | Annotation::Range(_)
            | Annotation::Values(_) => Err(AnnotationError::UnsupportedAnnotationForEntityType(annotation.category())),
        }
    }
}

impl From<EntityTypeAnnotation> for Annotation {
    fn from(val: EntityTypeAnnotation) -> Self {
        match val {
            EntityTypeAnnotation::Abstract(annotation) => Annotation::Abstract(annotation),
        }
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_entity_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> EntityType {
    EntityType::read_from(storage_key.into_bytes())
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_to_entity_type);
