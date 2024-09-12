/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
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

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl Hkt for EntityType<'static> {
    type HktSelf<'a> = EntityType<'static>;
}

impl<'a> EntityType<'a> {}

impl<'a> ConceptAPI<'a> for EntityType<'a> {}

impl<'a> PrefixedTypeVertexEncoding<'a> for EntityType<'a> {
    const PREFIX: Prefix = VertexEntityType;
}

impl<'a> TypeVertexEncoding<'a> for EntityType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        debug_assert!(Self::PREFIX == VertexEntityType);
        if vertex.prefix() != Prefix::VertexEntityType {
            Err(UnexpectedPrefix { expected_prefix: Prefix::VertexEntityType, actual_prefix: vertex.prefix() })
        } else {
            Ok(EntityType { vertex })
        }
    }

    fn vertex(&self) -> TypeVertex<'_> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> TypeAPI<'a> for EntityType<'a> {
    type SelfStatic = EntityType<'static>;
    fn new(vertex: TypeVertex<'a>) -> EntityType<'a> {
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
        type_manager.delete_entity_type(snapshot, thing_manager, self.into_owned())
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_entity_type_label(snapshot, self.clone().into_owned())
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<EntityType<'static>>, ConceptReadError> {
        type_manager.get_entity_type_supertype(snapshot, self.clone().into_owned())
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_supertypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_subtypes(snapshot, self.clone().into_owned())
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }
}

impl<'a> ObjectTypeAPI<'a> for EntityType<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static> {
        ObjectType::Entity(self.into_owned())
    }
}

impl<'a> KindAPI<'a> for EntityType<'a> {
    type AnnotationType = EntityTypeAnnotation;
    const KIND: Kind = Kind::Entity;

    fn get_annotations_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<EntityTypeAnnotation>>, ConceptReadError> {
        type_manager.get_entity_type_annotations_declared(snapshot, self.clone().into_owned())
    }

    fn get_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<TypeConstraint<EntityType<'static>>>>, ConceptReadError>
    where
        'a: 'static,
    {
        type_manager.get_entity_type_constraints(snapshot, self.clone().into_owned())
    }
}

impl<'a> ThingTypeAPI<'a> for EntityType<'a> {
    type InstanceType<'b> = Entity<'b>;
}

impl<'a> EntityType<'a> {
    pub fn set_label(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_label(snapshot, self.clone().into_owned(), label)
    }

    pub fn set_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        supertype: EntityType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_manager.set_entity_type_supertype(snapshot, thing_manager, self.clone().into_owned(), supertype)
    }

    pub fn unset_supertype(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError> {
        type_manager.unset_entity_type_supertype(snapshot, thing_manager, self.clone().into_owned())
    }

    pub fn set_annotation(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        annotation: EntityTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.set_entity_type_annotation_abstract(snapshot, thing_manager, self.clone().into_owned())?
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
        let entity_annotation = EntityTypeAnnotation::try_getting_default(annotation_category)
            .map_err(|source| ConceptWriteError::Annotation { source })?;
        match entity_annotation {
            EntityTypeAnnotation::Abstract(_) => {
                type_manager.unset_entity_type_annotation_abstract(snapshot, self.clone().into_owned())?
            }
        }

        Ok(())
    }

    pub fn get_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<TypeConstraint<EntityType<'static>>>, ConceptReadError> {
        type_manager.get_type_abstract_constraint(snapshot, self.clone().into_owned())
    }

    pub fn into_owned(self) -> EntityType<'static> {
        EntityType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for EntityType<'a> {
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
        Ok(Owns::new(ObjectType::Entity(self.clone().into_owned()), attribute_type))
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
        type_manager.get_entity_type_owns_declared(snapshot, self.clone().into_owned())
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_owns(snapshot, self.clone().into_owned())
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_owns_with_specialised(snapshot, self.clone().into_owned())
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns<'static>>>>, ConceptReadError> {
        type_manager.get_entity_type_owned_attribute_type_constraints(
            snapshot,
            self.clone().into_owned(),
            attribute_type,
        )
    }

    fn get_type_owns_constraints_cardinality<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_cardinality_constraints(
            snapshot,
            self.clone().into_owned_object_type(),
            attribute_type,
        )
    }

    fn get_type_owns_constraints_distinct<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_distinct_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn is_type_owns_distinct<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(!self.get_type_owns_constraints_distinct(snapshot, type_manager, attribute_type)?.is_empty())
    }

    fn get_type_owns_constraints_regex<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_regex_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_type_owns_constraints_range<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_range_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_type_owns_constraints_values<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_values_constraints(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }

    fn get_type_owns_constraint_unique<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, ConceptReadError> {
        type_manager.get_type_owns_unique_constraint(snapshot, self.clone().into_owned_object_type(), attribute_type)
    }
}

impl<'a> PlayerAPI<'a> for EntityType<'a> {
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
        type_manager.get_entity_type_plays_declared(snapshot, self.clone().into_owned())
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_plays(snapshot, self.clone().into_owned())
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        type_manager.get_entity_type_plays_with_specialised(snapshot, self.clone().into_owned())
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays<'static>>>>, ConceptReadError> {
        type_manager.get_entity_type_played_role_type_constraints(snapshot, self.clone().into_owned(), role_type)
    }

    fn get_type_plays_constraints_cardinality<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Plays<'static>>>, ConceptReadError> {
        type_manager.get_type_plays_cardinality_constraints(snapshot, self.clone().into_owned_object_type(), role_type)
    }
}

impl<'a> Display for EntityType<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
fn storage_key_to_entity_type(storage_key: StorageKey<'_, BUFFER_KEY_INLINE>) -> EntityType<'_> {
    EntityType::read_from(storage_key.into_bytes())
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_to_entity_type);
