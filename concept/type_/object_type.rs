/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt, sync::Arc};

use encoding::{
    error::{EncodingError, EncodingError::UnexpectedPrefix},
    graph::type_::vertex::{TypeVertex, TypeVertexEncoding},
    layout::prefix::Prefix,
    value::label::Label,
    Prefixed,
};
use itertools::Itertools;
use lending_iterator::higher_order::Hkt;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{object::Object, thing_manager::ThingManager},
    type_::{
        attribute_type::AttributeType, constraint::CapabilityConstraint, entity_type::EntityType, owns::Owns,
        plays::Plays, relation_type::RelationType, role_type::RoleType, type_manager::TypeManager, ObjectTypeAPI,
        Ordering, OwnerAPI, PlayerAPI, ThingTypeAPI, TypeAPI,
    },
    ConceptAPI,
};

macro_rules! with_object_type {
    ($object_type:ident, |$type_:ident| $expr:expr) => {
        match $object_type.clone() {
            ObjectType::Entity($type_) => $expr,
            ObjectType::Relation($type_) => $expr,
        }
    };
}
pub(crate) use with_object_type;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ObjectType {
    Entity(EntityType),
    Relation(RelationType),
}

impl fmt::Debug for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entity(inner) => fmt::Debug::fmt(inner, f),
            Self::Relation(inner) => fmt::Debug::fmt(inner, f),
        }
    }
}

impl TypeVertexEncoding for ObjectType {
    fn from_vertex(vertex: TypeVertex) -> Result<Self, EncodingError> {
        match vertex.prefix() {
            Prefix::VertexEntityType => Ok(ObjectType::Entity(EntityType::new(vertex))),
            Prefix::VertexRelationType => Ok(ObjectType::Relation(RelationType::new(vertex))),
            _ => Err(UnexpectedPrefix { actual_prefix: vertex.prefix(), expected_prefix: Prefix::VertexEntityType }), // TODO: That's not right. It can also be VertexRelationType
        }
    }

    fn vertex(&self) -> TypeVertex {
        match self {
            ObjectType::Entity(entity) => entity.vertex(),
            ObjectType::Relation(relation) => relation.vertex(),
        }
    }

    fn into_vertex(self) -> TypeVertex {
        with_object_type!(self, |object| { object.into_vertex() })
    }
}

impl primitive::prefix::Prefix for ObjectType {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().into_starts_with(other.vertex())
    }
}

impl OwnerAPI for ObjectType {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        ordering: Ordering,
    ) -> Result<Owns, Box<ConceptWriteError>> {
        with_object_type!(self, |object| {
            object.set_owns(snapshot, type_manager, thing_manager, attribute_type, ordering)
        })
    }

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.unset_owns(snapshot, type_manager, thing_manager, attribute_type) })
    }

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns_declared(snapshot, type_manager) })
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns(snapshot, type_manager) })
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns_with_specialised(snapshot, type_manager) })
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraint_abstract(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_cardinality(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_distinct(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_regex(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_range(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_values(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraint_unique(snapshot, type_manager, attribute_type)
        })
    }
}

impl ConceptAPI for ObjectType {}

impl TypeAPI for ObjectType {
    const MIN: Self = Self::Entity(EntityType::MIN);
    const MAX: Self = Self::Relation(RelationType::MAX);
    fn new(vertex: TypeVertex) -> Self {
        Self::from_vertex(vertex).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.is_abstract(snapshot, type_manager) })
    }

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.delete(snapshot, type_manager, thing_manager) })
    }

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_label(snapshot, type_manager) })
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_label_arc(snapshot, type_manager) })
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ObjectType>, Box<ConceptReadError>> {
        Ok(with_object_type!(self, |object| {
            object.get_supertype(snapshot, type_manager)?.map(|type_| type_.into_object_type())
        }))
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<ObjectType>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object
                .get_supertypes_transitive(snapshot, type_manager)?
                .iter()
                .map(|type_| (*type_).into_object_type())
                .collect_vec()
        })))
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<ObjectType>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object.get_subtypes(snapshot, type_manager)?.iter().map(|type_| (*type_).into_object_type()).collect()
        })))
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<ObjectType>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object
                .get_subtypes_transitive(snapshot, type_manager)?
                .iter()
                .map(|type_| (*type_).into_object_type())
                .collect_vec()
        })))
    }

    fn next_possible(&self) -> Option<Self> {
        match self {
            ObjectType::Entity(entity) => Some(
                entity
                    .next_possible()
                    .map(|entity_type| Self::Entity(entity_type))
                    .unwrap_or_else(|| Self::Relation(RelationType::MIN)),
            ),
            ObjectType::Relation(relation) => {
                relation.next_possible().map(|relation_type| Self::Relation(relation_type))
            }
        }
    }

    fn previous_possible(&self) -> Option<Self> {
        match self {
            ObjectType::Entity(entity) => entity.previous_possible().map(|entity_type| Self::Entity(entity_type)),
            ObjectType::Relation(relation) => Some(
                relation
                    .previous_possible()
                    .map(|relation_type| Self::Relation(relation_type))
                    .unwrap_or_else(|| Self::Entity(EntityType::MAX)),
            ),
        }
    }
}

impl ThingTypeAPI for ObjectType {
    type InstanceType = Object;
}

impl ObjectTypeAPI for ObjectType {
    fn into_object_type(self) -> ObjectType {
        self
    }
}

impl PlayerAPI for ObjectType {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<Plays, Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.set_plays(snapshot, type_manager, thing_manager, role_type) })
    }

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.unset_plays(snapshot, type_manager, thing_manager, role_type) })
    }

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays_declared(snapshot, type_manager) })
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays(snapshot, type_manager) })
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays_with_specialised(snapshot, type_manager) })
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_played_role_type_constraints(snapshot, type_manager, role_type) })
    }

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_played_role_type_constraint_abstract(snapshot, type_manager, role_type)
        })
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Plays>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_played_role_type_constraints_cardinality(snapshot, type_manager, role_type)
        })
    }
}

impl Hkt for ObjectType {
    type HktSelf<'a> = ObjectType;
}
