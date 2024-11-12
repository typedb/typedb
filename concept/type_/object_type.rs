/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ObjectType<'a> {
    Entity(EntityType<'a>),
    Relation(RelationType<'a>),
}

impl<'a> ObjectType<'a> {
    pub fn into_owned(self) -> ObjectType<'static> {
        match self {
            Self::Entity(entity_type) => ObjectType::Entity(entity_type.into_owned()),
            Self::Relation(relation_type) => ObjectType::Relation(relation_type.into_owned()),
        }
    }
}

impl<'a> TypeVertexEncoding<'a> for ObjectType<'a> {
    fn from_vertex(vertex: TypeVertex<'a>) -> Result<Self, EncodingError> {
        match vertex.prefix() {
            Prefix::VertexEntityType => Ok(ObjectType::Entity(EntityType::new(vertex))),
            Prefix::VertexRelationType => Ok(ObjectType::Relation(RelationType::new(vertex))),
            _ => Err(UnexpectedPrefix { actual_prefix: vertex.prefix(), expected_prefix: Prefix::VertexEntityType }), // TODO: That's not right. It can also be VertexRelationType
        }
    }

    fn vertex(&self) -> TypeVertex<'_> {
        match self {
            ObjectType::Entity(entity) => entity.vertex(),
            ObjectType::Relation(relation) => relation.vertex(),
        }
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        with_object_type!(self, |object| { object.into_vertex() })
    }
}

impl<'a> primitive::prefix::Prefix for ObjectType<'a> {
    fn starts_with(&self, other: &Self) -> bool {
        self.vertex().starts_with(&other.vertex())
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.vertex().as_reference().into_starts_with(other.vertex().as_reference())
    }
}

impl<'a> OwnerAPI<'a> for ObjectType<'a> {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, Box<ConceptWriteError>> {
        with_object_type!(self, |object| {
            object.set_owns(snapshot, type_manager, thing_manager, attribute_type, ordering)
        })
    }

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.unset_owns(snapshot, type_manager, thing_manager, attribute_type) })
    }

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns_declared(snapshot, type_manager) })
    }

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns(snapshot, type_manager) })
    }

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_owns_with_specialised(snapshot, type_manager) })
    }

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns<'static>>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraint_abstract(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_cardinality(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_distinct(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_regex(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_regex(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_range(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraints_values(snapshot, type_manager, attribute_type)
        })
    }

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_owned_attribute_type_constraint_unique(snapshot, type_manager, attribute_type)
        })
    }
}

impl<'a> ConceptAPI<'a> for ObjectType<'a> {}

impl<'a> TypeAPI<'a> for ObjectType<'a> {
    type SelfStatic = RelationType<'static>;

    fn new(vertex: TypeVertex<'a>) -> Self {
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
    ) -> Result<MaybeOwns<'m, Label<'static>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_label(snapshot, type_manager) })
    }

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_label_arc(snapshot, type_manager) })
    }

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<ObjectType<'static>>, Box<ConceptReadError>> {
        Ok(with_object_type!(self, |object| {
            object.get_supertype(snapshot, type_manager)?.map(|type_| type_.into_owned_object_type())
        }))
    }

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<ObjectType<'static>>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object
                .get_supertypes_transitive(snapshot, type_manager)?
                .iter()
                .map(|type_| type_.clone().into_owned_object_type())
                .collect_vec()
        })))
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<ObjectType<'static>>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object
                .get_subtypes(snapshot, type_manager)?
                .iter()
                .map(|type_| type_.clone().into_owned_object_type())
                .collect()
        })))
    }

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<ObjectType<'static>>>, Box<ConceptReadError>> {
        Ok(MaybeOwns::Owned(with_object_type!(self, |object| {
            object
                .get_subtypes_transitive(snapshot, type_manager)?
                .iter()
                .map(|type_| type_.clone().into_owned_object_type())
                .collect_vec()
        })))
    }
}

impl<'a> ThingTypeAPI<'a> for ObjectType<'a> {
    type InstanceType<'b> = Object<'b>;
}

impl<'a> ObjectTypeAPI<'a> for ObjectType<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static> {
        self.into_owned()
    }
}

impl<'a> PlayerAPI<'a> for ObjectType<'a> {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.set_plays(snapshot, type_manager, thing_manager, role_type) })
    }

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), Box<ConceptWriteError>> {
        with_object_type!(self, |object| { object.unset_plays(snapshot, type_manager, thing_manager, role_type) })
    }

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays_declared(snapshot, type_manager) })
    }

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays(snapshot, type_manager) })
    }

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_plays_with_specialised(snapshot, type_manager) })
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays<'static>>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| { object.get_played_role_type_constraints(snapshot, type_manager, role_type) })
    }

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<CapabilityConstraint<Plays<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_played_role_type_constraint_abstract(snapshot, type_manager, role_type)
        })
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Plays<'static>>>, Box<ConceptReadError>> {
        with_object_type!(self, |object| {
            object.get_played_role_type_constraints_cardinality(snapshot, type_manager, role_type)
        })
    }
}

impl Hkt for ObjectType<'static> {
    type HktSelf<'a> = ObjectType<'a>;
}
