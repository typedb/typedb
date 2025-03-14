/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, fmt, hash::Hash, iter, sync::Arc};

use bytes::Bytes;
use encoding::{
    graph::type_::{
        edge::TypeEdgeEncoding,
        property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
        vertex::{TypeVertex, TypeVertexEncoding},
        CapabilityKind, Kind,
    },
    layout::infix::Infix,
    value::label::Label,
};
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use serde::{Deserialize, Serialize};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{thing_manager::ThingManager, ThingAPI},
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationError},
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint, TypeConstraint},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        role_type::RoleType,
        type_manager::TypeManager,
    },
    ConceptAPI,
};

pub mod annotation;
pub mod attribute_type;
pub mod constraint;
pub mod entity_type;
pub mod object_type;
pub mod owns;
pub mod plays;
pub mod relates;
pub mod relation_type;
pub mod role_type;
pub mod sub;
pub mod type_manager;

pub trait TypeAPI: ConceptAPI + TypeVertexEncoding + Copy + Sized + Hash + Eq {
    const MIN: Self;
    const MAX: Self;

    fn new(vertex: TypeVertex) -> Self;

    fn read_from(b: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        Self::from_bytes(b).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>>;

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), Box<ConceptWriteError>>;

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label>, Box<ConceptReadError>>;

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label>, Box<ConceptReadError>>;

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<Self>, Box<ConceptReadError>>;

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, Box<ConceptReadError>>;

    fn get_supertype_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<Self>, Box<ConceptReadError>> {
        Ok(self.get_supertypes_transitive(snapshot, type_manager)?.into_iter().last().cloned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Self>>, Box<ConceptReadError>>;

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, Box<ConceptReadError>>;

    fn is_supertype_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(other.get_supertype(snapshot, type_manager)?.eq(&Some(*self)))
    }

    fn is_supertype_transitive_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(other.get_supertypes_transitive(snapshot, type_manager)?.contains(self))
    }

    fn is_supertype_transitive_of_or_same(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self == &other || self.is_supertype_transitive_of(snapshot, type_manager, other)?)
    }

    fn is_subtype_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(other.get_subtypes(snapshot, type_manager)?.contains(self))
    }

    fn is_subtype_transitive_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(other.get_subtypes_transitive(snapshot, type_manager)?.contains(self))
    }

    fn is_subtype_transitive_of_or_same(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self == &other || self.is_subtype_transitive_of(snapshot, type_manager, other)?)
    }

    fn chain_types<C: IntoIterator<Item = Self>>(first: Self, others: C) -> impl Iterator<Item = Self> {
        iter::once(first).chain(others)
    }

    fn next_possible(&self) -> Option<Self>;

    fn previous_possible(&self) -> Option<Self>;
}

pub trait KindAPI: TypeAPI {
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    const KIND: Kind;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, Box<ConceptReadError>>;

    fn get_constraints<'a>(
        self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'a TypeManager,
    ) -> Result<MaybeOwns<'a, HashSet<TypeConstraint<Self>>>, Box<ConceptReadError>>;
}

pub trait ObjectTypeAPI: TypeAPI + OwnerAPI + ThingTypeAPI {
    fn into_object_type(self) -> ObjectType;
}

pub trait ThingTypeAPI: TypeAPI {
    type InstanceType: ThingAPI;
}

pub trait OwnerAPI: TypeAPI {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
        ordering: Ordering,
    ) -> Result<Owns, Box<ConceptWriteError>>;

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType,
    ) -> Result<(), Box<ConceptWriteError>>;

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>>;

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>>;

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns>>, Box<ConceptReadError>>;

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns>>>, Box<ConceptReadError>>;

    fn get_owned_attribute_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<AttributeType>, Box<ConceptReadError>> {
        Ok(self.get_owns_declared(snapshot, type_manager)?.iter().map(|owns| owns.attribute()).collect())
    }

    fn get_owned_attribute_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<AttributeType>, Box<ConceptReadError>> {
        Ok(self.get_owns(snapshot, type_manager)?.iter().map(|owns| owns.attribute()).collect())
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn is_owned_attribute_type_bounded_to_one(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self
            .get_owned_attribute_type_constraints_cardinality(snapshot, type_manager, attribute_type)?
            .into_iter()
            .map(|constraint| constraint.description().unwrap_cardinality().expect("Only Cardinality constraints"))
            .any(|cardinality| cardinality.is_bounded_to_one()))
    }

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn is_owned_attribute_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_owned_attribute_type_constraint_abstract(snapshot, type_manager, attribute_type)?.is_some())
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
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<HashSet<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<CapabilityConstraint<Owns>>, Box<ConceptReadError>>;

    fn get_owns_attribute_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<Owns>, Box<ConceptReadError>> {
        Ok(self
            .get_owns_declared(snapshot, type_manager)?
            .iter()
            .find(|owns| owns.attribute() == attribute_type)
            .cloned())
    }

    fn has_owns_attribute_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_owns_attribute_declared(snapshot, type_manager, attribute_type)?.is_some())
    }

    fn get_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Option<Owns>, Box<ConceptReadError>> {
        Ok(self.get_owns(snapshot, type_manager)?.iter().find(|owns| owns.attribute() == attribute_type).cloned())
    }

    fn has_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_owns_attribute(snapshot, type_manager, attribute_type)?.is_some())
    }

    fn try_get_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType,
    ) -> Result<Owns, Box<ConceptReadError>> {
        let owns = self.get_owns_attribute(snapshot, type_manager, attribute_type)?;
        match owns {
            None => Err(Box::new(ConceptReadError::CannotGetOwnsDoesntExist {
                type_: self.get_label(snapshot, type_manager)?.clone(),
                owns: attribute_type.get_label(snapshot, type_manager)?.clone(),
            })),
            Some(owns) => Ok(owns),
        }
    }
}

pub trait PlayerAPI: TypeAPI {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<Plays, Box<ConceptWriteError>>;

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType,
    ) -> Result<(), Box<ConceptWriteError>>;

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>>;

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>>;

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays>>, Box<ConceptReadError>>;

    fn get_played_role_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType>, Box<ConceptReadError>> {
        Ok(self.get_plays_declared(snapshot, type_manager)?.iter().map(|plays| plays.role()).collect())
    }

    fn get_played_role_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType>, Box<ConceptReadError>> {
        Ok(self.get_plays(snapshot, type_manager)?.iter().map(|plays| plays.role()).collect())
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays>>>, Box<ConceptReadError>>;

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<CapabilityConstraint<Plays>>, Box<ConceptReadError>>;

    fn is_played_role_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_played_role_type_constraint_abstract(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<HashSet<CapabilityConstraint<Plays>>, Box<ConceptReadError>>;

    fn is_played_role_type_bounded_to_one(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self
            .get_played_role_type_constraints_cardinality(snapshot, type_manager, role_type)?
            .into_iter()
            .map(|constraint| constraint.description().unwrap_cardinality().expect("Only Cardinality constraints"))
            .any(|cardinality| cardinality.is_bounded_to_one()))
    }

    fn get_plays_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Plays>, Box<ConceptReadError>> {
        Ok(self.get_plays_declared(snapshot, type_manager)?.iter().find(|plays| plays.role() == role_type).cloned())
    }

    fn has_plays_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_plays_role_declared(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Option<Plays>, Box<ConceptReadError>> {
        Ok(self.get_plays(snapshot, type_manager)?.iter().find(|plays| plays.role() == role_type).cloned())
    }

    fn has_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<bool, Box<ConceptReadError>> {
        Ok(self.get_plays_role(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_plays_role_name(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Plays>, Box<ConceptReadError>> {
        let mut result: Option<Plays> = None;
        for plays in self.get_plays(snapshot, type_manager)?.into_iter() {
            if plays.role().get_label(snapshot, type_manager)?.name.as_str() == role_name {
                result = Some(*plays);
                break;
            }
        }
        Ok(result)
    }

    fn try_get_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType,
    ) -> Result<Plays, Box<ConceptReadError>> {
        let plays = self.get_plays_role(snapshot, type_manager, role_type)?;
        match plays {
            None => Err(Box::new(ConceptReadError::CannotGetPlaysDoesntExist {
                type_: self.get_label(snapshot, type_manager)?.clone(),
                plays: role_type.get_label(snapshot, type_manager)?.clone(),
            })),
            Some(plays) => Ok(plays),
        }
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Ordering {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    #[default]
    Unordered,
    Ordered,
}

impl fmt::Display for Ordering {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Ordering::Unordered => write!(f, ""),
            Ordering::Ordered => write!(f, "[]"),
        }
    }
}

impl TypeVertexPropertyEncoding for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: &[u8]) -> Ordering {
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

impl TypeEdgePropertyEncoding for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: &[u8]) -> Ordering {
        bincode::deserialize(value).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

pub trait Capability:
    TypeEdgeEncoding<From = Self::ObjectType, To = Self::InterfaceType> + Sized + Copy + Hash + Eq + 'static
{
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    type ObjectType: TypeAPI;
    type InterfaceType: KindAPI;
    const KIND: CapabilityKind;

    fn new(object_type: Self::ObjectType, attribute_type: Self::InterfaceType) -> Self;

    fn object(&self) -> Self::ObjectType;

    fn interface(&self) -> Self::InterfaceType;

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, Box<ConceptReadError>>;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, Box<ConceptReadError>>;

    fn get_constraints<'a>(
        self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'a TypeManager,
    ) -> Result<MaybeOwns<'a, HashSet<CapabilityConstraint<Self>>>, Box<ConceptReadError>>;

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Self>>, Box<ConceptReadError>>;

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, Box<ConceptReadError>>;
}
