/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    hash::Hash,
    iter,
    sync::Arc,
};

use bytes::{byte_reference::ByteReference, Bytes};
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
        constraint::{CapabilityConstraint, TypeConstraint},
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

macro_rules! get_with_specialised {
    ($(
        $vis:vis fn $method_name:ident() -> $output_type:ident = $input_type:ty | $get_method:ident;
    )*) => {
        $(
            $vis fn $method_name(
                &self,
                snapshot: &impl ReadableSnapshot,
                type_manager: &TypeManager,
                input_type: $input_type,
            ) -> Result<Option<$output_type<'static>>, ConceptReadError> {
                let self_result = self.$get_method(snapshot, type_manager, input_type.clone())?;
                Ok(match self_result {
                    Some(owns) => Some(owns),
                    None => match self.get_supertype(snapshot, type_manager)? {
                        Some(supertype) => {
                            let supertype_result = supertype.$get_method(snapshot, type_manager, input_type)?;
                            match supertype_result {
                                Some(supertype_result) => Some(supertype_result),
                                None => None,
                            }
                        }
                        None => None,
                    },
                })
            }
        )*
    }
}
pub(crate) use get_with_specialised;

pub trait TypeAPI<'a>: ConceptAPI<'a> + TypeVertexEncoding<'a> + Sized + Clone + Hash + Eq + 'a {
    type SelfStatic: KindAPI<'static> + 'static;

    fn new(vertex: TypeVertex<'a>) -> Self;

    fn read_from(b: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        Self::from_bytes(b).unwrap()
    }

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError>;

    fn delete(
        self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Result<(), ConceptWriteError>;

    fn get_label<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError>;

    fn get_label_arc(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Arc<Label<'static>>, ConceptReadError>;

    fn get_supertype(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<Self>, ConceptReadError>;

    fn get_supertypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, ConceptReadError>;

    fn get_supertype_root(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<Self>, ConceptReadError> {
        Ok(self.get_supertypes_transitive(snapshot, type_manager)?.into_iter().last().cloned())
    }

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Self>>, ConceptReadError>;

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, ConceptReadError>;

    fn is_supertype_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(other.get_supertype(snapshot, type_manager)?.eq(&Some(self.clone())))
    }

    fn is_supertype_transitive_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(other.get_supertypes_transitive(snapshot, type_manager)?.contains(self))
    }

    fn is_supertype_transitive_of_or_same(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(self == &other || self.is_supertype_transitive_of(snapshot, type_manager, other)?)
    }

    fn is_subtype_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(other.get_subtypes(snapshot, type_manager)?.contains(self))
    }

    fn is_subtype_transitive_of(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(other.get_subtypes_transitive(snapshot, type_manager)?.contains(self))
    }

    fn is_subtype_transitive_of_or_same(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        other: Self,
    ) -> Result<bool, ConceptReadError> {
        Ok(self == &other || self.is_subtype_transitive_of(snapshot, type_manager, other)?)
    }

    fn chain_types<C: IntoIterator<Item = Self>>(first: Self, others: C) -> impl Iterator<Item = Self> {
        iter::once(first).chain(others)
    }
}

pub trait KindAPI<'a>: TypeAPI<'a> {
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    const KIND: Kind;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, ConceptReadError>;

    fn get_constraints<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<TypeConstraint<Self>>>, ConceptReadError>
    where
        'a: 'static;
}

pub trait ObjectTypeAPI<'a>: TypeAPI<'a> + OwnerAPI<'a> + ThingTypeAPI<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static>;
}

pub trait ThingTypeAPI<'a>: TypeAPI<'a> {
    type InstanceType<'b>: ThingAPI<'b>;
}

pub trait OwnerAPI<'a>: TypeAPI<'a> {
    fn set_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, ConceptWriteError>;

    fn unset_owns(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError>;

    fn get_owns_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owns<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owns_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Owns<'static>>>>, ConceptReadError>;

    fn get_owned_attribute_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<AttributeType<'static>>, ConceptReadError> {
        Ok(self.get_owns_declared(snapshot, type_manager)?.iter().map(|owns| owns.attribute().clone()).collect())
    }

    fn get_owned_attribute_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<AttributeType<'static>>, ConceptReadError> {
        Ok(self.get_owns(snapshot, type_manager)?.iter().map(|owns| owns.attribute().clone()).collect())
    }

    fn get_owned_attribute_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraints_distinct(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn is_owned_attribute_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owned_attribute_type_constraint_abstract(snapshot, type_manager, attribute_type)?.is_some())
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
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraints_range(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraints_values(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owned_attribute_type_constraint_unique(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<CapabilityConstraint<Owns<'static>>>, ConceptReadError>;

    fn get_owns_attribute_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
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
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute_declared(snapshot, type_manager, attribute_type)?.is_some())
    }

    fn get_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        Ok(self.get_owns(snapshot, type_manager)?.iter().find(|owns| owns.attribute() == attribute_type).cloned())
    }

    get_with_specialised! {
        fn get_owns_attribute_with_specialised() -> Owns = AttributeType<'static> | get_owns_attribute;
    }

    fn has_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute(snapshot, type_manager, attribute_type)?.is_some())
    }

    fn try_get_owns_attribute(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        attribute_type: AttributeType<'static>,
    ) -> Result<Owns<'static>, ConceptReadError> {
        let owns = self.get_owns_attribute(snapshot, type_manager, attribute_type.clone())?;
        match owns {
            None => Err(ConceptReadError::CannotGetOwnsDoesntExist(
                self.get_label(snapshot, type_manager)?.clone(),
                attribute_type.get_label(snapshot, type_manager)?.clone(),
            )),
            Some(owns) => Ok(owns),
        }
    }
}

pub trait PlayerAPI<'a>: TypeAPI<'a> {
    fn set_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError>;

    fn unset_plays(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError>;

    fn get_plays_declared<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_plays<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_plays_with_specialised<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_played_role_types_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType<'static>>, ConceptReadError> {
        Ok(self.get_plays_declared(snapshot, type_manager)?.iter().map(|plays| plays.role().clone()).collect())
    }

    fn get_played_role_types(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<RoleType<'static>>, ConceptReadError> {
        Ok(self.get_plays(snapshot, type_manager)?.iter().map(|plays| plays.role().clone()).collect())
    }

    fn get_played_role_type_constraints<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<MaybeOwns<'m, HashSet<CapabilityConstraint<Plays<'static>>>>, ConceptReadError>;

    fn get_played_role_type_constraint_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<CapabilityConstraint<Plays<'static>>>, ConceptReadError>;

    fn is_played_role_type_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_played_role_type_constraint_abstract(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_played_role_type_constraints_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<HashSet<CapabilityConstraint<Plays<'static>>>, ConceptReadError>;

    fn get_plays_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        Ok(self.get_plays_declared(snapshot, type_manager)?.iter().find(|plays| plays.role() == role_type).cloned())
    }

    fn has_plays_role_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role_declared(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        Ok(self.get_plays(snapshot, type_manager)?.iter().find(|plays| plays.role() == role_type).cloned())
    }

    fn has_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_plays_role_name(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_name: &str,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let mut result: Option<Plays<'static>> = None;
        for plays in self.get_plays(snapshot, type_manager)?.into_iter() {
            if plays.role().get_label(snapshot, type_manager)?.name.as_str() == role_name {
                result = Some(plays.clone());
                break;
            }
        }
        Ok(result)
    }

    get_with_specialised! {
        fn get_plays_role_with_specialised() -> Plays = RoleType<'static> | get_plays_role;
        fn get_plays_role_name_with_specialised() -> Plays = &str | get_plays_role_name;
    }

    fn try_get_plays_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptReadError> {
        let plays = self.get_plays_role(snapshot, type_manager, role_type.clone())?;
        match plays {
            None => Err(ConceptReadError::CannotGetPlaysDoesntExist(
                self.get_label(snapshot, type_manager)?.clone(),
                role_type.get_label(snapshot, type_manager)?.clone(),
            )),
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

impl Display for Ordering {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Ordering::Unordered => write!(f, ""),
            Ordering::Ordered => write!(f, "[]"),
        }
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: ByteReference<'_>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: ByteReference<'_>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(self).unwrap().as_slice()))
    }
}

pub trait Capability<'a>:
    TypeEdgeEncoding<'a, From = Self::ObjectType, To = Self::InterfaceType> + Sized + Clone + Hash + Eq + 'a
{
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    type ObjectType: TypeAPI<'a>;
    type InterfaceType: KindAPI<'a>;
    const KIND: CapabilityKind;

    fn new(object_type: Self::ObjectType, attribute_type: Self::InterfaceType) -> Self;

    fn object(&self) -> Self::ObjectType;

    fn interface(&self) -> Self::InterfaceType;

    fn is_abstract(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<bool, ConceptReadError>;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, ConceptReadError>;

    fn get_constraints<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<CapabilityConstraint<Self>>>, ConceptReadError>
    where
        'a: 'static;

    fn get_cardinality_constraints(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<HashSet<CapabilityConstraint<Self>>, ConceptReadError>
    where
        'a: 'static;

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, ConceptReadError>;
}
