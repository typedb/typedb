/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
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
    AsBytes,
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
mod constraint;
pub mod entity_type;
pub mod object_type;
pub mod owns;
pub mod plays;
pub mod relates;
pub mod relation_type;
pub mod role_type;
pub mod sub;
pub mod type_manager;

macro_rules! get_with_overridden {
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
pub(crate) use get_with_overridden;

pub trait TypeAPI<'a>: ConceptAPI<'a> + TypeVertexEncoding<'a> + Sized + Clone + Hash + Eq + 'a {
    type SelfStatic: KindAPI<'static> + 'static;

    fn new(vertex: TypeVertex<'a>) -> Self;

    fn read_from(b: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        Self::from_bytes(b).unwrap()
    }

    fn vertex(&self) -> TypeVertex<'_>;

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

    fn get_label_cloned<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<Label<'m>, ConceptReadError> {
        self.get_label(snapshot, type_manager).map(|label| label.clone())
    }

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

    fn get_subtypes<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, ConceptReadError>;

    fn get_subtypes_transitive<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, Vec<Self>>, ConceptReadError>;
}

pub trait KindAPI<'a>: TypeAPI<'a> {
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    const ROOT_KIND: Kind;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, ConceptReadError>;

    fn get_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashMap<Self::AnnotationType, Self>>, ConceptReadError>;
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

    fn get_owns_overrides<'m>(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'m TypeManager,
    ) -> Result<MaybeOwns<'m, HashMap<Owns<'static>, Owns<'static>>>, ConceptReadError>;

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

    get_with_overridden! {
        fn get_owns_attribute_with_overridden() -> Owns = AttributeType<'static> | get_owns_attribute;
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

    get_with_overridden! {
        fn get_plays_role_with_overridden() -> Plays = RoleType<'static> | get_plays_role;
        fn get_plays_role_name_with_overridden() -> Plays = &str | get_plays_role_name;
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

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Ordering {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    Unordered,
    Ordered,
}

impl Ordering {
    pub fn default() -> Ordering {
        Ordering::Unordered
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: ByteReference<'_>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> TypeEdgePropertyEncoding<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn from_value_bytes(value: ByteReference<'_>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

pub trait Capability<'a>:
    TypeEdgeEncoding<'a, From = Self::ObjectType, To = Self::InterfaceType> + Sized + Clone + Hash + Eq + 'a
{
    type AnnotationType: Hash + Eq + Clone + TryFrom<Annotation, Error = AnnotationError> + Into<Annotation>;
    type ObjectType: TypeAPI<'a>;
    type InterfaceType: KindAPI<'a>;
    const KIND: CapabilityKind;

    fn new(object_type: Self::ObjectType, interface_type: Self::InterfaceType) -> Self;

    fn object(&self) -> Self::ObjectType;

    fn interface(&self) -> Self::InterfaceType;

    fn get_override<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, Option<Self>>, ConceptReadError>;

    fn get_overriding<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self>>, ConceptReadError>;

    fn get_overriding_transitive<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self>>, ConceptReadError>;

    fn get_annotations_declared<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashSet<Self::AnnotationType>>, ConceptReadError>;

    fn get_annotations<'this>(
        &'this self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &'this TypeManager,
    ) -> Result<MaybeOwns<'this, HashMap<Self::AnnotationType, Self>>, ConceptReadError>;

    fn get_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, ConceptReadError> {
        type_manager.get_cardinality(snapshot, self.clone())
    }

    fn get_cardinality_declared(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Option<AnnotationCardinality>, ConceptReadError> {
        type_manager.get_cardinality_declared(snapshot, self.clone())
    }

    fn get_default_cardinality(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<AnnotationCardinality, ConceptReadError>;
}

pub struct EdgeOverride<EDGE: TypeEdgeEncoding<'static>> {
    overrides: EDGE, // TODO: Consider storing EDGE::To instead
}

impl<'a, EDGE: TypeEdgeEncoding<'static>> TypeEdgePropertyEncoding<'a> for EdgeOverride<EDGE> {
    const INFIX: Infix = Infix::PropertyOverride;

    fn from_value_bytes(value: ByteReference<'_>) -> Self {
        Self { overrides: EDGE::decode_canonical_edge(Bytes::Reference(value).into_owned()) }
    }

    fn to_value_bytes(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Reference(self.overrides.to_canonical_type_edge().bytes()).into_owned())
    }
}
