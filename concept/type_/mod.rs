/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;

use bytes::byte_reference::ByteReference;
use encoding::{
    graph::type_::{edge::TypeEdge, vertex::TypeVertex},
    value::label::Label,
};
use primitive::maybe_owns::MaybeOwns;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use bytes::Bytes::Array;
use encoding::AsBytes;
use encoding::graph::type_::edge::EncodableParametrisedTypeEdge;
use encoding::graph::type_::property::{EncodableTypeEdgeProperty, EncodableTypeVertexProperty};
use encoding::graph::type_::vertex::EncodableTypeVertex;
use encoding::layout::infix::Infix;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use self::{annotation::AnnotationRegex, object_type::ObjectType};
use crate::{
    ConceptAPI,
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::AnnotationCardinality,
        attribute_type::AttributeType, entity_type::EntityType, owns::Owns, plays::Plays,
        relation_type::RelationType, role_type::RoleType,
        type_manager::TypeManager,
    },
};
use crate::type_::type_manager::KindAPI;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;
use crate::type_::annotation::Annotation;
use crate::type_::object_type::ObjectType;

pub mod annotation;
pub mod attribute_type;
pub mod entity_type;
pub mod object_type;
pub mod owns;
mod plays;
mod relates;
pub mod relation_type;
pub mod role_type;
pub mod type_manager;

pub trait TypeAPI<'a>: ConceptAPI<'a> + EncodableTypeVertex<'a> + Sized + Clone + Hash + Eq + 'a {
    type SelfStatic: KindAPI<'static> + 'static;
    fn new(vertex : TypeVertex<'a>) -> Self ;

    fn read_from(b: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        Self::new(TypeVertex::new(b))
    }

    fn vertex(&self) -> TypeVertex<'_>;

    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError>;

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<(), ConceptWriteError>;

    fn get_label<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError>;
}

pub trait ObjectTypeAPI<'a>: TypeAPI<'a> + OwnerAPI<'a> {
    fn into_owned_object_type(self) -> ObjectType<'static>;
}

pub trait OwnerAPI<'a>: TypeAPI<'a> {
    fn set_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Result<Owns<'static>, ConceptWriteError>;

    fn delete_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError>;

    fn get_owns<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owns_attribute<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError>;

    fn has_owns_attribute<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute(snapshot, type_manager, attribute_type)?.is_some())
    }

    fn get_owns_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashMap<AttributeType<'static>, Owns<'static>>>, ConceptReadError>;

    fn get_owns_attribute_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        Ok(self.get_owns_transitive(snapshot, type_manager)?.get(&attribute_type).map(|owns| owns.clone()))
    }

    fn has_owns_attribute_transitive<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute_transitive(snapshot, type_manager, attribute_type)?.is_some())
    }
}

pub trait PlayerAPI<'a>: TypeAPI<'a> {
    fn set_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Plays<'static>, ConceptWriteError>;

    fn delete_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<(), ConceptWriteError>;

    fn get_plays<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_plays_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError>;

    fn has_plays_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role(snapshot, type_manager, role_type)?.is_some())
    }

    fn get_plays_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashMap<RoleType<'static>, Plays<'static>>>, ConceptReadError>;

    fn get_plays_role_transitive<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        Ok(self.get_plays_transitive(snapshot, type_manager)?.get(&role_type).map(|plays| plays.clone()))
    }

    fn has_plays_role_transitive<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role_transitive(snapshot, type_manager, role_type)?.is_some())
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

impl<'a> EncodableTypeVertexProperty<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn decode_value<'b>(value: ByteReference<'b>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

impl<'a> EncodableTypeEdgeProperty<'a> for Ordering {
    const INFIX: Infix = Infix::PropertyOrdering;

    fn decode_value<'b>(value: ByteReference<'b>) -> Ordering {
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

pub(crate) trait InterfaceImplementation<'a> : EncodableParametrisedTypeEdge<'a, From=Self::ObjectType, To=Self::InterfaceType> + Sized + Clone + Hash + Eq + 'a
{
    type AnnotationType;
    type ObjectType: TypeAPI<'a>;
    type InterfaceType: KindAPI<'a>;

    fn object(&self) -> Self::ObjectType;

    fn interface(&self) -> Self::InterfaceType;

    fn unwrap_annotation(annotation: Self::AnnotationType) -> Annotation;
}

pub struct EdgeOverride<EDGE: EncodableParametrisedTypeEdge<'static>> {
    overridden: EDGE, // TODO: Consider storing EDGE::To instead
}

impl<'a, EDGE: EncodableParametrisedTypeEdge<'static>> EncodableTypeEdgeProperty<'a> for EdgeOverride<EDGE> {
    const INFIX: Infix = Infix::PropertyOverride;

    fn decode_value<'b>(value: ByteReference<'b>) -> Self {
        Self { overridden: EDGE::decode_canonical_edge(Bytes::Reference(value).into_owned()) }
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Reference(self.overridden.to_canonical_type_edge().bytes()).into_owned())
    }
}

#[derive(Clone, Debug)]
pub enum WrappedTypeForError {
    EntityType(EntityType<'static>),
    RelationType(RelationType<'static>),
    AttributeType(AttributeType<'static>),
    RoleType(RoleType<'static>),
}
