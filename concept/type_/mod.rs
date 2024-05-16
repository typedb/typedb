/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use bytes::byte_reference::ByteReference;
use encoding::{
    graph::type_::{edge::TypeEdge, vertex::TypeVertex},
    value::label::Label,
};
use primitive::maybe_owns::MaybeOwns;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use self::{annotation::AnnotationRegex, object_type::ObjectType};
use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::AnnotationCardinality,
        attribute_type::AttributeType, entity_type::EntityType, role_type::RoleType, relation_type::RelationType,
        owns::Owns, plays::Plays,
        type_manager::TypeManager,
    },
    ConceptAPI,
};
use crate::type_::encoding_helper::EdgeEncoder;
use crate::type_::type_manager::KindAPI;
use resource::constants::snapshot::BUFFER_KEY_INLINE;

pub mod annotation;
pub mod attribute_type;
pub mod entity_type;
pub mod object_type;
pub mod owns;
mod plays;
mod relates;
pub mod relation_type;
pub mod role_type;
pub mod type_cache;
pub mod type_manager;
mod type_reader;
mod type_writer;
pub mod validation;
mod encoding_helper;

pub trait TypeAPI<'a>: ConceptAPI<'a> + Sized + Clone + Hash + Eq {
    type SelfStatic: KindAPI<'static> + 'static;
    fn new(vertex : TypeVertex<'a>) -> Self ;

    fn read_from(b: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        Self::new(TypeVertex::new(b))
    }

    fn vertex(&self) -> TypeVertex<'_>;

    fn into_vertex(self) -> TypeVertex<'a>;

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

pub(crate) trait IntoCanonicalTypeEdge<'a> {
    fn as_type_edge(&self) -> TypeEdge<'static>;

    fn into_type_edge(self) -> TypeEdge<'static>;
}
pub(crate) trait InterfaceEdge<'a> : IntoCanonicalTypeEdge<'a> + Sized + Clone
{
    type AnnotationType;
    type ObjectType: TypeAPI<'a>;
    type InterfaceType: KindAPI<'a>;
    type Encoder: EdgeEncoder<'a, Self>;

    fn new(implementor: Self::ObjectType, interface: Self::InterfaceType) -> Self;

    fn object(&self) -> Self::ObjectType;

    fn interface(&self) -> Self::InterfaceType;
}

// TODO: where do these belong?
fn serialise_annotation_cardinality(annotation: AnnotationCardinality) -> Box<[u8]> {
    bincode::serialize(&annotation).unwrap().into_boxed_slice()
}

fn deserialise_annotation_cardinality(value: ByteReference<'_>) -> AnnotationCardinality {
    bincode::deserialize(value.bytes()).unwrap()
}

fn deserialise_annotation_regex(value: ByteReference<'_>) -> AnnotationRegex {
    // TODO this .unwrap() should be handled as an error
    // although it does indicate data corruption
    AnnotationRegex::new(std::str::from_utf8(value.bytes()).unwrap().to_owned())
}

fn serialise_ordering(ordering: Ordering) -> Box<[u8]> {
    bincode::serialize(&ordering).unwrap().into_boxed_slice()
}

fn deserialise_ordering(value: ByteReference<'_>) -> Ordering {
    bincode::deserialize(value.bytes()).unwrap()
}


#[derive(Clone, Debug)]
pub enum WrappedTypeForError {
    EntityType(EntityType<'static>),
    RelationType(RelationType<'static>),
    AttributeType(AttributeType<'static>),
    RoleType(RoleType<'static>),
}
