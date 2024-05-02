/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use bytes::byte_reference::ByteReference;
use encoding::graph::type_::edge::TypeEdge;

use encoding::graph::type_::vertex::TypeVertex;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, owns::Owns, plays::Plays, role_type::RoleType, type_manager::TypeManager},
    ConceptAPI,
};
use crate::error::ConceptWriteError;
use crate::type_::annotation::AnnotationCardinality;

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

pub trait TypeAPI<'a>: ConceptAPI<'a> + Sized + Clone {
    fn vertex<'this>(&'this self) -> TypeVertex<'this>;

    fn into_vertex(self) -> TypeVertex<'a>;

    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>
    ) -> Result<bool, ConceptReadError>;

    fn delete<Snapshot: WritableSnapshot>(
        self, snapshot: &mut Snapshot, type_manager: &TypeManager<Snapshot>
    ) -> Result<(), ConceptWriteError>;
}

pub trait ObjectTypeAPI<'a>: TypeAPI<'a> {}

pub trait OwnerAPI<'a>: TypeAPI<'a> {
    fn set_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Owns<'static>;

    fn delete_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>
    );

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
}

pub trait PlayerAPI<'a>: TypeAPI<'a> {
    fn set_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Plays<'static>;

    fn delete_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>
    );

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


// TODO: where do these belong?
fn serialise_annotation_cardinality(annotation: AnnotationCardinality) -> Box<[u8]> {
    bincode::serialize(&annotation).unwrap().into_boxed_slice()
}

fn deserialise_annotation_cardinality(value: ByteReference<'_>) -> AnnotationCardinality {
    bincode::deserialize(value.bytes()).unwrap()
}

fn serialise_ordering(ordering: Ordering) -> Box<[u8]> {
    bincode::serialize(&ordering).unwrap().into_boxed_slice()
}

fn deserialise_ordering(value: ByteReference<'_>) -> Ordering {
    bincode::deserialize(value.bytes()).unwrap()
}
