/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::edge::{build_edge_plays, build_edge_plays_reverse, TypeEdge};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{object_type::ObjectType, role_type::RoleType, type_manager::TypeManager, IntoCanonicalTypeEdge, TypeAPI},
};
use crate::error::ConceptWriteError;
use crate::type_::encoding_helper::PlaysEncoder;
use crate::type_::InterfaceEdge;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Plays<'a> {
    player: ObjectType<'a>,
    role: RoleType<'a>,
}

impl<'a> Plays<'a> {
    pub(crate) fn new(player: ObjectType<'a>, role: RoleType<'a>) -> Self {
        Self { player, role }
    }

    pub fn player(&self) -> ObjectType<'a> {
        self.player.clone()
    }

    pub fn role(&self) -> RoleType<'a> {
        self.role.clone()
    }

    pub fn get_override<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, Option<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_overridden(snapshot, self.clone().into_owned())
    }

    pub fn set_override<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        overridden: Plays<'static>,
    ) -> Result<(), ConceptWriteError>{
        // TODO: Validation
        type_manager.set_plays_overridden(snapshot, self.clone().into_owned(), overridden)
    }

    fn into_owned(self) -> Plays<'static> {
        Plays { player: ObjectType::new(self.player.vertex().into_owned()), role: self.role.into_owned() }
    }
}

impl<'a> IntoCanonicalTypeEdge<'a> for Plays<'a> {
    fn as_type_edge(&self) -> TypeEdge<'static> {
        build_edge_plays(self.player.vertex().clone().into_owned(), self.role.vertex().clone().into_owned())
    }

    fn into_type_edge(self) -> TypeEdge<'static> {
        build_edge_plays(self.player.vertex().clone().into_owned(), self.role.vertex().clone().into_owned())
    }
}

// Can plays not be annotated?
pub struct __PlaceholderPlaysAnnotation {}

impl<'a> InterfaceEdge<'a> for Plays<'a> {
    type AnnotationType = __PlaceholderPlaysAnnotation;
    type ObjectType = ObjectType<'a>;
    type InterfaceType = RoleType<'a>;
    type Encoder = PlaysEncoder;

    fn new(player: ObjectType<'a>, role: RoleType<'a>) -> Self {
        Plays::new(player, role)
    }

    fn object(&self) -> ObjectType<'a> {
        self.player.clone()
    }

    fn interface(&self) -> RoleType<'a> {
        self.role.clone()
    }
}
