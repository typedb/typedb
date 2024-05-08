/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::edge::{build_edge_owns, build_edge_plays, TypeEdge};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use crate::error::ConceptReadError;
use crate::type_::{IntoCanonicalTypeEdge, TypeAPI};
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::relates::Relates;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::TypeManager;

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
        type_manager: &'this TypeManager<Snapshot>
    ) -> Result<MaybeOwns<'this, Option<Plays<'static>>>, ConceptReadError> {
        type_manager.get_plays_overridden(snapshot, self.clone().into_owned())
    }

    pub fn set_override<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        overridden: Plays<'static>
    ) {
        // TODO: Validation
        type_manager.storage_set_plays_overridden(snapshot, self.clone().into_owned(), overridden)
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
