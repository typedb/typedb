/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::graph::type_::vertex::TypeVertex;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{attribute_type::AttributeType, owns::Owns, plays::Plays, role_type::RoleType, type_manager::TypeManager},
    ConceptAPI,
};

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

pub trait TypeAPI<'a>: ConceptAPI<'a> + Sized + Clone {
    fn vertex<'this>(&'this self) -> TypeVertex<'this>;

    fn into_vertex(self) -> TypeVertex<'a>;
}

pub trait ObjectTypeAPI<'a>: TypeAPI<'a> {}

pub trait OwnerAPI<'a> {
    fn set_owns(&self, type_manager: &TypeManager<impl WritableSnapshot>, attribute_type: AttributeType<'static>);

    fn delete_owns(&self, type_manager: &TypeManager<impl WritableSnapshot>, attribute_type: AttributeType<'static>);

    fn get_owns<'m>(
        &self,
        type_manager: &'m TypeManager<impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owns_attribute(
        &self,
        type_manager: &TypeManager<impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError>;

    fn has_owns_attribute(
        &self,
        type_manager: &TypeManager<impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute(type_manager, attribute_type)?.is_some())
    }
}

pub trait PlayerAPI<'a> {
    fn set_plays(&self, type_manager: &TypeManager<impl WritableSnapshot>, role_type: RoleType<'static>);

    fn delete_plays(&self, type_manager: &TypeManager<impl WritableSnapshot>, role_type: RoleType<'static>);

    fn get_plays<'m>(
        &self,
        type_manager: &'m TypeManager<impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_plays_role(
        &self,
        type_manager: &TypeManager<impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError>;

    fn has_plays_role(
        &self,
        type_manager: &TypeManager<impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role(type_manager, role_type)?.is_some())
    }
}
