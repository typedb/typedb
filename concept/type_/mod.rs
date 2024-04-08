/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::HashSet;

use encoding::graph::type_::vertex::TypeVertex;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
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
    fn set_owns(
        &self,
        type_manager: &TypeManager<'_, impl WritableSnapshot>,
        attribute_type: AttributeType<'static>,
    );

    fn delete_owns(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, attribute_type: AttributeType<'static>);

    fn get_owns<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError>;

    fn get_owns_attribute(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError>;

    fn has_owns_attribute(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_owns_attribute(type_manager, attribute_type)?.is_some())
    }
}

pub trait PlayerAPI<'a> {
    fn set_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>);

    fn delete_plays(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, role_type: RoleType<'static>);

    fn get_plays<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError>;

    fn get_plays_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError>;

    fn has_plays_role(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
        role_type: RoleType<'static>,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_plays_role(type_manager, role_type)?.is_some())
    }
}
