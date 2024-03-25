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

use std::ops::Deref;

use encoding::graph::type_::vertex::TypeVertex;
use primitive::maybe_owns::MaybeOwns;
use std::collections::HashSet;

use crate::ConceptAPI;
use crate::type_::attribute_type::AttributeType;
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::TypeManager;

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
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a>;

    fn into_vertex(self) -> TypeVertex<'a>;
}

pub trait OwnerAPI<'a> {
    fn set_owns<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) -> Owns<'static>;

    fn delete_owns<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>);

    fn get_owns<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Owns<'static>>>;

    fn get_owns_attribute<D>(&self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) -> Option<Owns<'static>>;

    fn has_owns_attribute<D>( &self, type_manager: &TypeManager<'_, '_, D>, attribute_type: AttributeType<'static>) -> bool {
        self.get_owns_attribute(type_manager, attribute_type).is_some()
    }
}


// --- Player API ---
pub trait PlayerAPI<'a> {
    fn set_plays<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) -> Plays<'static>;

    fn delete_plays<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>);

    fn get_plays<'m, D>(&self, type_manager: &'m TypeManager<'_, '_, D>) -> MaybeOwns<'m, HashSet<Plays<'static>>>;

    fn get_plays_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) -> Option<Plays<'static>>;

    fn has_plays_role<D>(&self, type_manager: &TypeManager<'_, '_, D>, role_type: RoleType<'static>) -> bool {
        self.get_plays_role(type_manager, role_type).is_some()
    }
}
