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

use encoding::{AsBytes, Keyable};
use encoding::graph::type_::vertex::TypeVertex;
use encoding::primitive::label::Label;
use primitive::maybe_owns::MaybeOwns;

use crate::ConceptAPI;
use crate::type_::attribute_type::AttributeType;
use crate::type_::owns::Owns;
use crate::type_::type_manager::TypeManager;

pub mod attribute_type;
pub mod relation_type;
pub mod entity_type;
pub mod type_manager;
mod owns;
mod plays;
mod sub;
mod type_cache;

pub trait TypeAPI<'a>: ConceptAPI<'a> + Sized {
    fn vertex(&'a self) -> &TypeVertex<'a>;

    fn get_label(&self, type_manager: &TypeManager) -> &Label;

    fn set_label(&mut self, type_manager: &TypeManager, label: &Label);

    fn is_root(&self, type_manager: &TypeManager) -> bool;

}

pub trait EntityTypeAPI<'a>: TypeAPI<'a> {

    fn get_supertype<'b>(&self, type_manager: &'b TypeManager) -> Option<MaybeOwns<'a, Self>> {
        // type_manager.get_entity_type_supertype(self)
        todo!()
    }

    // fn get_supertypes(&'a self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size

    // fn get_subtypes(&self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size
}

pub trait RelationTypeAPI<'a>: TypeAPI<'a> {

    fn get_supertype<'b>(&self, type_manager: &'b TypeManager) -> Option<MaybeOwns<'a, Self>> {
        // type_manager.get_relation_type_supertype(self.vertex())
        todo!()
    }

    // fn get_supertypes(&'a self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size

    // fn get_subtypes(&self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size
}

pub trait AttributeTypeAPI<'a>: TypeAPI<'a> {

    fn get_supertype<'b>(&self, type_manager: &'b TypeManager) -> Option<MaybeOwns<'a, Self>> {
        // type_manager.get_attribute_type_supertype(self.vertex())
        todo!()
    }
}

trait OwnerAPI<'a>: TypeAPI<'a> {
    fn create_owns(&self, attribute_type: &AttributeType) -> Owns {
        // create Owns
        todo!()
    }

    fn get_owns(&self) {
        // fetch iterator of Owns
        todo!()
    }

    fn get_owns_owned(&self) {
        // fetch iterator of owned attribute types
        todo!()
    }

    fn has_owns_owned(&self, attribute_type: &AttributeType) -> bool {
        todo!()
    }
}

trait OwnedAPI<'a>: AttributeTypeAPI<'a> {
    fn get_owns(&self) {
        // return iterator of Owns
        todo!()
    }

    fn get_owns_owners(&self) {
        // return iterator of Owns
        todo!()
    }
}

trait PlayerAPI<'a>: TypeAPI<'a> {

    // fn create_plays(&self, role_type: &RoleType) -> Plays;

    fn get_plays(&self) {
        // return iterator of Plays
        todo!()
    }

    fn get_plays_played(&self) {
        // return iterator of played types
        todo!()
    }

    // fn has_plays_played(&self, role_type: &RoleType);
}

trait PlayedAPI<'a>: TypeAPI<'a> {
    fn get_plays(&self) {
        // return iterator of Plays
        todo!()
    }

    fn get_plays_players(&self) {
        // return iterator of player types
        todo!()
    }
}
