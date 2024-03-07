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

use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::property::TypeToLabelProperty;
use encoding::graph::type_::vertex::TypeVertex;
use encoding::primitive::label::Label;
use encoding::primitive::string::StringBytes;
use resource::constants::encoding::LABEL_SCOPED_NAME_STRING_INLINE;
use storage::snapshot::snapshot::Snapshot;

use crate::{ConceptAPI, IIDAPI};
use crate::type_::attribute_type::AttributeType;
use crate::type_::owns::Owns;

pub mod attribute_type;
pub mod entity_type;
pub mod type_manager;
mod owns;
mod plays;

pub trait TypeAPI<'a>: ConceptAPI<'a> + Sized {
    fn vertex(&'a self) -> &TypeVertex<'a>;

    fn get_label(&self, snapshot: &Snapshot) -> &Label;

    fn _fetch_label(&'a self, snapshot: &Snapshot<'a>) -> Label<'static> {
        let key = TypeToLabelProperty::build(self.vertex());
        snapshot.get_mapped(key.into_storage_key().as_reference(), |reference| {
            let value = StringBytes::new(ByteArrayOrRef::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
            let as_str = value.decode();
            let mut splits = as_str.split(":");
            let first = splits.next().unwrap();
            if let Some(second) = splits.next() {
                Label::build_scoped(first, second)
            } else {
                Label::build(first)
            }
        }).unwrap()
    }

    // fn set_label(&self, label: &Label);
    //
    // fn get_supertype(&self) -> Option<Self>;
}

pub trait EntityTypeAPI<'a>: TypeAPI<'a> {

    // fn get_supertypes(&'a self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size

    // fn get_subtypes(&self) -> EntityTypeIterator<'static, 1>; // TODO: correct prefix size
}

pub trait AttributeTypeAPI<'a>: TypeAPI<'a> {}

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
