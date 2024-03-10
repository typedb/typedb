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

use std::cell::OnceCell;
use encoding::graph::type_::Root;

use encoding::graph::type_::vertex::TypeVertex;
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use encoding::primitive::label::Label;

use crate::ConceptAPI;
use crate::type_::entity_type::EntityType;
use crate::type_::type_manager::TypeManager;
use crate::type_::{AttributeTypeAPI, TypeAPI};

#[derive(Debug)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
    label: OnceCell<Label<'static>>,
    is_root: OnceCell<bool>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType {
        if vertex.prefix() != PrefixType::VertexAttributeType {
            panic!("Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                   PrefixType::VertexAttributeType, vertex.prefix())
        }
        AttributeType { vertex: vertex, label: OnceCell::new(), is_root: OnceCell::new() }
    }

    fn into_owned(self) -> AttributeType<'static> {
        let v = self.vertex.into_owned();
        AttributeType { vertex: v, label: self.label, is_root: self.is_root }
    }
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }

    fn get_label(&self, type_manager: &TypeManager) -> &Label {
        self.label.get_or_init(|| type_manager.get_storage_label(self.vertex()).unwrap())
    }

    fn set_label(&mut self, type_manager: &TypeManager, label: &Label) {
        type_manager.set_storage_label(self.vertex(), label);
        self.label = OnceCell::from(label.clone().into_owned());
    }

    fn is_root(&self, type_manager: &TypeManager) -> bool {
        *self.is_root.get_or_init(|| self.get_label(type_manager) == &Root::Attribute.label())
    }
}

impl<'a> AttributeTypeAPI<'a> for AttributeType<'a> {}

impl<'a> PartialEq<Self> for AttributeType<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.vertex.eq(other.vertex())
    }
}

impl<'a> Eq for AttributeType<'a> {}

//
// impl<'a> IIDAPI<'a> for AttributeType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }
//
// impl<'a> OwnedAPI<'a> for AttributeType<'a> {
//
// }
