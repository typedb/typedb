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

use encoding::graph::type_::vertex::TypeVertex;
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use encoding::primitive::label::Label;
use storage::snapshot::snapshot::Snapshot;

use crate::ConceptAPI;
use crate::type_::TypeAPI;

#[derive(Debug, Eq, PartialEq)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
    label: OnceCell<Label<'static>>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType {
        if vertex.prefix() != PrefixType::VertexAttributeType {
            panic!("Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                   PrefixType::VertexAttributeType, vertex.prefix())
        }
        AttributeType { vertex: vertex, label: OnceCell::new() }
    }

    fn into_owned(self) -> AttributeType<'static> {
        let v = self.vertex.into_owned();
        AttributeType { vertex: v, label: self.label }
    }
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }

    fn get_label(&self, snapshot: &Snapshot) -> &Label {
        self.label.get_or_init(|| self._get_storage_label(snapshot).unwrap())
    }

    fn set_label(&mut self, label: &Label, snapshot: &Snapshot) {
        self._set_storage_label(label, snapshot);
        self.label = OnceCell::from(label.clone().into_owned());
    }
}
//
// impl<'a> AttributeTypeAPI<'a> for AttributeType<'a> {
//
// }
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
