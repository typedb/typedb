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

use encoding::graph::type_::vertex::TypeVertex;
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;

use crate::{Concept, Type};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType {
        if vertex.prefix() != PrefixType::AttributeType.prefix() {
            panic!("Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                   PrefixType::AttributeType.prefix(), vertex.prefix())
        }
        AttributeType { vertex: vertex }
    }

    fn into_owned(self) -> AttributeType<'static> {
        let v = self.vertex.into_owned();
        AttributeType { vertex: v }
    }
}

impl<'a> Concept<'a> for AttributeType<'a> {}

impl<'a> Type<'a> for AttributeType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }
}

