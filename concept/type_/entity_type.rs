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
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> EntityType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> EntityType {
        if vertex.prefix() != PrefixType::EntityType.prefix() {
            panic!("Type IID prefix was expected to be Prefix::EntityType ({:?}) but was {:?}",
                   PrefixType::EntityType.prefix(), vertex.prefix())
        }
        EntityType { vertex: vertex }
    }

    fn into_owned<'b>(self) -> EntityType<'static> {
        let v = self.vertex.into_owned();
        EntityType { vertex: v }
    }
}

impl<'a> Concept<'a> for EntityType<'a> { }

impl<'a> Type<'a> for EntityType<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a> {
        &self.vertex
    }
}

