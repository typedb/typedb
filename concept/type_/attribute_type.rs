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

use crate::ConceptAPI;
use crate::type_::{AttributeTypeAPI, TypeAPI};
use crate::type_::annotation::AnnotationAbstract;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType {
        if vertex.prefix() != PrefixType::VertexAttributeType {
            panic!("Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                   PrefixType::VertexAttributeType, vertex.prefix())
        }
        AttributeType { vertex: vertex }
    }
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> AttributeTypeAPI<'a> for AttributeType<'a> {
    fn into_owned(self) -> AttributeType<'static> {
        AttributeType { vertex: self.vertex.into_owned() }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttributeTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<AnnotationAbstract> for AttributeTypeAnnotation {
    fn from(annotation: AnnotationAbstract) -> Self {
        AttributeTypeAnnotation::Abstract(annotation)
    }
}
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
