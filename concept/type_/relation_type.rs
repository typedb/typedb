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
use encoding::graph::type_::vertex::{new_vertex_relation_type, TypeVertex};
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use storage::key_value::StorageKeyReference;
use storage::snapshot::iterator::SnapshotPrefixIterator;

use crate::{concept_iterator, ConceptAPI};
use crate::error::{ConceptError, ConceptErrorKind};
use crate::type_::{RelationTypeAPI, TypeAPI};

#[derive(Debug, Clone)]
pub struct RelationType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RelationType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RelationType {
        if vertex.prefix() != PrefixType::VertexRelationType {
            panic!("Type IID prefix was expected to be Prefix::RelationType ({:?}) but was {:?}",
                   PrefixType::VertexRelationType, vertex.prefix())
        }
        RelationType { vertex: vertex }
    }

    fn into_owned(self) -> RelationType<'static> {
        let v = self.vertex.into_owned();
        RelationType { vertex: v }
    }
}

impl<'a> ConceptAPI<'a> for RelationType<'a> {}

impl<'a> TypeAPI<'a> for RelationType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }
}


impl<'a> RelationTypeAPI<'a> for RelationType<'a> {
    fn into_owned(self) -> RelationType<'static> {
        RelationType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> PartialEq<Self> for RelationType<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.vertex.eq(other.vertex())
    }
}

impl<'a> Eq for RelationType<'a> {}

// impl<'a> IIDAPI<'a> for RelationType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_relation_type<'a>(storage_key_ref: StorageKeyReference<'a>) -> RelationType<'a> {
    RelationType::new(new_vertex_relation_type(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
