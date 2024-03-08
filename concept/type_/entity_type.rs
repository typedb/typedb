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

use bytes::byte_array_or_ref::ByteArrayOrRef;
use encoding::graph::type_::Root;
use encoding::graph::type_::vertex::{new_entity_type_vertex, TypeVertex};
use encoding::layout::prefix::PrefixType;
use encoding::Prefixed;
use encoding::primitive::label::Label;
use storage::key_value::StorageKeyReference;
use storage::snapshot::iterator::SnapshotPrefixIterator;

use crate::{concept_iterator, ConceptAPI};
use crate::error::{ConceptError, ConceptErrorKind};
use crate::type_::type_manager::TypeManager;
use crate::type_::TypeAPI;

#[derive(Debug, Eq, PartialEq)]
pub struct EntityType<'a> {
    vertex: TypeVertex<'a>,
    label: OnceCell<Label<'static>>,
    is_root: OnceCell<bool>,
}

impl<'a> EntityType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> EntityType {
        if vertex.prefix() != PrefixType::VertexEntityType {
            panic!("Type IID prefix was expected to be Prefix::EntityType ({:?}) but was {:?}",
                   PrefixType::VertexEntityType, vertex.prefix())
        }
        EntityType { vertex: vertex, label: OnceCell::new(), is_root: OnceCell::new(), }
    }

    fn into_owned(self) -> EntityType<'static> {
        let v = self.vertex.into_owned();
        EntityType { vertex: v, label: self.label, is_root: self.is_root, }
    }
}

impl<'a> ConceptAPI<'a> for EntityType<'a> {}

impl<'a> TypeAPI<'a> for EntityType<'a> {
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

//
// impl<'a> EntityTypeAPI<'a> for EntityType<'a> {
//
// }

// impl<'a> IIDAPI<'a> for EntityType<'a> {
//     fn iid(&'a self) -> ByteReference<'a> {
//         self.vertex.bytes()
//     }
// }

// TODO: can we inline this into the macro invocation?
fn storage_key_to_entity_type<'a>(storage_key_ref: StorageKeyReference<'a>) -> EntityType<'a> {
    EntityType::new(new_entity_type_vertex(ByteArrayOrRef::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(EntityTypeIterator, EntityType, storage_key_to_entity_type);
