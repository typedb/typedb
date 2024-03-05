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

use std::ops::Range;

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use bytes::byte_reference::ByteReference;
use storage::key_value::StorageKey;
use storage::keyspace::keyspace::KeyspaceId;
use storage::snapshot::buffer::BUFFER_INLINE_KEY;
use crate::{AsBytes, EncodingKeyspace, Keyable, Prefixed};

use crate::graph::type_::vertex::TypeVertex;
use crate::layout::prefix::{PrefixID, PrefixType};
use crate::primitive::string::StringBytes;

// TODO: maybe we want to either use Generics or Macros to implement all Index'es the same way?

#[derive(Debug, PartialEq, Eq)]
pub struct TypeToLabelIndex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>,
}

impl<'a> TypeToLabelIndex<'a> {
    const LENGTH: usize = PrefixID::LENGTH + TypeVertex::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeToLabelIndex { bytes: bytes }
    }

    pub fn build_key_value(vertex: &TypeVertex<'a>, label: &'a str) -> (Self, StringBytes<'a>) {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::TypeToLabelIndex.prefix().bytes().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        (TypeToLabelIndex { bytes: ByteArrayOrRef::Array(array) }, StringBytes::build_ref(label))
    }

    const fn range_type_vertex() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_INLINE_KEY> for TypeToLabelIndex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }


    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_KEY> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_INLINE_KEY> for TypeToLabelIndex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }
}

impl<'a> Prefixed<'a, BUFFER_INLINE_KEY> for TypeToLabelIndex<'a> {}

pub struct LabelToTypeIndex<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>,
}

impl<'a> LabelToTypeIndex<'a> {
    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        LabelToTypeIndex { bytes: bytes }
    }

    pub fn build(label: &str) -> Self {
        let label_bytes = StringBytes::build(&label);
        let mut array = ByteArray::zeros(label_bytes.length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::LabelToTypeIndex.prefix().bytes().bytes());
        array.bytes_mut()[Self::range_label(label_bytes.length())].copy_from_slice(label_bytes.bytes().bytes());
        LabelToTypeIndex { bytes: ByteArrayOrRef::Array(array) }
    }

    fn label(&'a self) -> StringBytes<'a> {
        StringBytes::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_label(self.label_length())])))
    }

    fn label_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_label(label_length: usize) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + label_length
    }
}

impl<'a> AsBytes<'a, BUFFER_INLINE_KEY> for LabelToTypeIndex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_KEY> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_INLINE_KEY> for LabelToTypeIndex<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }
}

impl<'a> Prefixed<'a, BUFFER_INLINE_KEY> for LabelToTypeIndex<'a> { }
