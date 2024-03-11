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
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::{StorageKey, StorageKeyArray};
use storage::keyspace::keyspace::KeyspaceId;

use crate::{AsBytes, EncodingKeyspace, Keyable, Prefixed};
use crate::graph::type_::vertex::TypeVertex;
use crate::layout::prefix::{PrefixID, PrefixType};
use crate::primitive::label::Label;
use crate::primitive::string::StringBytes;

// TODO: maybe we want to either use Generics or Macros to implement all Properties the same way?

#[derive(Debug, PartialEq, Eq)]
pub struct TypeToLabelProperty<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> TypeToLabelProperty<'a> {
    const LENGTH: usize = PrefixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let property = TypeToLabelProperty { bytes: bytes };
        debug_assert_eq!(property.prefix(), PrefixType::PropertyTypeToLabel);
        property
    }

    pub fn build(vertex: TypeVertex<'_>) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::PropertyTypeToLabel.prefix_id().bytes().bytes());
        array.bytes_mut()[Self::range_type_vertex()].copy_from_slice(vertex.bytes().bytes());
        TypeToLabelProperty { bytes: ByteArrayOrRef::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { TypeToLabelProperty::LENGTH_PREFIX }> {
        let mut array = ByteArray::zeros(Self::LENGTH_PREFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::PropertyTypeToLabel.prefix_id().bytes().bytes());
        StorageKey::Array(StorageKeyArray::new(Self::keyspace_id(), array))
    }

    pub fn type_vertex(&'a self) -> TypeVertex<'a> {
        TypeVertex::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_type_vertex()])))
    }

    const fn keyspace_id() -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }

    const fn range_type_vertex() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + TypeVertex::LENGTH
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeToLabelProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }


    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeToLabelProperty<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        Self::keyspace_id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeToLabelProperty<'a> {}

pub struct LabelToTypeProperty<'a> {
    bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>,
}

impl<'a> LabelToTypeProperty<'a> {
    pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        LabelToTypeProperty { bytes: bytes }
    }

    pub fn build(label: &Label) -> Self {
        let label_string_bytes = label.scoped_name();
        let mut array = ByteArray::zeros(label_string_bytes.bytes().length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(PrefixType::PropertyLabelToType.prefix_id().bytes().bytes());
        array.bytes_mut()[Self::range_label(label_string_bytes.bytes().length())].copy_from_slice(label_string_bytes.bytes().bytes());
        LabelToTypeProperty { bytes: ByteArrayOrRef::Array(array) }
    }

    fn label(&'a self) -> StringBytes<'a, BUFFER_KEY_INLINE> {
        StringBytes::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_label(self.label_length())])))
    }

    fn label_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_label(label_length: usize) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + label_length
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for LabelToTypeProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for LabelToTypeProperty<'a> {
    fn keyspace_id(&self) -> KeyspaceId {
        EncodingKeyspace::Schema.id()
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for LabelToTypeProperty<'a> {}
