/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, ops::Range};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{
    graph::{definition::r#struct::StructDefinition, type_::vertex::TypeVertex},
    layout::prefix::{Prefix, PrefixID},
    value::string_bytes::StringBytes,
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub(crate) trait IndexedType {
    const PREFIX: Prefix;
}

pub struct IdentifierToTypeIndex<'a, T: IndexedType> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
    indexed_identifier: PhantomData<T>,
}

impl<'a, T: IndexedType> IdentifierToTypeIndex<'a, T> {
    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        Self { bytes, indexed_identifier: PhantomData }
    }

    pub fn build<const INLINE_SIZE: usize>(identifier: StringBytes<INLINE_SIZE>) -> Self {
        let mut array = ByteArray::zeros(identifier.bytes().length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&T::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_identifier(identifier.bytes().length())]
            .copy_from_slice(identifier.bytes().bytes());
        Self { bytes: Bytes::Array(array), indexed_identifier: PhantomData }
    }

    pub fn identifier(&'a self) -> StringBytes<'a, BUFFER_KEY_INLINE> {
        StringBytes::new(Bytes::Reference(ByteReference::new(
            &self.bytes.bytes()[Self::range_identifier(self.identifier_length())],
        )))
    }

    fn identifier_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_identifier(identifier_length: usize) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + identifier_length
    }
}

impl<'a, T: IndexedType> AsBytes<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a, T: IndexedType> Keyable<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Schema
    }
}

impl<'a, T: IndexedType> Prefixed<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {}

// Specialisations
pub type LabelToTypeVertexIndex<'a> = IdentifierToTypeIndex<'a, TypeVertex<'static>>;
impl<'a> IndexedType for TypeVertex<'a> {
    const PREFIX: Prefix = Prefix::IndexLabelToType;
}

pub type LabelToStructDefinitionIndex<'a> = IdentifierToTypeIndex<'a, StructDefinition>;
impl IndexedType for StructDefinition {
    const PREFIX: Prefix = Prefix::IndexLabelToDefinitionStruct;
}
