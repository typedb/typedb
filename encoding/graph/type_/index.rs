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
    value::{label::Label, string_bytes::StringBytes},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub(crate) trait IndexedKey {
    const PREFIX: Prefix;
}

pub struct IdentifierToTypeIndex<'a, T: IndexedKey> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
    indexed_identifier: PhantomData<T>,
}

impl<'a, T: IndexedKey> IdentifierToTypeIndex<'a, T> {
    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        Self { bytes, indexed_identifier: PhantomData }
    }

    pub fn build(label: &Label) -> Self {
        let label_string_bytes = label.scoped_name();
        let mut array = ByteArray::zeros(label_string_bytes.bytes().length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&T::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_label(label_string_bytes.bytes().length())]
            .copy_from_slice(label_string_bytes.bytes().bytes());
        Self { bytes: Bytes::Array(array), indexed_identifier: PhantomData }
    }

    pub fn label(&'a self) -> StringBytes<'a, BUFFER_KEY_INLINE> {
        StringBytes::new(Bytes::Reference(ByteReference::new(
            &self.bytes.bytes()[Self::range_label(self.label_length())],
        )))
    }

    fn label_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_label(label_length: usize) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + label_length
    }
}

impl<'a, T: IndexedKey> AsBytes<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a, T: IndexedKey> Keyable<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Schema
    }
}

impl<'a, T: IndexedKey> Prefixed<'a, BUFFER_KEY_INLINE> for IdentifierToTypeIndex<'a, T> {}

// Specialisations
pub type LabelToTypeVertexIndex<'a> = IdentifierToTypeIndex<'a, TypeVertex<'static>>;
impl<'a> IndexedKey for TypeVertex<'a> {
    const PREFIX: Prefix = Prefix::IndexLabelToType;
}

pub type LabelToStructDefinitionIndex<'a> = IdentifierToTypeIndex<'a, StructDefinition>;
impl IndexedKey for StructDefinition {
    const PREFIX: Prefix = Prefix::IndexLabelToDefinitionStruct;
}
