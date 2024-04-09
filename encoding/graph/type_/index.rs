/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::Range;

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{
    layout::prefix::{PrefixID, Prefix},
    value::{label::Label, string::StringBytes},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub struct LabelToTypeVertexIndex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> LabelToTypeVertexIndex<'a> {
    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        LabelToTypeVertexIndex { bytes }
    }

    pub fn build(label: &Label) -> Self {
        let label_string_bytes = label.scoped_name();
        let mut array = ByteArray::zeros(label_string_bytes.bytes().length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Prefix::IndexLabelToType.prefix_id().bytes());
        array.bytes_mut()[Self::range_label(label_string_bytes.bytes().length())]
            .copy_from_slice(label_string_bytes.bytes().bytes());
        LabelToTypeVertexIndex { bytes: Bytes::Array(array) }
    }

    fn label(&'a self) -> StringBytes<'a, BUFFER_KEY_INLINE> {
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

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for LabelToTypeVertexIndex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for LabelToTypeVertexIndex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Schema
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for LabelToTypeVertexIndex<'a> {}
