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

use bytes::{byte_array::ByteArray, Bytes, byte_reference::ByteReference};
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{
    layout::prefix::{PrefixID, PrefixType},
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
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&PrefixType::IndexLabelToType.prefix_id().bytes());
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
    const KEYSPACE_ID: EncodingKeyspace = EncodingKeyspace::Schema;
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for LabelToTypeVertexIndex<'a> {}
