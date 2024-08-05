/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, ops::Range};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{
    graph::{
        definition::{function::FunctionDefinition, r#struct::StructDefinition},
        type_::vertex::TypeVertex,
    },
    layout::prefix::{Prefix, PrefixID},
    value::string_bytes::StringBytes,
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub trait Indexable {
    const INDEX_PREFIX: Prefix;
}

pub struct IdentifierIndex<'a, T: Indexable> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
    indexed_type: PhantomData<T>,
}

impl<'a, T: Indexable> IdentifierIndex<'a, T> {
    pub const FIXED_WIDTH_ENCODING: bool = false;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        Self { bytes, indexed_type: PhantomData }
    }

    pub fn build_prefix() -> Self {
        Self { bytes: Bytes::Array(ByteArray::copy(&T::INDEX_PREFIX.prefix_id().bytes())), indexed_type: PhantomData }
    }

    pub fn build<const INLINE_SIZE: usize>(identifier: StringBytes<INLINE_SIZE>) -> Self {
        let mut array = ByteArray::zeros(identifier.bytes().length() + PrefixID::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&T::INDEX_PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_identifier(identifier.bytes().length())]
            .copy_from_slice(identifier.bytes().bytes());
        Self { bytes: Bytes::Array(array), indexed_type: PhantomData }
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

impl<'a, T: Indexable> AsBytes<'a, BUFFER_KEY_INLINE> for IdentifierIndex<'a, T> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a, T: Indexable> Keyable<'a, BUFFER_KEY_INLINE> for IdentifierIndex<'a, T> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Schema
    }
}

impl<'a, T: Indexable> Prefixed<'a, BUFFER_KEY_INLINE> for IdentifierIndex<'a, T> {}

// Specialisations
pub type LabelToTypeVertexIndex<'a> = IdentifierIndex<'a, TypeVertex<'static>>;
impl<'a> Indexable for TypeVertex<'a> {
    const INDEX_PREFIX: Prefix = Prefix::IndexLabelToType;
}

pub type NameToStructDefinitionIndex<'a> = IdentifierIndex<'a, StructDefinition>;
impl Indexable for StructDefinition {
    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionStruct;
}

pub type NameToFunctionDefinitionIndex<'a> = IdentifierIndex<'a, FunctionDefinition<'a>>;
impl<'a> Indexable for FunctionDefinition<'a> {
    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionFunction;
}
