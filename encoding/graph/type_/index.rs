/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, ops::Range};

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;

use crate::{
    graph::{
        definition::{function::FunctionDefinition, r#struct::StructDefinition},
        type_::vertex::TypeVertex,
    },
    layout::prefix::{Prefix, PrefixID},
    value::{label::Label, string_bytes::StringBytes},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

pub trait Indexable: Clone {
    type KeyType<'a>;
    const INDEX_PREFIX: Prefix;
    fn key_type_to_identifier(key: Self::KeyType<'_>) -> StringBytes<'_, BUFFER_KEY_INLINE>;
}

#[derive(Clone, Debug)]
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

    pub fn build(key: T::KeyType<'_>) -> Self {
        let identifier = T::key_type_to_identifier(key);
        let mut array = ByteArray::zeros(identifier.bytes().len() + PrefixID::LENGTH);
        array[Self::RANGE_PREFIX].copy_from_slice(&T::INDEX_PREFIX.prefix_id().bytes());
        array[Self::range_identifier(identifier.bytes().len())].copy_from_slice(identifier.bytes());
        Self { bytes: Bytes::Array(array), indexed_type: PhantomData }
    }

    pub fn identifier(&'a self) -> StringBytes<'a, BUFFER_KEY_INLINE> {
        StringBytes::new(Bytes::reference(&self.bytes[Self::range_identifier(self.identifier_length())]))
    }

    fn identifier_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_identifier(identifier_length: usize) -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + identifier_length
    }
}

impl<'a, T: Indexable> AsBytes<'a, BUFFER_KEY_INLINE> for IdentifierIndex<'a, T> {
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
pub type LabelToTypeVertexIndex<'a> = IdentifierIndex<'a, TypeVertex>;
impl<'a> Indexable for TypeVertex {
    type KeyType<'b> = &'b Label<'b>;
    const INDEX_PREFIX: Prefix = Prefix::IndexLabelToType;

    fn key_type_to_identifier(key: Self::KeyType<'_>) -> StringBytes<'_, BUFFER_KEY_INLINE> {
        if let Some(scope) = &key.scope {
            let mut vec = Vec::with_capacity(key.name.length() + 1 + scope.length());
            vec.extend_from_slice(key.name.bytes());
            vec.push(b':');
            vec.extend_from_slice(scope.bytes());
            StringBytes::new(Bytes::copy(vec.as_slice()))
        } else {
            StringBytes::new(Bytes::copy(key.name.bytes()))
        }
    }
}

pub type NameToStructDefinitionIndex<'a> = IdentifierIndex<'a, StructDefinition>;
impl Indexable for StructDefinition {
    type KeyType<'b> = &'b str;
    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionStruct;

    fn key_type_to_identifier(key: Self::KeyType<'_>) -> StringBytes<'_, BUFFER_KEY_INLINE> {
        StringBytes::build_owned(key)
    }
}

pub type NameToFunctionDefinitionIndex<'a> = IdentifierIndex<'a, FunctionDefinition<'a>>;
impl<'a> Indexable for FunctionDefinition<'a> {
    type KeyType<'b> = &'b str;

    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionFunction;

    fn key_type_to_identifier(key: Self::KeyType<'_>) -> StringBytes<'_, BUFFER_KEY_INLINE> {
        StringBytes::build_owned(key)
    }
}
