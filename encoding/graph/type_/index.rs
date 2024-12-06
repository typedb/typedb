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
    type KeyType: ?Sized;
    const INDEX_PREFIX: Prefix;
    fn key_type_to_identifier(key: &Self::KeyType) -> StringBytes<BUFFER_KEY_INLINE>;
}

#[derive(Clone, Debug)]
pub struct IdentifierIndex<T: Indexable> {
    bytes: Bytes<'static, BUFFER_KEY_INLINE>,
    indexed_type: PhantomData<T>,
}

impl<T: Indexable> IdentifierIndex<T> {
    pub const FIXED_WIDTH_ENCODING: bool = false;

    pub fn new(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= PrefixID::LENGTH);
        Self { bytes: Bytes::copy(&bytes), indexed_type: PhantomData }
    }

    pub fn build_prefix() -> Self {
        Self {
            bytes: Bytes::Array(ByteArray::copy(&T::INDEX_PREFIX.prefix_id().to_bytes())),
            indexed_type: PhantomData,
        }
    }

    pub fn build(key: &T::KeyType) -> Self {
        let identifier = T::key_type_to_identifier(key);
        let mut array = ByteArray::zeros(identifier.bytes().len() + PrefixID::LENGTH);
        array[Self::INDEX_PREFIX] = T::INDEX_PREFIX.prefix_id().byte;
        array[Self::range_identifier(identifier.bytes().len())].copy_from_slice(identifier.bytes());
        Self { bytes: Bytes::Array(array), indexed_type: PhantomData }
    }

    pub fn identifier(&self) -> StringBytes<BUFFER_KEY_INLINE> {
        StringBytes::new(Bytes::copy(&self.bytes[Self::range_identifier(self.identifier_length())]))
    }

    fn identifier_length(&self) -> usize {
        self.bytes.length() - PrefixID::LENGTH
    }

    fn range_identifier(identifier_length: usize) -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + identifier_length
    }
}

impl<T: Indexable> AsBytes<BUFFER_KEY_INLINE> for IdentifierIndex<T> {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<T: Indexable> Keyable<BUFFER_KEY_INLINE> for IdentifierIndex<T> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::DefaultOptimisedPrefix11
    }
}

impl<T: Indexable> Prefixed<BUFFER_KEY_INLINE> for IdentifierIndex<T> {}

// Specialisations
pub type LabelToTypeVertexIndex = IdentifierIndex<TypeVertex>;
impl Indexable for TypeVertex {
    type KeyType = Label;
    const INDEX_PREFIX: Prefix = Prefix::IndexLabelToType;

    fn key_type_to_identifier(key: &Self::KeyType) -> StringBytes<BUFFER_KEY_INLINE> {
        if let Some(scope) = &key.scope {
            let mut vec = Vec::with_capacity(key.name.len() + 1 + scope.len());
            vec.extend_from_slice(key.name.bytes());
            vec.push(b':');
            vec.extend_from_slice(scope.bytes());
            StringBytes::new(Bytes::copy(vec.as_slice()))
        } else {
            StringBytes::new(Bytes::copy(key.name.bytes()))
        }
    }
}

pub type NameToStructDefinitionIndex = IdentifierIndex<StructDefinition>;
impl Indexable for StructDefinition {
    type KeyType = str;
    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionStruct;

    fn key_type_to_identifier(key: &Self::KeyType) -> StringBytes<BUFFER_KEY_INLINE> {
        StringBytes::build_owned(key)
    }
}

pub type NameToFunctionDefinitionIndex = IdentifierIndex<FunctionDefinition>;
impl Indexable for FunctionDefinition {
    type KeyType = str;

    const INDEX_PREFIX: Prefix = Prefix::IndexNameToDefinitionFunction;

    fn key_type_to_identifier(key: &Self::KeyType) -> StringBytes<BUFFER_KEY_INLINE> {
        StringBytes::build_owned(key)
    }
}
