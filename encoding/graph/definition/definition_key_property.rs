/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{convert::TryInto, io::Read, ops::Range};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    graph::definition::definition_key::DefinitionKey,
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct DefinitionProperty<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> DefinitionProperty<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    const PREFIX: Prefix = Prefix::PropertyDefinition;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + DefinitionKey::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_DEFINITION: usize = PrefixID::LENGTH + DefinitionKey::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        let property = DefinitionProperty { bytes };
        debug_assert_eq!(property.prefix(), Self::PREFIX);
        property
    }

    pub fn build(definition_key: DefinitionKey<'_>, infix: Infix) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_definition_key()].copy_from_slice(definition_key.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        DefinitionProperty { bytes: Bytes::Array(array) }
    }

    fn build_suffixed<const INLINE_BYTES: usize>(
        definition_key: DefinitionProperty<'_>,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + suffix.length());
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        array.bytes_mut()[Self::range_definition_key()].copy_from_slice(definition_key.bytes().bytes());
        array.bytes_mut()[Self::range_infix()].copy_from_slice(&infix.infix_id().bytes());
        array.bytes_mut()[Self::range_suffix(suffix.length())].copy_from_slice(suffix.bytes());
        DefinitionProperty { bytes: Bytes::Array(array) }
    }

    pub fn build_prefix() -> StorageKey<'static, { DefinitionProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = DefinitionProperty::PREFIX.prefix_id().bytes();
        StorageKey::new_ref(Self::KEYSPACE, ByteReference::new(&PREFIX_BYTES))
    }

    pub fn definition_key(&'a self) -> DefinitionKey<'a> {
        DefinitionKey::new(Bytes::Reference(ByteReference::new(&self.bytes().bytes()[Self::range_definition_key()])))
    }

    pub fn infix(&self) -> Infix {
        let infix_bytes = &self.bytes.bytes()[Self::range_infix()];
        Infix::from_infix_id(InfixID::new(infix_bytes.try_into().unwrap()))
    }

    fn suffix_length(&self) -> usize {
        self.bytes().length() - Self::LENGTH_NO_SUFFIX
    }

    pub fn suffix(&self) -> Option<ByteReference> {
        let suffix_length = self.suffix_length();
        if suffix_length > 0 {
            Some(ByteReference::new(&self.bytes.bytes()[Self::range_suffix(self.suffix_length())]))
        } else {
            None
        }
    }

    const fn range_definition_key() -> Range<usize> {
        Self::RANGE_PREFIX.end..Self::RANGE_PREFIX.end + DefinitionKey::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_definition_key().end..Self::range_definition_key().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for DefinitionProperty<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for DefinitionProperty<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for DefinitionProperty<'a> {}
