/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, ops::Range};

use bytes::{Bytes, byte_array::ByteArray, util::HexBytesFormatter};
use resource::constants::{encoding::DefinitionIDUInt, snapshot::BUFFER_KEY_INLINE};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Visitor},
};
use storage::key_value::StorageKey;

use crate::{
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
    layout::prefix::{Prefix, PrefixID},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DefinitionKey {
    prefix: Prefix,
    definition_id: DefinitionID,
}

impl DefinitionKey {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub(crate) const LENGTH: usize = PrefixID::LENGTH + DefinitionID::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    pub(crate) const RANGE_DEFINITION_ID: Range<usize> =
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + DefinitionID::LENGTH;

    pub fn new(prefix: Prefix, definition_id: DefinitionID) -> Self {
        Self { prefix, definition_id }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        Self {
            prefix: Prefix::from_prefix_id(PrefixID::new(bytes[Self::INDEX_PREFIX])).unwrap(),
            definition_id: DefinitionID::decode(bytes[Self::RANGE_DEFINITION_ID].try_into().unwrap()),
        }
    }

    pub fn definition_id(&self) -> DefinitionID {
        self.definition_id
    }

    pub fn build_prefix(prefix: Prefix) -> StorageKey<'static, { DefinitionKey::LENGTH_PREFIX }> {
        StorageKey::new(
            DefinitionKey::KEYSPACE,
            // TODO: Can we use a static const byte reference
            Bytes::Array(ByteArray::inline(prefix.prefix_id().to_bytes(), DefinitionKey::LENGTH_PREFIX)),
        )
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        let mut array = [0; 3];
        array[Self::INDEX_PREFIX] = self.prefix.prefix_id().byte;
        array[Self::RANGE_DEFINITION_ID].copy_from_slice(&self.definition_id.bytes());
        array
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for DefinitionKey {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        Bytes::Array(ByteArray::copy(&self.bytes()))
    }
}

impl Keyable<BUFFER_KEY_INLINE> for DefinitionKey {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for DefinitionKey {}

impl fmt::Display for DefinitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(&self.bytes()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DefinitionID {
    id: u16,
}

impl DefinitionID {
    pub(crate) const LENGTH: usize = std::mem::size_of::<DefinitionIDUInt>();

    pub fn decode(bytes: [u8; Self::LENGTH]) -> DefinitionID {
        DefinitionID { id: DefinitionIDUInt::from_be_bytes(bytes) }
    }

    pub fn new(id: DefinitionIDUInt) -> Self {
        debug_assert_eq!(std::mem::size_of_val(&id), DefinitionID::LENGTH);
        DefinitionID { id }
    }

    pub fn as_uint(&self) -> DefinitionIDUInt {
        self.id
    }

    pub fn bytes(&self) -> [u8; DefinitionID::LENGTH] {
        self.id.to_be_bytes()
    }
}

impl Serialize for DefinitionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.bytes())
    }
}

impl<'de> Deserialize<'de> for DefinitionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        pub struct DefinitionKeyVisitor;
        impl Visitor<'_> for DefinitionKeyVisitor {
            type Value = DefinitionKey;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("`DefinitionKey`")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DefinitionKey::decode(Bytes::Reference(v)))
            }
        }

        deserializer.deserialize_bytes(DefinitionKeyVisitor)
    }
}
