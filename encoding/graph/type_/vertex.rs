/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, hash::Hash};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::{StorageKey, StorageKeyArray},
    keyspace::KeyspaceSet,
};

use crate::{
    error::EncodingError,
    graph::Typed,
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

// TODO: we could make all Type constructs contain plain byte arrays, since they will always be 64 bytes (BUFFER_KEY_INLINE), then make Types all Copy
//       However, we should benchmark this first, since 64 bytes may be better off referenced

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TypeVertex {
    value: u32,
}

impl fmt::Debug for TypeVertex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:06X}", self.value)
    }
}

impl TypeVertex {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub const fn new(prefix: PrefixID, type_id: TypeID) -> Self {
        let prefix = (prefix.byte as u32) << 16;
        let type_id = type_id.value as u32;
        Self { value: prefix + type_id }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let mut be_bytes = [0; 4];
        be_bytes[1..].copy_from_slice(&bytes);
        Self { value: u32::from_be_bytes(be_bytes) }
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for TypeVertex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        Bytes::Array(ByteArray::copy(&self.value.to_be_bytes()[1..]))
    }
}

impl Keyable<BUFFER_KEY_INLINE> for TypeVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }

    fn into_storage_key(self) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        StorageKey::new(self.keyspace(), self.to_bytes())
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for TypeVertex {}

impl Typed<BUFFER_KEY_INLINE> for TypeVertex {
    fn type_id_(&self) -> TypeID {
        debug_assert_eq!(Self::LENGTH, 3);
        debug_assert_eq!(Self::RANGE_TYPE_ID, 1..3);
        TypeID { value: (self.value & 0xFFFF) as u16 }
    }
}

impl primitive::prefix::Prefix for TypeVertex {
    fn starts_with(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.value == other.value
    }
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeID {
    value: u16,
}

impl fmt::Debug for TypeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04X}", self.value)
    }
}

// TODO: this type should move into constants.rs, similarly to DefinitionIDUInt
pub type TypeIDUInt = u16;

impl TypeID {
    pub const MIN: Self = Self::new(TypeIDUInt::MIN);
    pub const MAX: Self = Self::new(TypeIDUInt::MAX);
    pub(crate) const LENGTH: usize = std::mem::size_of::<TypeIDUInt>();

    pub const fn new(id: TypeIDUInt) -> Self {
        Self { value: id }
    }

    pub fn decode(bytes: [u8; TypeID::LENGTH]) -> TypeID {
        TypeID { value: TypeIDUInt::from_be_bytes(bytes) }
    }

    pub fn as_u16(&self) -> u16 {
        self.value
    }

    pub fn to_bytes(&self) -> [u8; TypeID::LENGTH] {
        self.value.to_be_bytes()
    }
}

impl fmt::Display for TypeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(&self.to_bytes()))
    }
}

// Encoder traits
pub(crate) fn build_type_vertex_prefix_key(prefix: Prefix) -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
    StorageKey::new(
        TypeVertex::KEYSPACE,
        // TODO: Can we revert this to being a static const byte reference
        Bytes::Array(ByteArray::inline(prefix.prefix_id().to_bytes(), TypeVertex::LENGTH_PREFIX)),
    )
}

pub trait TypeVertexEncoding: Sized {
    fn from_vertex(vertex: TypeVertex) -> Result<Self, EncodingError>;

    fn from_bytes(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Result<Self, EncodingError> {
        Self::from_vertex(TypeVertex::decode(bytes))
    }

    fn vertex(&self) -> TypeVertex;

    fn into_vertex(self) -> TypeVertex;
}

pub trait PrefixedTypeVertexEncoding: TypeVertexEncoding {
    const PREFIX: Prefix;

    fn build_from_type_id(type_id: TypeID) -> Self {
        Self::from_vertex(TypeVertex::new(Self::PREFIX.prefix_id(), type_id)).unwrap()
    }

    fn prefix_for_kind() -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
        build_type_vertex_prefix_key(Self::PREFIX)
    }

    fn is_decodable_from_key(key: &StorageKeyArray<BUFFER_KEY_INLINE>) -> bool {
        key.keyspace_id() == EncodingKeyspace::DefaultOptimisedPrefix11.id()
            && Self::is_decodable_from(Bytes::Reference(key.bytes()))
    }

    fn is_decodable_from(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        bytes.length() == TypeVertex::LENGTH && TypeVertex::decode(bytes).prefix() == Self::PREFIX
    }
}
