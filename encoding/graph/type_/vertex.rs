/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, hash::Hash};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::{StorageKey, StorageKeyReference},
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TypeVertex {
    value: u32,
}

impl TypeVertex {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let mut be_bytes = [0; 4];
        be_bytes[1..].copy_from_slice(&bytes);
        Self { value: u32::from_be_bytes(be_bytes) }
    }

    pub(crate) fn build(prefix: PrefixID, type_id: TypeID) -> Self {
        let prefix = (prefix.bytes[0] as u32) << 16;
        let type_id = u16::from_be_bytes(type_id.bytes) as u32;
        Self { value: prefix + type_id }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeVertex {
    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        Bytes::Array(ByteArray::copy(&self.value.to_be_bytes()[1..]))
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertex {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for TypeVertex {}

impl primitive::prefix::Prefix for TypeVertex {
    fn starts_with(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn into_starts_with(self, other: Self) -> bool {
        self.value == other.value
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TypeID {
    bytes: [u8; TypeID::LENGTH],
}

// TODO: this type should move into constants.rs, similarly to DefinitionIDUInt
pub type TypeIDUInt = u16;

impl TypeID {
    pub(crate) const LENGTH: usize = std::mem::size_of::<TypeIDUInt>();

    pub fn new(bytes: [u8; TypeID::LENGTH]) -> TypeID {
        TypeID { bytes }
    }

    pub fn build(id: TypeIDUInt) -> Self {
        debug_assert_eq!(std::mem::size_of_val(&id), TypeID::LENGTH);
        TypeID { bytes: id.to_be_bytes() }
    }

    pub fn as_u16(&self) -> u16 {
        u16::from_be_bytes(self.bytes)
    }

    pub fn bytes(&self) -> [u8; TypeID::LENGTH] {
        self.bytes
    }
}

impl fmt::Display for TypeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(&self.bytes()))
    }
}

// Encoder traits
pub(crate) fn build_type_vertex_prefix_key(prefix: Prefix) -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
    StorageKey::new(
        TypeVertex::KEYSPACE,
        // TODO: Can we revert this to being a static const byte reference
        Bytes::Array(ByteArray::inline(prefix.prefix_id().bytes(), TypeVertex::LENGTH_PREFIX)),
    )
}

pub trait TypeVertexEncoding<'a>: Sized {
    fn from_vertex(vertex: TypeVertex) -> Result<Self, EncodingError>;

    fn from_bytes(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Result<Self, EncodingError> {
        Self::from_vertex(TypeVertex::new(bytes))
    }

    fn vertex(&self) -> TypeVertex;

    fn into_vertex(self) -> TypeVertex;
}

pub trait PrefixedTypeVertexEncoding<'a>: TypeVertexEncoding<'a> {
    const PREFIX: Prefix;

    fn build_from_type_id(type_id: TypeID) -> Self {
        Self::from_vertex(TypeVertex::build(Self::PREFIX.prefix_id(), type_id)).unwrap()
    }

    fn prefix_for_kind() -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
        build_type_vertex_prefix_key(Self::PREFIX)
    }

    fn is_decodable_from_key(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == EncodingKeyspace::Schema.id() && Self::is_decodable_from(Bytes::Reference(key.bytes()))
    }

    fn is_decodable_from(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> bool {
        bytes.length() == TypeVertex::LENGTH && TypeVertex::new(bytes).prefix() == Self::PREFIX
    }
}
