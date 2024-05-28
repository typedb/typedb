/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::hash::Hash;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::{StorageKey, StorageKeyReference},
    keyspace::KeyspaceSet,
};

use crate::{
    graph::Typed,
    layout::prefix::{Prefix, PrefixID},
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

// TODO: we could make all Type constructs contain plain byte arrays, since they will always be 64 bytes (BUFFER_KEY_INLINE), then make Types all Copy
//       However, we should benchmark this first, since 64 bytes may be better off referenced

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TypeVertex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

// TODO: Refactor into factories: https://github.com/vaticle/typedb/pull/7040#discussion_r1567373838

pub(crate) fn build_type_vertex_prefix_key(prefix: Prefix) -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
    StorageKey::new(
        TypeVertex::KEYSPACE,
        // TODO: Can we revert this to being a static const byte reference
        Bytes::Array(ByteArray::inline(prefix.prefix_id().bytes(), TypeVertex::LENGTH_PREFIX))
    )
}

pub trait EncodableTypeVertex<'a> : Sized {

    fn from_vertex(vertex: TypeVertex<'a>) -> Self;

    fn into_vertex(self) -> TypeVertex<'a>;

    fn decode(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        Self::from_vertex(TypeVertex::new(bytes))
    }
}

pub trait PrefixedEncodableTypeVertex<'a> : EncodableTypeVertex<'a>{
    const PREFIX: Prefix;

    fn build(type_id: TypeID) -> Self {
        Self::from_vertex(TypeVertex::build(Self::PREFIX.prefix_id(), type_id))
    }

    fn prefix_for_kind() -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
        build_type_vertex_prefix_key(Self::PREFIX)
    }

    fn is_of_kind(key: StorageKeyReference<'_>) -> bool {
        key.keyspace_id() == EncodingKeyspace::Schema.id()
            && key.length() == TypeVertex::LENGTH
            && TypeVertex::new(Bytes::Reference(key.byte_ref())).prefix() == Self::PREFIX
    }
}

macro_rules! type_vertex_constructors {
    ($new_name:ident, $build_name:ident, $build_name_prefix:ident, $is_name:ident, Prefix::$prefix:ident) => {
        pub fn $new_name(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> TypeVertex<'_> {
            let vertex = TypeVertex::new(bytes);
            debug_assert_eq!(vertex.prefix(), Prefix::$prefix);
            vertex
        }

        pub fn $build_name(type_id: TypeID) -> TypeVertex<'static> {
            TypeVertex::build(Prefix::$prefix.prefix_id(), type_id)
        }

        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        pub fn $build_name_prefix() -> StorageKey<'static, { TypeVertex::LENGTH_PREFIX }> {
            const BYTES: [u8; TypeVertex::LENGTH_PREFIX] = Prefix::$prefix.prefix_id().bytes();
            StorageKey::new_ref(TypeVertex::KEYSPACE, ByteReference::new(&BYTES))
        }

        pub fn $is_name(key: StorageKeyReference<'_>) -> bool {
            key.keyspace_id() == crate::EncodingKeyspace::Schema.id()
                && key.length() == TypeVertex::LENGTH
                && TypeVertex::new(Bytes::Reference(key.byte_ref())).prefix() == Prefix::$prefix
        }
    };
}

type_vertex_constructors!(
    new_vertex_entity_type,
    build_vertex_entity_type,
    build_vertex_entity_type_prefix,
    is_vertex_entity_type,
    Prefix::VertexEntityType
);
type_vertex_constructors!(
    new_vertex_relation_type,
    build_vertex_relation_type,
    build_vertex_relation_type_prefix,
    is_vertex_relation_type,
    Prefix::VertexRelationType
);
type_vertex_constructors!(
    new_vertex_role_type,
    build_vertex_role_type,
    build_vertex_role_type_prefix,
    is_vertex_role_type,
    Prefix::VertexRoleType
);
type_vertex_constructors!(
    new_vertex_attribute_type,
    build_vertex_attribute_type,
    build_vertex_attribute_type_prefix,
    is_vertex_attribute_type,
    Prefix::VertexAttributeType
);

impl<'a> TypeVertex<'a> {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Schema;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub(crate) const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> TypeVertex<'a> {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        TypeVertex { bytes }
    }

    pub(crate) fn build(prefix: PrefixID, type_id: TypeID) -> Self {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&prefix.bytes());
        array.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        TypeVertex { bytes: Bytes::Array(array) }
    }

    pub fn as_reference<'this: 'a>(&'this self) -> TypeVertex<'this> {
        Self::new(Bytes::Reference(self.bytes.as_reference()))
    }

    pub fn into_owned(self) -> TypeVertex<'static> {
        TypeVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for TypeVertex<'a> {}

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
