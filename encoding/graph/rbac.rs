/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, ops::Range};

use bytes::{Bytes, byte_array::ByteArray};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, snapshot::WritableSnapshot};

use crate::{
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
    error::EncodingError,
    graph::{common::schema_id_allocator::RBACVertexAllocator, type_::vertex::TypeVertex},
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RBACRoleId(u64);

impl RBACRoleId {
    const LENGTH: usize = std::mem::size_of::<u64>();

    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for RBACRoleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04X}", self.0)
    }
}

#[derive(Debug)]
pub struct RBACVertexGenerator(RBACVertexAllocator);

impl RBACVertexGenerator {
    pub fn new() -> Self {
        Self(RBACVertexAllocator::new())
    }

    pub fn allocate(&self, snapshot: &mut impl WritableSnapshot) -> Result<RBACRoleVertex, EncodingError> {
        self.0.allocate(snapshot)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RBACRoleVertex {
    // Unlike TypeVertex, We don't store the prefix here since we have only one type of role vertex.
    role_id: RBACRoleId,
}

impl fmt::Debug for RBACRoleVertex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#018X}", self.role_id.0)
    }
}

impl RBACRoleVertex {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub const PREFIX: Prefix = Prefix::RBACRole;

    pub const LENGTH: usize = PrefixID::LENGTH + RBACRoleId::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub const fn new(role_id: RBACRoleId) -> Self {
        Self { role_id }
    }

    const fn range_role_id() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRoleId::LENGTH
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);
        let mut be_bytes = [0u8; RBACRoleId::LENGTH];
        be_bytes.copy_from_slice(&bytes[Self::range_role_id()]);
        Self { role_id: RBACRoleId::new(u64::from_be_bytes(be_bytes)) }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }
        let mut be_bytes = [0u8; RBACRoleId::LENGTH];
        be_bytes.copy_from_slice(&bytes[Self::range_role_id()]);
        Some(Self { role_id: RBACRoleId::new(u64::from_be_bytes(be_bytes)) })
    }

    pub fn role_id(&self) -> RBACRoleId {
        self.role_id
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACRoleVertex::LENGTH_PREFIX }> {
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACRoleVertex::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleVertex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_role_id()].copy_from_slice(&self.role_id.0.to_be_bytes());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }

    fn into_storage_key(self) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        StorageKey::new(self.keyspace(), self.to_bytes())
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleVertex {
    fn prefix(&self) -> Prefix {
        Self::PREFIX
    }
}

// Properties such as label
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACRoleProperty {
    role: RBACRoleVertex,
    infix: Infix,
    suffix: ByteArray<0>,
}

impl RBACRoleProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyRBACRole;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub fn new_key_for_label(role: RBACRoleVertex) -> Self {
        Self::new_no_suffix(role, Infix::PropertyLabel)
    }

    fn new_no_suffix(role: RBACRoleVertex, infix: Infix) -> Self {
        Self { role, infix, suffix: ByteArray::<0>::empty() }
    }

    fn new_suffixed<const INLINE_BYTES: usize>(
        role: RBACRoleVertex,
        infix: Infix,
        suffix: Bytes<'_, INLINE_BYTES>,
    ) -> Self {
        Self { role, infix, suffix: ByteArray::copy(&suffix) }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRoleVertex::decode(bytes.clone().into_range(Self::range_rbac_role()));
        let infix = Infix::from_infix_id(InfixID::new((&bytes[Self::range_infix()]).try_into().unwrap()));
        let suffix = ByteArray::copy(&bytes[Self::LENGTH_NO_SUFFIX..]);
        Self { role, infix, suffix }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::LENGTH_NO_SUFFIX {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }

        let role = RBACRoleVertex::try_decode(bytes.get(Self::range_rbac_role())?)?;
        let infix = Infix::from_infix_id(InfixID::new((bytes.get(Self::range_infix())?).try_into().ok()?));
        let suffix = ByteArray::copy(bytes.get(Self::LENGTH_NO_SUFFIX..)?);
        Some(Self { role, infix, suffix })
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACRoleProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACRoleProperty::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn rbac_role(&self) -> RBACRoleVertex {
        self.role
    }

    pub fn infix(&self) -> Infix {
        self.infix
    }

    fn suffix_length(&self) -> usize {
        self.suffix.len()
    }

    pub fn suffix(&self) -> &[u8] {
        &self.suffix
    }

    const fn range_rbac_role() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRoleVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_rbac_role().end..Self::range_rbac_role().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleProperty {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + self.suffix_length());
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_rbac_role()].copy_from_slice(&self.role.to_bytes());
        array[Self::range_infix()].copy_from_slice(&self.infix.infix_id().bytes());
        array[Self::range_suffix(self.suffix_length())].copy_from_slice(self.suffix());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleProperty {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleProperty {
    fn prefix(&self) -> Prefix {
        Self::PREFIX
    }
}

// Accesses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub enum RBACAccessKind {
    ReadObjectVertex,
    WriteObjectVertex,
    // More
    ReadAttributeVertex,
    WriteAttributeVertex,
}

impl RBACAccessKind {
    pub fn decode(bytes: [u8; InfixID::LENGTH]) -> Self {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACReadObjectVertex => Self::ReadObjectVertex,
            Infix::RBACWriteObjectVertex => Self::WriteObjectVertex,
            Infix::RBACReadAttributeVertex => Self::ReadAttributeVertex,
            Infix::RBACWriteAttributeVertex => Self::WriteAttributeVertex,
            _other => panic!("Unexpected infix for RBACAccessType: {infix:?}"),
        }
    }

    pub(crate) fn infix(&self) -> Infix {
        match self {
            Self::ReadObjectVertex => Infix::RBACReadObjectVertex,
            Self::WriteObjectVertex => Infix::RBACWriteObjectVertex,
            Self::ReadAttributeVertex => Infix::RBACReadAttributeVertex,
            Self::WriteAttributeVertex => Infix::RBACWriteAttributeVertex,
        }
    }

    fn try_decode(bytes: [u8; InfixID::LENGTH]) -> Option<Self> {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACReadObjectVertex => Some(Self::ReadObjectVertex),
            Infix::RBACWriteObjectVertex => Some(Self::WriteObjectVertex),
            Infix::RBACReadAttributeVertex => Some(Self::ReadAttributeVertex),
            Infix::RBACWriteAttributeVertex => Some(Self::WriteAttributeVertex),
            _other => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACTypeAccessEdge {
    role: RBACRoleVertex,
    access_kind: RBACAccessKind,
    type_: TypeVertex,
}

impl RBACTypeAccessEdge {
    // Note: Using u64 for RBAC roles means the length is > 11. We still use this prefix as:
    // * Not having the TypeVertex within the prefix is not a big deal.
    // * All schema goes in here anyway.
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::RBACAccess;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH + InfixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_ROLE: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH;

    pub fn new(role: RBACRoleVertex, access_kind: RBACAccessKind, type_: TypeVertex) -> Self {
        Self { role, access_kind, type_ }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRoleVertex::decode(bytes.clone().into_range(Self::range_rbac_role()));
        let access_kind = RBACAccessKind::decode((&bytes[Self::range_access_kind()]).try_into().unwrap());
        let type_ = TypeVertex::decode(bytes.clone().into_range(Self::range_type()));
        Self { role, access_kind: access_kind, type_ }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }

        let role = RBACRoleVertex::try_decode(bytes.get(Self::range_rbac_role())?)?;
        let access_kind = RBACAccessKind::try_decode((bytes.get(Self::range_access_kind())?).try_into().ok()?)?;
        let type_ = TypeVertex::try_decode(bytes.get(Self::range_type())?)?;
        Some(Self { role, access_kind: access_kind, type_ })
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACTypeAccessEdge::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACTypeAccessEdge::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn build_prefix_role(role: RBACRoleVertex) -> StorageKey<'static, { Self::LENGTH_PREFIX_ROLE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_ROLE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_rbac_role()].copy_from_slice(&role.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn rbac_role(&self) -> RBACRoleVertex {
        self.role
    }

    pub fn access_kind(&self) -> RBACAccessKind {
        self.access_kind
    }

    pub fn type_(&self) -> TypeVertex {
        self.type_
    }

    const fn range_rbac_role() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRoleVertex::LENGTH
    }

    const fn range_access_kind() -> Range<usize> {
        Self::range_rbac_role().end..Self::range_rbac_role().end + InfixID::LENGTH
    }

    fn range_type() -> Range<usize> {
        Self::range_access_kind().end..Self::range_access_kind().end + TypeVertex::LENGTH
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACTypeAccessEdge {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_rbac_role()].copy_from_slice(&self.role.to_bytes());
        array[Self::range_access_kind()].copy_from_slice(&self.access_kind.infix().infix_id().bytes());
        array[Self::range_type()].copy_from_slice(&self.type_.to_bytes());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACTypeAccessEdge {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACTypeAccessEdge {
    fn prefix(&self) -> Prefix {
        Self::PREFIX
    }
}
