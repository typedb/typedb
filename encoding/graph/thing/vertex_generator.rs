/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::byte_array::ByteArray;
use bytes::byte_array_or_ref::ByteArrayOrRef;
use storage::snapshot::snapshot::{Snapshot, WriteSnapshot};

use crate::{AsBytes, Keyable};
use crate::graph::thing::vertex_attribute::{AttributeID, AttributeID_16, AttributeID_8, AttributeVertex};
use crate::graph::{
    thing::vertex_object::{ObjectID, ObjectVertex},
    type_::vertex::{TypeID, TypeIDUInt},
};
use crate::layout::prefix::PrefixType;
use crate::value::long::Long;
use crate::value::string::StringBytes;

pub struct ThingVertexGenerator {
    entity_ids: Box<[AtomicU64]>,
    relation_ids: Box<[AtomicU64]>,
    large_value_hasher: fn(&[u8]) -> u64,
}

impl Default for ThingVertexGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl ThingVertexGenerator {
    pub fn new() -> Self {
        // TODO: we should create a resizable Vector linked to the id of types/highest id of each type
        //       this will speed up booting time on load (loading this will require MAX types * 3 iterator searches) and reduce memory footprint
        ThingVertexGenerator {
            entity_ids: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<AtomicU64>>()
                .into_boxed_slice(),
            relation_ids: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<AtomicU64>>()
                .into_boxed_slice(),
            large_value_hasher: seahash::hash,
        }
    }

    pub fn new_with_hasher(large_value_hasher: fn(&[u8]) -> u64) -> Self {
        // TODO: we should create a resizable Vector linked to the id of types/highest id of each type
        //       this will speed up booting time on load (loading this will require MAX types * 3 iterator searches) and reduce memory footprint
        ThingVertexGenerator {
            entity_ids: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
            relation_ids: (0..TypeIDUInt::MAX as usize)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<AtomicU64>>()
                .into_boxed_slice(),
            large_value_hasher: large_value_hasher,
        }
    }

    pub fn load() -> Self {
        todo!()
    }

    pub fn create_entity(&self, type_id: TypeID, snapshot: &WriteSnapshot<'_>) -> ObjectVertex<'static> {
        let entity_id = self.entity_ids[type_id.as_u16() as usize].fetch_add(1, Ordering::Relaxed);
        let vertex = ObjectVertex::build_entity(type_id, ObjectID::build(entity_id));
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_relation(&self, type_id: TypeID, snapshot: &WriteSnapshot<'_>) -> ObjectVertex<'static> {
        let relation_id = self.relation_ids[type_id.as_u16() as usize].fetch_add(1, Ordering::Relaxed);
        let vertex = ObjectVertex::build_entity(type_id, ObjectID::build(relation_id));
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    pub fn create_attribute_long(&self, type_id: TypeID, value: Long, snapshot: &WriteSnapshot<'_>) -> AttributeVertex<'static> {
        let attribute_id = AttributeID::Bytes_8(AttributeID_8::new(value.bytes()));
        let vertex = AttributeVertex::build(PrefixType::VertexAttributeLong, type_id, attribute_id);
        snapshot.put(vertex.as_storage_key().into_owned_array());
        vertex
    }

    ///
    /// We create a unique attribute ID representing the string value.
    /// We guarantee that the same value will map the same ID as long as the value remains mapped, and concurrent creation
    ///   will lead to an isolation error to maintain this invariant. The user should retry.
    ///
    /// If the value is fully removed and recreated it is possible to get a different tail of the ID.
    ///
    /// We do not need to retain a reverse index from String -> ID, since 99.9% of the time the prefix + hash
    /// lets us retrieve the ID from the forward index by prefix (ID -> String).
    ///
    pub fn create_attribute_string<'a, const INLINE_LENGTH: usize>(&self, type_id: TypeID, value: StringBytes<'a, INLINE_LENGTH>, snapshot: &WriteSnapshot<'_>) -> AttributeVertex<'static> {
        let attribute_id = AttributeID::Bytes_16(self.string_to_attribute_id(type_id, value.clone_as_ref(), snapshot));
        let vertex = AttributeVertex::build(PrefixType::VertexAttributeString, type_id, attribute_id);
        snapshot.put_val(vertex.as_storage_key().into_owned_array(), ByteArray::from(value.bytes()));
        vertex
    }

    fn string_to_attribute_id<const INLINE_LENGTH: usize>(&self, type_id: TypeID, string: StringBytes<'_, INLINE_LENGTH>, snapshot: &WriteSnapshot<'_>) -> AttributeID_16 {
        if string.length() <= StringAttributeID::ENCODING_INLINE_CAPACITY {
            StringAttributeID::build_inline_id(string).attribute_id
        } else {
            StringAttributeID::build_hashed_id(type_id, string, snapshot, &self.large_value_hasher).attribute_id
            // TODO: mark snapshot BYTES without the tail set as an exclusive key so concurrent txn will fail to commit
        }
    }
}


///
/// String encoding scheme uses 16 bytes:
///
///   Case 1: string fits in 15 bytes
///     [15: string][1: 0b0[length]]
///
///   Case 2: string does not fit in 15 bytes:
///     [8: prefix][7: hash][1: 0b1[disambiguator]]
///
///  4 byte hash: collision probability of 50% at 77k elements
///  5 byte hash: collision probability of 50% at 1.25m elements
///  6 byte hash: collision probability of 50% at 20m elements
///  7 byte hash: collision probability of 50% at 320m elements
///  8 byte hash: collision probability of 50% at 5b elements
///
///  With an 8 byte prefix and 7 byte hash we can insert up to 100 million elements behind the same prefix
///  before we have a 5% chance of collision. With 100 million entries with 100 bytes each, we can store 20GB of data in the same prefix.
///  We also allow disambiguation in the tail byte of the ID, so we can tolerate up to 127 collsions, or approximately 2TB of data with above assumptions.
///
pub struct StringAttributeID {
    attribute_id: AttributeID_16,
}

impl StringAttributeID {
    pub(crate) const ENCODING_INLINE_CAPACITY: usize = 15;
    const ENCODING_STRING_PREFIX_LENGTH: usize = 8;
    pub const ENCODING_STRING_PREFIX_RANGE: Range<usize> = 0..Self::ENCODING_STRING_PREFIX_LENGTH;
    pub const ENCODING_STRING_HASH_LENGTH: usize = 7;
    const ENCODING_STRING_HASH_RANGE: Range<usize> = Self::ENCODING_STRING_PREFIX_RANGE.end..Self::ENCODING_STRING_PREFIX_RANGE.end + Self::ENCODING_STRING_HASH_LENGTH;
    const ENCODING_STRING_TAIL_BYTE_INDEX: usize = Self::ENCODING_STRING_HASH_RANGE.end;
    const ENCODING_STRING_TAIL_MASK: u8 = 0b10000000;

    pub fn new(attribute_id: AttributeID_16) -> Self {
        Self { attribute_id: attribute_id }
    }

    fn build_inline_id<const INLINE_LENGTH: usize>(string: StringBytes<'_, INLINE_LENGTH>) -> Self {
        debug_assert!(string.length() < Self::ENCODING_INLINE_CAPACITY);
        let mut bytes = [0u8; AttributeID_16::LENGTH];
        bytes[0..string.length()].copy_from_slice(string.bytes().bytes());
        Self::set_tail_inline_length(&mut bytes, string.length() as u8);
        Self::new(AttributeID_16::new(bytes))
    }

    ///
    /// Encode the last byte by setting 0b0[7 bits representing length of the prefix characters]
    ///
    fn set_tail_inline_length(bytes: &mut [u8; AttributeID_16::LENGTH], length: u8) {
        assert!(length & Self::ENCODING_STRING_TAIL_MASK == 0); // ie < 128, high bit not set
        // because the high bit is not set, we already conform to the required mask of high bit = 0
        bytes[Self::ENCODING_STRING_TAIL_BYTE_INDEX] = length;
    }

    fn build_hashed_id<const INLINE_LENGTH: usize>(type_id: TypeID, string: StringBytes<'_, INLINE_LENGTH>,
                                                   snapshot: &WriteSnapshot<'_>, hasher: &impl Fn(&[u8]) -> u64) -> Self {
        let mut bytes = [0u8; AttributeID_16::LENGTH];
        let string_bytes = string.bytes().bytes();
        bytes[Self::ENCODING_STRING_PREFIX_RANGE].copy_from_slice(&string_bytes[Self::ENCODING_STRING_PREFIX_RANGE]);
        let hash_bytes: [u8; Self::ENCODING_STRING_HASH_LENGTH] = (hasher(string_bytes).to_be_bytes()[0..Self::ENCODING_STRING_HASH_LENGTH]).try_into().unwrap();
        bytes[Self::ENCODING_STRING_HASH_RANGE].copy_from_slice(&hash_bytes);

        // find first unused tail value
        bytes[Self::ENCODING_STRING_TAIL_BYTE_INDEX] = Self::ENCODING_STRING_TAIL_MASK;
        let prefix_search = AttributeVertex::build_prefix(PrefixType::VertexAttributeString, type_id, &bytes);

        let mut iter = snapshot.iterate_prefix(prefix_search);
        let mut next = iter.next().transpose().unwrap(); // TODO: handle error
        let mut tail: u8 = 0;
        while let Some((key, value)) = next {
            let mapped_string = StringBytes::new(ByteArrayOrRef::Reference(value.clone()));
            let existing_attribute_id = AttributeVertex::new(ByteArrayOrRef::Reference(key.byte_ref()))
                .attribute_id().unwrap_bytes_16();
            if mapped_string == string {
                return Self::new(existing_attribute_id);
            } else if tail != StringAttributeID::new(existing_attribute_id).get_hash_disambiguator() {
                // found unused tail ID
                break;
            }
            tail += 1;
            next = iter.next().transpose().unwrap();
        }
        if tail & Self::ENCODING_STRING_TAIL_MASK != 0 { // over 127
            // TODO: should we panic?
            panic!("String encoding space has no space remaining within the prefix and hash prefix.")
        }
        Self::set_tail_hash_disambiguator(&mut bytes, tail);
        Self::new(AttributeID_16::new(bytes))
    }

    ///
    /// Encode the last byte by setting 0b1[7 bits representing disambiguator]
    ///
    fn set_tail_hash_disambiguator(bytes: &mut [u8; AttributeID_16::LENGTH], disambiguator: u8) {
        debug_assert!(disambiguator & Self::ENCODING_STRING_TAIL_MASK == 0); // ie. disambiguator < 128, not using high bit
        // sets 0x1[disambiguator]
        bytes[Self::ENCODING_STRING_TAIL_BYTE_INDEX] = disambiguator | Self::ENCODING_STRING_TAIL_MASK;
    }

    pub fn is_inline(&self) -> bool {
        self.attribute_id.bytes()[Self::ENCODING_STRING_TAIL_BYTE_INDEX] & Self::ENCODING_STRING_TAIL_MASK == 0
    }

    pub fn get_inline_string_bytes(&self) -> StringBytes<'static, 16> {
        debug_assert!(self.is_inline());
        let mut bytes = ByteArray::zeros(AttributeID_16::LENGTH);
        let inline_string_length = self.get_inline_length();
        bytes.bytes_mut()[0..inline_string_length as usize].copy_from_slice(&self.attribute_id.bytes()[0..inline_string_length as usize]);
        bytes.truncate(inline_string_length as usize);
        StringBytes::new(ByteArrayOrRef::Array(bytes))
    }

    pub fn get_inline_length(&self) -> u8 {
        debug_assert!(self.is_inline());
        self.attribute_id.bytes()[Self::ENCODING_STRING_TAIL_BYTE_INDEX]
    }

    pub fn get_hash_prefix(&self) -> [u8; Self::ENCODING_STRING_PREFIX_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.attribute_id.bytes()[Self::ENCODING_STRING_PREFIX_RANGE]).try_into().unwrap()
    }

    pub fn get_hash_hash(&self) -> [u8; Self::ENCODING_STRING_HASH_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.attribute_id.bytes()[Self::ENCODING_STRING_HASH_RANGE]).try_into().unwrap()
    }

    pub fn get_hash_disambiguator(&self) -> u8 {
        debug_assert!(!self.is_inline());
        let byte = self.attribute_id.bytes()[Self::ENCODING_STRING_TAIL_BYTE_INDEX];
        byte & !Self::ENCODING_STRING_TAIL_MASK // unsets 0x1___ high bit
    }
}
