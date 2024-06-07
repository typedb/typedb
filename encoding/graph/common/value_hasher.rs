use std::ops::Range;
use std::sync::Arc;
use bytes::Bytes;
use lending_iterator::LendingIterator;
use primitive::either::Either;
use storage::key_range::KeyRange;
use storage::snapshot::iterator::SnapshotIteratorError;
use storage::snapshot::ReadableSnapshot;
use crate::graph::thing::vertex_attribute::AttributeVertex;
use crate::value::value_type::ValueTypeCategory;

pub trait DisambiguatedValueHash<const LENGTH: usize>: Sized {
    type ContextIDType; // e.g. TypeID or DefinitionID
    const VALUE_TYPE_CATEGORY: ValueTypeCategory;  // TODO: Move to an AttributeIDHasher
    const ENCODING_HASH_LENGTH: usize;

    const ENCODING_TAIL_BYTE_IS_HASH_FLAG: u8 = 0b10000000;
    const ENCODING_TAIL_BYTE_INDEX: usize = LENGTH - 1;

    // Range
    const ENCODING_PREFIX_LENGTH: usize = LENGTH - (Self::ENCODING_HASH_LENGTH + 1);
    const ENCODING_PREFIX_RANGE: Range<usize> = 0..Self::ENCODING_PREFIX_LENGTH;
    const ENCODING_HASH_RANGE: Range<usize> =
        Self::ENCODING_PREFIX_LENGTH..Self::ENCODING_PREFIX_LENGTH + Self::ENCODING_HASH_LENGTH;

    fn new(bytes: [u8; LENGTH]) -> Self;

    fn set_prefix(value_bytes: &[u8], into_slice: &mut [u8; LENGTH]);

    fn build_ambiguous_key_range(type_id: Self::ContextIDType, id_without_tail: &[u8]) -> KeyRange<> {
        KeyRange::new_within(
            // The tail byte must be excluded
            AttributeVertex::build_prefix_type_attribute_id(
                Self::VALUE_TYPE_CATEGORY,
                type_id,
                &id_with_ambiguous_tail[0..(Self::ENCODING_TAIL_BYTE_INDEX)],
            ),
            AttributeVertex::value_type_category_to_prefix_type(Self::VALUE_TYPE_CATEGORY).fixed_width_keys(),
        );
    }

    fn build_hashed_id_with_ambiguous_tail(value_bytes: &[u8], hasher: &impl Fn(&[u8]) -> u64) -> [u8; LENGTH] {
        let mut id_bytes: [u8; LENGTH] = [0; LENGTH];
        let hash = hasher(value_bytes);
        Self::set_prefix(value_bytes, &mut id_bytes);
        id_bytes[Self::ENCODING_HASH_RANGE].copy_from_slice(&hash.to_be_bytes()[0..Self::ENCODING_HASH_LENGTH]);
        id_bytes[Self::ENCODING_TAIL_BYTE_INDEX] = Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG;
        id_bytes
    }

    fn build_hashed_id_from_value_bytes<Snapshot>(
        type_id: Self::ContextIDType,
        value_bytes: &[u8],
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Either<Self, Self>, Arc<SnapshotIteratorError>>
        where
            Snapshot: ReadableSnapshot,
    {
        let mut id_bytes = Self::build_hashed_id_with_ambiguous_tail(value_bytes, hasher);
        let existing_or_tail = Self::find_existing_or_next_tail(type_id, value_bytes, &id_bytes, snapshot)?;
        match existing_or_tail {
            Either::First(existing_tail) => {
                Self::set_hash_disambiguator(&mut id_bytes, existing_tail);
                Ok(Either::First(Self::new(id_bytes)))
            }
            Either::Second(new_tail) => {
                if new_tail & Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG != 0 {
                    // over 127 // TODO: should we panic?
                    panic!("String encoding space has no space remaining within the prefix and hash prefix.")
                }
                Self::set_hash_disambiguator(&mut id_bytes, new_tail);
                Ok(Either::Second(Self::new(id_bytes)))
            }
        }
    }

    fn find_hashed_id_for_value_bytes<Snapshot>(
        type_id: Self::ContextIDType,
        value_bytes: &[u8],
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Option<Self>, Arc<SnapshotIteratorError>>
        where
            Snapshot: ReadableSnapshot,
    {
        let mut id_bytes = Self::build_hashed_id_with_ambiguous_tail(value_bytes, hasher);
        let existing_or_next_tail = Self::find_existing_or_next_tail(type_id, value_bytes, &id_bytes, snapshot)?;
        match existing_or_next_tail {
            Either::First(existing_tail) => {
                Self::set_hash_disambiguator(&mut id_bytes, existing_tail);
                Ok(Some(Self::new(id_bytes)))
            }
            Either::Second(_) => Ok(None),
        }
    }

    fn find_existing_or_next_tail<Snapshot>(
        type_id: Self::ContextIDType,
        value_bytes: &[u8],
        id_with_ambiguous_tail: &[u8; LENGTH],
        snapshot: &Snapshot,
    ) -> Result<Either<u8, u8>, Arc<SnapshotIteratorError>>
        where
            Snapshot: ReadableSnapshot,
    {
        let prefix_search = Self::build_key_range(type_id, id_with_ambiguous_tail);

        let mut iter = snapshot.iterate_range(prefix_search);
        let mut next = iter.next().transpose()?;
        let mut first_unused_tail: Option<u8> = None;

        let mut tail: u8 = 0;
        while let Some((key, value)) = next {
            let existing_attribute_id = AttributeVertex::new(Bytes::reference(key.bytes())).attribute_id();
            if value.bytes() == value_bytes {
                let existing_tail = Self::get_hash_disambiguator(key.bytes());
                return Ok(Either::First(existing_tail));
            } else if tail != Self::get_hash_disambiguator(existing_attribute_id.bytes()) {
                // found unused tail ID
                first_unused_tail = Some(tail);
            }
            tail += 1;
            next = iter.next().transpose()?;
        }
        Ok(Either::Second(first_unused_tail.unwrap_or(tail)))
    }

    fn get_hash_disambiguator(bytes: &[u8]) -> u8 {
        debug_assert!(LENGTH == bytes.len() && !Self::is_inline_bytes(bytes));
        let byte = bytes[Self::ENCODING_TAIL_BYTE_INDEX];
        byte & !Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG // unsets 0b1___ high bit
    }

    /// Encode the last byte by setting 0b1[7 bits representing disambiguator]
    fn set_hash_disambiguator(bytes: &mut [u8; LENGTH], disambiguator: u8) {
        debug_assert!(disambiguator & Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG == 0); // ie. disambiguator < 128, not using high bit
        bytes[Self::ENCODING_TAIL_BYTE_INDEX] = disambiguator | Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG;
    }

    fn is_inline_bytes(bytes: &[u8]) -> bool {
        bytes[Self::ENCODING_TAIL_BYTE_INDEX] & Self::ENCODING_TAIL_BYTE_IS_HASH_FLAG == 0
    }
}
