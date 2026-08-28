/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{
        BTreeMap,
        btree_map::{self, Entry},
    },
    iter::Map,
};

use bytes::Bytes;
use durability::DurabilityRecordType;
use itertools::EitherOrBoth;
use primitive::prefix::Prefix;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    durability_client::{DurabilityRecord, UnsequencedDurabilityRecord},
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::StorageKey,
    keyspace::KeyspaceSet,
    sequence_number::SequenceNumber,
};

type CleanupKey = StorageKey<'static, BUFFER_KEY_INLINE>;

#[derive(Debug)]
pub struct CleanupRecord {
    pub sequence_number: SequenceNumber,
    pub cleanup_intervals: CleanupIntervals,
}

#[derive(Debug, Clone)]
struct KeyRangeInclusive {
    start: CleanupKey,
    end: CleanupKey,
    fixed_width: bool,
}

impl KeyRangeInclusive {
    fn merge(self, other: Self) -> Self {
        debug_assert_eq!(self.fixed_width, other.fixed_width);
        Self {
            start: CleanupKey::min(self.start, other.start),
            end: CleanupKey::max(self.end, other.end),
            fixed_width: self.fixed_width,
        }
    }
}

impl From<KeyRangeInclusive> for KeyRange<CleanupKey> {
    fn from(value: KeyRangeInclusive) -> Self {
        let KeyRangeInclusive { start, end, fixed_width } = value;
        Self::new(RangeStart::Inclusive(start), RangeEnd::EndPrefixInclusive(end), fixed_width)
    }
}

#[derive(Debug, Default, Clone)]
pub struct CleanupIntervals {
    intervals: BTreeMap<CleanupKey, KeyRangeInclusive>,
}

impl CleanupIntervals {
    pub fn new() -> Self {
        Self { intervals: BTreeMap::new() }
    }

    pub fn everything<KS: KeyspaceSet>() -> Self {
        Self {
            intervals: KS::iter()
                .map(|ks| {
                    let empty_key = || CleanupKey::new(ks, Bytes::copy(&[]));
                    (empty_key(), KeyRangeInclusive { start: empty_key(), end: empty_key(), fixed_width: false })
                })
                .collect(),
        }
    }

    pub fn insert(&mut self, prefix: CleanupKey, key: CleanupKey, fixed_width_keys: bool) {
        debug_assert!(key.starts_with(&prefix));
        match self.intervals.entry(prefix) {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(KeyRangeInclusive { start: key.clone(), end: key, fixed_width: fixed_width_keys });
            }
            Entry::Occupied(mut occupied_entry) => {
                debug_assert_eq!(occupied_entry.get().fixed_width, fixed_width_keys);
                if key < occupied_entry.get().start {
                    occupied_entry.get_mut().start = key;
                } else if key > occupied_entry.get().end {
                    occupied_entry.get_mut().end = key;
                }
            }
        }
    }

    pub fn merge(a: Self, b: Self) -> Self {
        let intervals =
            itertools::merge_join_by(a.intervals, b.intervals, |(apfx, _), (bpfx, _)| CleanupKey::cmp(apfx, bpfx))
                .map(|x| match x {
                    EitherOrBoth::Left(kv) | EitherOrBoth::Right(kv) => kv,
                    EitherOrBoth::Both((pfx, range_a), (_pfx, range_b)) => {
                        debug_assert_eq!(pfx, _pfx);
                        (pfx, range_a.merge(range_b))
                    }
                })
                .collect();
        Self { intervals }
    }

    pub fn into_record(self, sequence_number: SequenceNumber) -> CleanupRecord {
        CleanupRecord { sequence_number, cleanup_intervals: self }
    }
}

impl IntoIterator for CleanupIntervals {
    type Item = (CleanupKey, KeyRange<CleanupKey>);

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.intervals.into_iter().map(|(prefix, range)| (prefix, range.into())))
    }
}

pub struct IntoIter(
    Map<
        btree_map::IntoIter<CleanupKey, KeyRangeInclusive>,
        fn((CleanupKey, KeyRangeInclusive)) -> (CleanupKey, KeyRange<CleanupKey>),
    >,
);

impl Iterator for IntoIter {
    type Item = (CleanupKey, KeyRange<CleanupKey>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DurabilityRecord for CleanupRecord {
    const RECORD_TYPE: DurabilityRecordType = 11;
    const RECORD_NAME: &'static str = "cleanup_record";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        bincode::serialize_into(writer, self)
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        bincode::deserialize_from(reader)
    }
}

impl UnsequencedDurabilityRecord for CleanupRecord {}

mod serialise {
    use std::{any::type_name, collections::BTreeMap, fmt};

    use serde::{
        Deserialize, Serialize,
        de::{self, Visitor},
        ser::{SerializeMap, SerializeSeq},
    };

    use super::{CleanupIntervals, CleanupKey, CleanupRecord, KeyRangeInclusive};

    impl Serialize for CleanupRecord {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(2))?;
            seq.serialize_element(&self.sequence_number)?;
            seq.serialize_element(&self.cleanup_intervals)?;
            seq.end()
        }
    }

    impl<'de> Deserialize<'de> for CleanupRecord {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct CleanupRecordVisitor;

            impl<'de> Visitor<'de> for CleanupRecordVisitor {
                type Value = CleanupRecord;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a sequence of two elements")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>,
                {
                    let sequence_number = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &"a sequence of two elements"))?;
                    let cleanup_intervals = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &"a sequence of two elements"))?;
                    Ok(CleanupRecord { sequence_number, cleanup_intervals })
                }
            }

            deserializer.deserialize_seq(CleanupRecordVisitor)
        }
    }

    impl Serialize for CleanupIntervals {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(self.intervals.len()))?;
            for (prefix, range) in &self.intervals {
                map.serialize_entry(&prefix.clone().into_owned_array(), &range)?;
            }
            map.end()
        }
    }

    impl<'de> Deserialize<'de> for CleanupIntervals {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct KeyRangeMapVisitor;

            impl<'de> Visitor<'de> for KeyRangeMapVisitor {
                type Value = BTreeMap<CleanupKey, KeyRangeInclusive>;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(type_name::<Self>())
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut intervals = BTreeMap::new();
                    while let Some((prefix, range)) = map.next_entry()? {
                        intervals.insert(CleanupKey::Array(prefix), range);
                    }
                    Ok(intervals)
                }
            }

            Ok(CleanupIntervals { intervals: deserializer.deserialize_map(KeyRangeMapVisitor)? })
        }
    }

    impl Serialize for KeyRangeInclusive {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(3))?;
            map.serialize_entry("start", &self.start.clone().into_owned_array())?;
            map.serialize_entry("end", &self.end.clone().into_owned_array())?;
            map.serialize_entry("fixed_width", &self.fixed_width)?;
            map.end()
        }
    }

    impl<'de> Deserialize<'de> for KeyRangeInclusive {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct KeyRangeInclusiveVisitor;

            impl<'de> Visitor<'de> for KeyRangeInclusiveVisitor {
                type Value = KeyRangeInclusive;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(type_name::<Self>())
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut start = None;
                    let mut end = None;
                    let mut fixed_width = None;

                    while let Some(key) = map.next_key::<String>()? {
                        match key.as_str() {
                            "start" => start = Some(map.next_value()?),
                            "end" => end = Some(map.next_value()?),
                            "fixed_width" => fixed_width = Some(map.next_value()?),
                            field => return Err(de::Error::unknown_field(field, &["start", "end", "fixed_width"])),
                        }
                    }

                    Ok(KeyRangeInclusive {
                        start: CleanupKey::Array(start.ok_or_else(|| de::Error::missing_field("start"))?),
                        end: CleanupKey::Array(end.ok_or_else(|| de::Error::missing_field("end"))?),
                        fixed_width: fixed_width.ok_or_else(|| de::Error::missing_field("fixed_width"))?,
                    })
                }
            }

            deserializer.deserialize_map(KeyRangeInclusiveVisitor)
        }
    }
}
