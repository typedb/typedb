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

use durability::DurabilityRecordType;
use itertools::EitherOrBoth;
use primitive::prefix::Prefix;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    durability_client::{DurabilityRecord, UnsequencedDurabilityRecord},
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::StorageKey,
};

type Key = StorageKey<'static, BUFFER_KEY_INLINE>;

#[derive(Debug)]
struct KeyRangeInclusive {
    start: Key,
    end: Key,
    fixed_width: bool,
}

impl KeyRangeInclusive {
    fn merge(self, other: Self) -> Self {
        debug_assert_eq!(self.fixed_width, other.fixed_width);
        Self {
            start: Key::min(self.start, other.start),
            end: Key::max(self.end, other.end),
            fixed_width: self.fixed_width,
        }
    }
}

impl From<KeyRangeInclusive> for KeyRange<Key> {
    fn from(value: KeyRangeInclusive) -> Self {
        let KeyRangeInclusive { start, end, fixed_width } = value;
        Self::new(RangeStart::Inclusive(start), RangeEnd::EndPrefixInclusive(end), fixed_width)
    }
}

#[derive(Debug, Default)]
pub struct CleanupRecord {
    intervals: BTreeMap<Key, KeyRangeInclusive>,
}

impl CleanupRecord {
    pub fn new() -> Self {
        Self { intervals: BTreeMap::new() }
    }

    pub fn insert(&mut self, prefix: Key, key: Key, fixed_width_keys: bool) {
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
        let intervals = itertools::merge_join_by(a.intervals, b.intervals, |(apfx, _), (bpfx, _)| Key::cmp(apfx, bpfx))
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
}

impl IntoIterator for CleanupRecord {
    type Item = (Key, KeyRange<Key>);

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.intervals.into_iter().map(|(prefix, range)| (prefix, range.into())))
    }
}

pub struct IntoIter(
    Map<btree_map::IntoIter<Key, KeyRangeInclusive>, fn((Key, KeyRangeInclusive)) -> (Key, KeyRange<Key>)>,
);

impl Iterator for IntoIter {
    type Item = (Key, KeyRange<Key>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DurabilityRecord for CleanupRecord {
    const RECORD_TYPE: DurabilityRecordType = 11;
    const RECORD_NAME: &'static str = "";

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
        ser::SerializeMap,
    };

    use super::{CleanupRecord, Key, KeyRangeInclusive};

    impl Serialize for CleanupRecord {
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

    impl<'de> Deserialize<'de> for CleanupRecord {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct KeyRangeMapVisitor;

            impl<'de> Visitor<'de> for KeyRangeMapVisitor {
                type Value = BTreeMap<Key, KeyRangeInclusive>;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(type_name::<Self>())
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut intervals = BTreeMap::new();
                    while let Some((prefix, range)) = map.next_entry()? {
                        intervals.insert(Key::Array(prefix), range);
                    }
                    Ok(intervals)
                }
            }

            Ok(CleanupRecord { intervals: deserializer.deserialize_map(KeyRangeMapVisitor)? })
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
                        start: Key::Array(start.ok_or_else(|| de::Error::missing_field("start"))?),
                        end: Key::Array(end.ok_or_else(|| de::Error::missing_field("end"))?),
                        fixed_width: fixed_width.ok_or_else(|| de::Error::missing_field("fixed_width"))?,
                    })
                }
            }

            deserializer.deserialize_map(KeyRangeInclusiveVisitor)
        }
    }
}
