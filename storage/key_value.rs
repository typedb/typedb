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

use std::cmp::Ordering;
use std::fmt;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;

pub type KeyspaceId = u8;

pub(crate) const KEYSPACE_ID_MAX: usize = KeyspaceId::MAX as usize;
const FIXED_KEY_LENGTH_BYTES: usize = 48;


// TODO: we will need to know if these are from storage or from memory
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KeyspaceKey {
    Fixed(SectionKeyFixed),
    Dynamic(SectionKeyDynamic),
}

impl KeyspaceKey {
    pub fn bytes(&self) -> &[u8] {
        match self {
            KeyspaceKey::Fixed(fixed_key) => fixed_key.bytes(),
            KeyspaceKey::Dynamic(dynamic_key) => dynamic_key.bytes(),
        }
    }

    pub fn keyspace_id(&self) -> KeyspaceId {
        match self {
            KeyspaceKey::Fixed(fixed_key) => fixed_key.keyspace_id(),
            KeyspaceKey::Dynamic(dynamic_key) => dynamic_key.keyspace_id(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SectionKeyFixed {
    section_id: KeyspaceId,
    key_length: u64,
    data: [u8; FIXED_KEY_LENGTH_BYTES],
}

impl SectionKeyFixed {
    pub fn new(section_id: KeyspaceId, key_length: usize, data: [u8; FIXED_KEY_LENGTH_BYTES]) -> SectionKeyFixed {
        SectionKeyFixed {
            section_id: section_id,
            key_length: key_length as u64,
            data: data,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[0..(self.key_length as usize)]
    }

    fn keyspace_id(&self) -> KeyspaceId {
        self.section_id
    }
}

impl Serialize for SectionKeyFixed {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_struct("KeyFixed", 2)?;
        state.serialize_field("section_id", &self.section_id)?;
        state.serialize_field("data", self.bytes())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SectionKeyFixed {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        enum Field { SectionID, Data }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                where
                    D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`section_id` or `data`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                        where
                            E: de::Error,
                    {
                        match value {
                            "section_id" => Ok(Field::SectionID),
                            "data" => Ok(Field::Data),
                            _ => Err(de::Error::unknown_field(value, &["section_id", "data"])),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct KeyFixedVisitor;

        impl<'de> Visitor<'de> for KeyFixedVisitor {
            type Value = SectionKeyFixed;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Duration")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<SectionKeyFixed, V::Error>
                where
                    V: SeqAccess<'de>,
            {
                let section_id = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let bytes: &[u8] = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let length = bytes.len();
                let mut data = [0; FIXED_KEY_LENGTH_BYTES];
                data[0..length].copy_from_slice(bytes);

                Ok(SectionKeyFixed::new(section_id, length, data))
            }

            fn visit_map<V>(self, mut map: V) -> Result<SectionKeyFixed, V::Error>
                where
                    V: MapAccess<'de>,
            {
                let mut section_id = None;
                let mut bytes: Option<&[u8]> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::SectionID => {
                            if section_id.is_some() {
                                return Err(de::Error::duplicate_field("section_id"));
                            }
                            section_id = Some(map.next_value()?);
                        }
                        Field::Data => {
                            if bytes.is_some() {
                                return Err(de::Error::duplicate_field("data"));
                            }
                            bytes = Some(map.next_value()?);
                        }
                    }
                }
                let section_id = section_id.ok_or_else(|| de::Error::missing_field("section_id"))?;
                let bytes = bytes.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let length = bytes.len();
                let mut data = [0; FIXED_KEY_LENGTH_BYTES];
                data[0..length].copy_from_slice(bytes);

                Ok(SectionKeyFixed::new(section_id, length, data))
            }
        }

        deserializer.deserialize_struct("KeyFixed", &["section_id", "data"], KeyFixedVisitor)
    }
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SectionKeyDynamic {
    section_id: KeyspaceId,
    data: Box<[u8]>,
}

impl SectionKeyDynamic {
    pub fn new(section_id: KeyspaceId, data: Box<[u8]>) -> SectionKeyDynamic {
        SectionKeyDynamic {
            section_id: section_id,
            data: data,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    fn keyspace_id(&self) -> KeyspaceId {
        self.section_id
    }
}

impl PartialOrd<Self> for KeyspaceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyspaceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            KeyspaceKey::Fixed(fixedKey) => {
                match other {
                    KeyspaceKey::Fixed(otherFixedKey) => fixedKey.cmp(otherFixedKey),
                    KeyspaceKey::Dynamic(_) => Ordering::Less,
                }
            }
            KeyspaceKey::Dynamic(dynamicKey) => {
                match other {
                    KeyspaceKey::Fixed(_) => Ordering::Greater,
                    KeyspaceKey::Dynamic(otherDynamicKey) => dynamicKey.cmp(otherDynamicKey),
                }
            }
        }
    }
}

impl From<(Vec<u8>, u8)> for SectionKeyFixed {
    // For tests
    fn from((bytes, section_id): (Vec<u8>, u8)) -> Self {
        SectionKeyFixed::from((bytes.as_slice(), section_id))
    }
}

impl From<(&[u8], u8)> for SectionKeyFixed {
    // For tests
    fn from((bytes, section_id): (&[u8], u8)) -> Self {
        assert!(bytes.len() < FIXED_KEY_LENGTH_BYTES);
        let mut data = [0; FIXED_KEY_LENGTH_BYTES];
        data[0..bytes.len()].copy_from_slice(bytes);
        SectionKeyFixed {
            section_id: section_id,
            key_length: bytes.len() as u64,
            data: data,
        }
    }
}

impl PartialOrd<Self> for SectionKeyFixed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SectionKeyFixed {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = self.data.partial_cmp(&other.data).unwrap();
        if ordering.is_eq() {
            self.key_length.cmp(&other.key_length)
        } else {
            ordering
        }
    }
}

impl PartialOrd<Self> for SectionKeyDynamic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SectionKeyDynamic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Empty,
    Value(Box<[u8]>),
}

impl Value {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Value::Empty => &[0; 0],
            Value::Value(bytes) => bytes,
        }
    }

    pub fn has_value(&self) -> bool {
        match self {
            Value::Empty => false,
            Value::Value(_) => true,
        }
    }
}

impl From<Option<Box<[u8]>>> for Value {
    fn from(value: Option<Box<[u8]>>) -> Self {
        value.map_or_else(|| Value::Empty, |bytes| Value::Value(bytes))
    }
}
