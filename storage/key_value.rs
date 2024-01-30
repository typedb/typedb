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

use crate::SectionId;

pub const FIXED_KEY_LENGTH_BYTES: usize = 48;

// TODO: we will need to know if these are from storage or from memory
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Key {
    Fixed(KeyFixed),
    Dynamic(KeyDynamic),
}

impl Key {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Key::Fixed(fixed_key) => fixed_key.bytes(),
            Key::Dynamic(dynamic_key) => dynamic_key.bytes(),
        }
    }

    pub fn section_id(&self) -> SectionId {
        match self {
            Key::Fixed(fixed_key) => fixed_key.section_id(),
            Key::Dynamic(dynamic_key) => dynamic_key.section_id(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct KeyFixed {
    section_id: SectionId,
    key_length: u64,
    data: [u8; FIXED_KEY_LENGTH_BYTES],
}

impl KeyFixed {
    pub fn new(section_id: SectionId, key_length: usize, data: [u8; FIXED_KEY_LENGTH_BYTES]) -> KeyFixed {
        KeyFixed {
            section_id: section_id,
            key_length: key_length as u64,
            data: data,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[0..(self.key_length as usize)]
    }

    fn section_id(&self) -> SectionId {
        self.section_id
    }
}

impl Serialize for KeyFixed {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_struct("KeyFixed", 2)?;
        state.serialize_field("section_id", &self.section_id)?;
        state.serialize_field("data", self.bytes())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for KeyFixed {
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
            type Value = KeyFixed;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Duration")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<KeyFixed, V::Error>
                where
                    V: SeqAccess<'de>,
            {
                let section_id = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let bytes: &[u8] = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let length = bytes.len();
                let mut data = [0; FIXED_KEY_LENGTH_BYTES];
                data[0..length].copy_from_slice(bytes);

                Ok(KeyFixed::new(section_id, length, data))
            }

            fn visit_map<V>(self, mut map: V) -> Result<KeyFixed, V::Error>
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
                        Field::Data  => {
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

                Ok(KeyFixed::new(section_id, length, data))
            }
        }


        deserializer.deserialize_struct("KeyFixed", &["section_id", "data"], KeyFixedVisitor)
    }
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct KeyDynamic {
    section_id: SectionId,
    data: Box<[u8]>,
}

impl KeyDynamic {
    pub fn new(section_id: SectionId, data: Box<[u8]>) -> KeyDynamic {
        KeyDynamic {
            section_id: section_id,
            data: data,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    fn section_id(&self) -> SectionId {
        self.section_id
    }
}

impl PartialOrd<Self> for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            Key::Fixed(fixedKey) => {
                match other {
                    Key::Fixed(otherFixedKey) => fixedKey.cmp(otherFixedKey),
                    Key::Dynamic(_) => Ordering::Less,
                }
            }
            Key::Dynamic(dynamicKey) => {
                match other {
                    Key::Fixed(_) => Ordering::Greater,
                    Key::Dynamic(otherDynamicKey) => dynamicKey.cmp(otherDynamicKey),
                }
            }
        }
    }
}

impl From<(Vec<u8>, u8)> for KeyFixed {
    // For tests
    fn from((bytes, section_id): (Vec<u8>, u8)) -> Self {
        KeyFixed::from((bytes.as_slice(), section_id))
    }
}

impl From<(&[u8], u8)> for KeyFixed {
    // For tests
    fn from((bytes, section_id): (&[u8], u8)) -> Self {
        assert!(bytes.len() < FIXED_KEY_LENGTH_BYTES);
        let mut data = [0; FIXED_KEY_LENGTH_BYTES];
        data[0..bytes.len()].copy_from_slice(bytes);
        KeyFixed {
            section_id: section_id,
            key_length: bytes.len() as u64,
            data: data,
        }
    }
}

impl PartialOrd<Self> for KeyFixed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyFixed {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = self.data.partial_cmp(&other.data).unwrap();
        if ordering.is_eq() {
            self.key_length.cmp(&other.key_length)
        } else {
            ordering
        }
    }
}

impl PartialOrd<Self> for KeyDynamic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyDynamic {
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
