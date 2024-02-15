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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;

use crate::byte_array::ByteArrayErrorKind::Overflow;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ByteArray<const INLINE_BYTES: usize> {
    Inline(ByteArrayInline<INLINE_BYTES>),
    Boxed(ByteArrayBoxed),
}

impl<const INLINE_BYTES: usize> ByteArray<INLINE_BYTES> {
    pub const fn empty() -> ByteArray<INLINE_BYTES> {
        ByteArray::Inline(ByteArrayInline::empty())
    }

    pub fn zeros(length: usize) -> ByteArray<INLINE_BYTES> {
        if length < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::zeros(length))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::zeros(length))
        }
    }

    pub fn from(bytes: &[u8]) -> ByteArray<INLINE_BYTES> {
        if bytes.len() < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::from(bytes))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::from(bytes))
        }
    }

    pub fn from_2(bytes_1: &[u8], bytes_2: &[u8]) -> ByteArray<INLINE_BYTES> {
        let length = bytes_1.len() + bytes_2.len();
        if length < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::from_2(bytes_1, bytes_2))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::from_2(bytes_1, bytes_2))
        }
    }

    pub fn from_3(bytes_1: &[u8], bytes_2: &[u8], bytes_3: &[u8]) -> ByteArray<INLINE_BYTES> {
        let length = bytes_1.len() + bytes_2.len() + bytes_3.len();
        if length < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::from_3(bytes_1, bytes_2, bytes_3))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::from_3(bytes_1, bytes_2, bytes_3))
        }
    }

    pub fn new(bytes: [u8; INLINE_BYTES], length: usize) -> ByteArray<INLINE_BYTES> {
        ByteArray::Inline(ByteArrayInline::new(bytes, length))
    }

    pub fn boxed(bytes: Box<[u8]>) -> ByteArray<INLINE_BYTES> {
        ByteArray::Boxed(ByteArrayBoxed::wrap(bytes))
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            ByteArray::Inline(inline) => inline.bytes(),
            ByteArray::Boxed(boxed) => boxed.bytes(),
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        match self {
            ByteArray::Inline(inline) => inline.bytes_mut(),
            ByteArray::Boxed(boxed) => boxed.bytes_mut(),
        }
    }

    pub fn length(&self) -> usize {
        self.bytes().len()
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.length());
        match self {
            ByteArray::Inline(inline) => inline.truncate(length),
            ByteArray::Boxed(boxed) => boxed.truncate(length),
        }
    }

    pub fn starts_with(&self, bytes: &[u8]) -> bool {
        self.length() >= bytes.len() && &self.bytes()[0..bytes.len()] == bytes
    }

    // TODO: this needs to be optimised using bigger strides than a single byte!
    ///
    /// Performs a big-endian overflowing +1 operation
    ///
    pub fn increment(&mut self) -> Result<(), ByteArrayError> {
        for byte in self.bytes_mut().iter_mut().rev() {
            let (val, overflow) = byte.overflowing_add(1);
            *byte = val;
            if !overflow {
                return Ok(());
            }
        }
        return Err(ByteArrayError { kind: Overflow {} });
    }
}


#[derive(Debug, Clone)]
pub struct ByteArrayInline<const BYTES: usize> {
    data: [u8; BYTES],
    length: u64,
}

impl<const BYTES: usize> ByteArrayInline<BYTES> {
    const fn empty() -> ByteArrayInline<BYTES> {
        ByteArrayInline {
            data: [0; BYTES],
            length: 0,
        }
    }

    fn zeros(length: usize) -> ByteArrayInline<BYTES> {
        assert!(length < BYTES);
        ByteArrayInline {
            data: [0; BYTES],
            length: length as u64,
        }
    }

    fn from(bytes: &[u8]) -> ByteArrayInline<BYTES> {
        let length = bytes.len();
        assert!(length < BYTES);
        let mut data = [0; BYTES];
        data[0..length].copy_from_slice(bytes);
        ByteArrayInline {
            data: data,
            length: length as u64,
        }
    }

    fn from_2(bytes_1: &[u8], bytes_2: &[u8]) -> ByteArrayInline<BYTES> {
        let length = bytes_1.len() + bytes_2.len();
        assert!(length < BYTES);
        let mut data = [0; BYTES];

        let end_1 = bytes_1.len();
        let end_2 = end_1 + bytes_2.len();

        data[0..end_1].copy_from_slice(bytes_1);
        data[end_1..end_2].copy_from_slice(bytes_2);
        ByteArrayInline {
            data: data,
            length: length as u64,
        }
    }

    fn from_3(bytes_1: &[u8], bytes_2: &[u8], bytes_3: &[u8]) -> ByteArrayInline<BYTES> {
        let length = bytes_1.len() + bytes_2.len() + bytes_3.len();
        assert!(length < BYTES);
        let mut data = [0; BYTES];

        let end_1 = bytes_1.len();
        let end_2 = end_1 + bytes_2.len();
        let end_3 = end_2 + bytes_3.len();

        data[0..end_1].copy_from_slice(bytes_1);
        data[end_1..end_2].copy_from_slice(bytes_2);
        data[end_2..end_3].copy_from_slice(bytes_3);
        ByteArrayInline {
            data: data,
            length: length as u64,
        }
    }

    fn new(bytes: [u8; BYTES], length: usize) -> ByteArrayInline<BYTES> {
        ByteArrayInline {
            data: bytes,
            length: length as u64,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[0..(self.length as usize)]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[0..(self.length as usize)]
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length as u64 <= self.length);
        self.length = length as u64
    }
}

impl<const SIZE: usize> Serialize for ByteArrayInline<SIZE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_struct("ByteArrayInline", 1)?;
        state.serialize_field("data", self.bytes())?;
        state.end()
    }
}

impl<'de, const SIZE: usize> Deserialize<'de> for ByteArrayInline<SIZE> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        enum Field { Data }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                where
                    D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`data`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                        where
                            E: de::Error,
                    {
                        match value {
                            "data" => Ok(Field::Data),
                            _ => Err(de::Error::unknown_field(value, &["data"])),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ByteArrayInlineVisitor<const SIZE: usize>;

        impl<'de, const SIZE: usize> Visitor<'de> for ByteArrayInlineVisitor<SIZE> {
            type Value = ByteArrayInline<SIZE>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ByteArrayInlineVisitor")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<ByteArrayInline<SIZE>, V::Error>
                where
                    V: SeqAccess<'de>,
            {
                let bytes: &[u8] = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(ByteArrayInline::from(bytes))
            }

            fn visit_map<V>(self, mut map: V) -> Result<ByteArrayInline<SIZE>, V::Error>
                where
                    V: MapAccess<'de>,
            {
                let mut bytes: Option<&[u8]> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Data => {
                            if bytes.is_some() {
                                return Err(de::Error::duplicate_field("data"));
                            }
                            bytes = Some(map.next_value()?);
                        }
                    }
                }
                let bytes = bytes.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(ByteArrayInline::from(bytes))
            }
        }

        deserializer.deserialize_struct("ByteArrayInline", &["data"], ByteArrayInlineVisitor)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ByteArrayBoxed {
    data: Box<[u8]>,
    length: usize,
}

impl ByteArrayBoxed {
    fn zeros(length: usize) -> ByteArrayBoxed {
        ByteArrayBoxed {
            data: vec![0; length].into_boxed_slice(),
            length: length,
        }
    }

    fn from(bytes: &[u8]) -> ByteArrayBoxed {
        ByteArrayBoxed {
            data: Box::from(bytes),
            length: bytes.len(),
        }
    }

    fn from_2(bytes_1: &[u8], bytes_2: &[u8]) -> ByteArrayBoxed {
        let length = bytes_1.len() + bytes_2.len();
        let mut data = vec![0; length].into_boxed_slice();

        let end_1 = bytes_1.len();
        let end_2 = end_1 + bytes_2.len();

        data[0..end_1].copy_from_slice(bytes_1);
        data[end_1..end_2].copy_from_slice(bytes_2);

        ByteArrayBoxed {
            data: data,
            length: length,
        }
    }

    fn from_3(bytes_1: &[u8], bytes_2: &[u8], bytes_3: &[u8]) -> ByteArrayBoxed {
        let length = bytes_1.len() + bytes_2.len() + bytes_3.len();
        let mut data = vec![0; length].into_boxed_slice();

        let end_1 = bytes_1.len();
        let end_2 = end_1 + bytes_2.len();
        let end_3 = end_2 + bytes_3.len();

        data[0..end_1].copy_from_slice(bytes_1);
        data[end_1..end_2].copy_from_slice(bytes_2);
        data[end_2..end_3].copy_from_slice(bytes_3);

        ByteArrayBoxed {
            data: data,
            length: length,
        }
    }

    fn wrap(bytes: Box<[u8]>) -> ByteArrayBoxed {
        ByteArrayBoxed {
            length: bytes.len(),
            data: bytes,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.data[0..self.length]
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[0..self.length]
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.length);
        self.length = length;
    }
}

impl<const INLINE_SIZE: usize> Borrow<[u8]> for ByteArray<INLINE_SIZE> {
    fn borrow(&self) -> &[u8] {
        self.bytes()
    }
}

impl<const INLINE_BYTES: usize> PartialOrd<Self> for ByteArray<INLINE_BYTES> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const INLINE_BYTES: usize> Ord for ByteArray<INLINE_BYTES> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

impl<const INLINE_BYTES: usize> PartialEq<Self> for ByteArray<INLINE_BYTES> {
    fn eq(&self, other: &Self) -> bool {
        // Note: we assume boxed and inline will never be equal since they are split by size
        if matches!(self, ByteArray::Inline(_)) && matches!(other, ByteArray::Boxed(_)) {
            false
        } else if matches!(self, ByteArray::Boxed(_)) && matches!(other, ByteArray::Inline(_)) {
            return false;
        } else {
            self.bytes() == other.bytes()
        }
    }
}

impl<const INLINE_BYTES: usize> Eq for ByteArray<INLINE_BYTES> {}


#[derive(Debug)]
pub struct ByteArrayError {
    pub kind: ByteArrayErrorKind,
}

#[derive(Debug)]
pub enum ByteArrayErrorKind {
    Overflow {},
}

impl Display for ByteArrayError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
