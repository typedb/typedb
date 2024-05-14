/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Range,
};

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::{
    byte_reference::ByteReference,
    util::{increment, BytesError},
};

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
        if length <= INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::zeros(length))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::zeros(length))
        }
    }

    pub fn copy(bytes: &[u8]) -> ByteArray<INLINE_BYTES> {
        if bytes.len() < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::from(bytes))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::from(bytes))
        }
    }

    pub fn copy_concat<const N: usize>(slices: [&[u8]; N]) -> ByteArray<INLINE_BYTES> {
        let length: usize = slices.iter().map(|slice| slice.len()).sum();
        if length < INLINE_BYTES {
            ByteArray::Inline(ByteArrayInline::concat(slices))
        } else {
            ByteArray::Boxed(ByteArrayBoxed::concat(slices))
        }
    }

    pub fn inline(bytes: [u8; INLINE_BYTES], length: usize) -> ByteArray<INLINE_BYTES> {
        ByteArray::Inline(ByteArrayInline::new(bytes, length))
    }

    pub fn boxed(bytes: Box<[u8]>) -> ByteArray<INLINE_BYTES> {
        ByteArray::Boxed(ByteArrayBoxed::wrap(bytes))
    }

    pub fn as_ref(&self) -> ByteReference<'_> {
        ByteReference::new(self.bytes())
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

    pub fn truncate_range(&mut self, range: Range<usize>) {
        assert!(range.start <= self.length() && range.end <= self.length());
        match self {
            ByteArray::Inline(inline) => inline.truncate_range(range),
            ByteArray::Boxed(boxed) => boxed.truncate_range(range),
        }
    }

    pub fn starts_with(&self, bytes: &[u8]) -> bool {
        self.length() >= bytes.len() && &self.bytes()[0..bytes.len()] == bytes
    }

    pub fn increment(&mut self) -> Result<(), BytesError> {
        increment(self.bytes_mut())
    }
}

impl<const BYTES: usize> From<ByteReference<'_>> for ByteArray<BYTES> {
    fn from(byte_reference: ByteReference<'_>) -> Self {
        ByteArray::copy(byte_reference.bytes())
    }
}

impl<const INLINE_BYTES: usize> Hash for ByteArray<INLINE_BYTES> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes().hash(state)
    }
}

#[derive(Clone)]
pub struct ByteArrayInline<const BYTES: usize> {
    data: [u8; BYTES],
    start: u8,
    length: u8,
}

impl<const BYTES: usize> ByteArrayInline<BYTES> {
    const fn checked_build(data: [u8; BYTES], start: u8, length: u8) -> Self {
        assert!(BYTES < u8::MAX as usize);
        assert!(length <= BYTES as u8);
        Self { data, start, length }
    }

    const fn empty() -> Self {
        Self::checked_build([0; BYTES], 0, 0)
    }

    const fn zeros(length: usize) -> Self {
        Self::checked_build([0; BYTES], 0, length as u8)
    }

    fn from(bytes: &[u8]) -> Self {
        let length = bytes.len();
        assert!(length <= BYTES);
        let mut data = [0; BYTES];
        data[..length].copy_from_slice(bytes);
        Self::checked_build(data, 0, length as u8)
    }

    fn concat<const N: usize>(slices: [&[u8]; N]) -> Self {
        let total_length: usize = slices.iter().map(|slice| slice.len()).sum();
        assert!(total_length <= BYTES);
        let mut data = [0; BYTES];
        let mut end = 0;
        for slice in slices {
            data[end..][..slice.len()].copy_from_slice(slice);
            end += slice.len();
        }
        Self::checked_build(data, 0, total_length as u8)
    }

    fn new(bytes: [u8; BYTES], length: usize) -> Self {
        Self::checked_build(bytes, 0, length as u8)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[self.start as usize..][..self.length as usize]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start as usize..][..self.length as usize]
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.length as usize);
        self.length = length as u8;
    }

    pub fn truncate_range(&mut self, range: Range<usize>) {
        assert!(range.end <= self.length as usize);
        self.start += range.start as u8;
        self.length = range.len() as u8;
    }
}

impl<const SIZE: usize> Serialize for ByteArrayInline<SIZE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ByteArrayInline", 1)?;
        state.serialize_field("data", self.bytes())?;
        state.end()
    }
}

impl<'de, const SIZE: usize> Deserialize<'de> for ByteArrayInline<SIZE> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Data,
        }

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

impl<const BYTES: usize> fmt::Debug for ByteArrayInline<BYTES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}, allocated_size: {}]", self.bytes(), self.length)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ByteArrayBoxed {
    data: Box<[u8]>,
    start: usize,
    length: usize,
}

impl ByteArrayBoxed {
    fn zeros(length: usize) -> ByteArrayBoxed {
        ByteArrayBoxed { data: vec![0; length].into_boxed_slice(), start: 0, length }
    }

    fn from(bytes: &[u8]) -> ByteArrayBoxed {
        ByteArrayBoxed { data: Box::from(bytes), start: 0, length: bytes.len() }
    }

    fn concat<const N: usize>(slices: [&[u8]; N]) -> ByteArrayBoxed {
        let data = slices.concat().into_boxed_slice();
        ByteArrayBoxed { length: data.len(), start: 0, data }
    }

    fn wrap(bytes: Box<[u8]>) -> ByteArrayBoxed {
        ByteArrayBoxed { length: bytes.len(), start: 0, data: bytes }
    }

    fn bytes(&self) -> &[u8] {
        &self.data[self.start..self.start + self.length]
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start..self.start + self.length]
    }

    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.length);
        self.length = length;
    }

    pub fn truncate_range(&mut self, range: Range<usize>) {
        assert!(range.start >= self.start && range.end <= self.start + self.length && range.len() <= self.length);
        self.start = range.start;
        self.length = range.len();
    }
}

impl<const INLINE_SIZE: usize> Borrow<[u8]> for ByteArray<INLINE_SIZE> {
    fn borrow(&self) -> &[u8] {
        self.bytes()
    }
}

impl<const INLINE_BYTES: usize> PartialOrd for ByteArray<INLINE_BYTES> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const INLINE_BYTES: usize> Ord for ByteArray<INLINE_BYTES> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

impl<const INLINE_BYTES: usize> PartialEq for ByteArray<INLINE_BYTES> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<const INLINE_BYTES: usize> Eq for ByteArray<INLINE_BYTES> {}

impl<const INLINE_BYTES: usize> PartialEq<[u8]> for ByteArray<INLINE_BYTES> {
    fn eq(&self, other: &[u8]) -> bool {
        self.bytes() == other
    }
}
