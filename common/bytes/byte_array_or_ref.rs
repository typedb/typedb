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

use std::fmt::{Display, Formatter};
use std::fs::remove_file;

use crate::byte_array::ByteArray;
use crate::byte_reference::ByteReference;

#[derive(Debug)]
pub enum ByteArrayOrRef<'bytes, const ARRAY_INLINE_SIZE: usize> {
    Array(ByteArray<ARRAY_INLINE_SIZE>),
    Reference(ByteReference<'bytes>),
}

impl<'bytes, const INLINE_SIZE: usize> Clone for ByteArrayOrRef<'bytes, INLINE_SIZE> {
    fn clone(&self) -> ByteArrayOrRef<'static, INLINE_SIZE> {
        match self {
            ByteArrayOrRef::Array(array) => ByteArrayOrRef::Array(array.clone()),
            ByteArrayOrRef::Reference(reference) => ByteArrayOrRef::Array(ByteArray::from(reference.clone())),
        }
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> ByteArrayOrRef<'bytes, ARRAY_INLINE_SIZE> {
    pub fn bytes(&'bytes self) -> &'bytes [u8] {
        match self {
            ByteArrayOrRef::Array(array) => array.bytes(),
            ByteArrayOrRef::Reference(reference) => reference.bytes(),
        }
    }

    pub fn length(&self) -> usize {
        match self {
            ByteArrayOrRef::Array(array) => array.length(),
            ByteArrayOrRef::Reference(reference) => reference.length(),
        }
    }

    pub fn truncate(self, length: usize) -> ByteArrayOrRef<'bytes, ARRAY_INLINE_SIZE> {
        assert!(length <= self.length());
        match self {
            ByteArrayOrRef::Array(mut array) => {
                array.truncate(length);
                ByteArrayOrRef::Array(array)
            }
            ByteArrayOrRef::Reference(reference) => ByteArrayOrRef::Reference(
                reference.truncate(length)
            )
        }
    }

    pub fn to_owned(&self) -> ByteArrayOrRef<'static, ARRAY_INLINE_SIZE> {
        ByteArrayOrRef::Array(self.to_array())
    }

    pub fn into_owned(self) -> ByteArrayOrRef<'static, ARRAY_INLINE_SIZE> {
        ByteArrayOrRef::Array(self.into_array())
    }

    pub fn as_reference(&'bytes self) -> ByteReference<'bytes> {
        match self {
            ByteArrayOrRef::Array(array) => ByteReference::from(array),
            ByteArrayOrRef::Reference(reference) => reference.clone()
        }
    }

    pub fn unwrap_reference(self) -> ByteReference<'bytes> {
        if let ByteArrayOrRef::Reference(reference) = self {
            reference
        } else {
            panic!("{} cannot be unwrapped as a reference", self)
        }
    }

    pub fn into_array(self) -> ByteArray<ARRAY_INLINE_SIZE> {
        match self {
            ByteArrayOrRef::Array(array) => array,
            ByteArrayOrRef::Reference(byte_reference) => ByteArray::from(byte_reference),
        }
    }

    pub fn to_array(&self) -> ByteArray<ARRAY_INLINE_SIZE> {
        match self {
            ByteArrayOrRef::Array(array) => array.clone(),
            ByteArrayOrRef::Reference(byte_reference) => ByteArray::from(byte_reference.clone()),
        }
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Display for ByteArrayOrRef<'bytes, ARRAY_INLINE_SIZE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", dbg!(self))
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> PartialEq for ByteArrayOrRef<'bytes, ARRAY_INLINE_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes().eq(other.bytes())
    }
}

impl<'bytes, const ARRAY_INLINE_SIZE: usize> Eq for ByteArrayOrRef<'bytes, ARRAY_INLINE_SIZE> {}
