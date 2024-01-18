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

pub const FIXED_KEY_LENGTH_BYTES: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteKey {
    Fixed(WriteKeyFixed),
    Dynamic(WriteKeyDynamic),
}

impl WriteKey {
    pub fn bytes(&self) -> &[u8] {
        match self {
            WriteKey::Fixed(fixed_key) => fixed_key.bytes(),
            WriteKey::Dynamic(dynamic_key) => dynamic_key.bytes(),
        }
    }

    pub fn section_id(&self) -> u8 {
        match self {
            WriteKey::Fixed(fixed_key) => fixed_key.section_id(),
            WriteKey::Dynamic(dynamic_key) => dynamic_key.section_id(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WriteKeyFixed {
    section_id: u8,
    key_length: usize,
    data: [u8; FIXED_KEY_LENGTH_BYTES],
}

impl WriteKeyFixed {
    pub fn new(section_id: u8, key_length: usize, data: [u8; FIXED_KEY_LENGTH_BYTES]) -> WriteKeyFixed {
        WriteKeyFixed {
            section_id: section_id,
            key_length: key_length,
            data: data,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[0..self.key_length]
    }

    fn section_id(&self) -> u8 {
        self.section_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteKeyDynamic {
    section_id: u8,
    data: Box<[u8]>,
}

impl WriteKeyDynamic {
    pub fn new(section_id: u8, data: Box<[u8]>) -> WriteKeyDynamic {
        WriteKeyDynamic {
            section_id: section_id,
            data: data
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    fn section_id(&self) -> u8 {
        self.section_id
    }
}

impl PartialOrd<Self> for WriteKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WriteKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            WriteKey::Fixed(fixedKey) => {
                match other {
                    WriteKey::Fixed(otherFixedKey) => fixedKey.cmp(otherFixedKey),
                    WriteKey::Dynamic(_) => Ordering::Less,
                }
            }
            WriteKey::Dynamic(dynamicKey) => {
                match other {
                    WriteKey::Fixed(_) => Ordering::Greater,
                    WriteKey::Dynamic(otherDynamicKey) => dynamicKey.cmp(otherDynamicKey),
                }
            }
        }
    }
}

impl From<(u8, Vec<u8>)> for WriteKeyFixed {
    // For tests
    fn from((section_id, value): (u8, Vec<u8>)) -> Self {
        assert!(value.len() < FIXED_KEY_LENGTH_BYTES);

        let mut data = [0; FIXED_KEY_LENGTH_BYTES];
        data[0..value.len()].copy_from_slice(value.as_slice());
        WriteKeyFixed {
            section_id: section_id,
            key_length: value.len(),
            data: data,
        }
    }
}

impl PartialOrd<Self> for WriteKeyFixed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WriteKeyFixed {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = self.data.partial_cmp(&other.data).unwrap();
        if ordering.is_eq() {
            self.key_length.cmp(&other.key_length)
        } else {
            ordering
        }
    }
}

impl PartialOrd<Self> for WriteKeyDynamic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WriteKeyDynamic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

pub fn empty() -> Box<[u8; 0]> {
    Box::new([0; 0])
}