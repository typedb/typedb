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

use struct_deser::IntoBytes;

const KEY_LENGTH_BYTES: usize = 48;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WriteKey {
    data: [u8; KEY_LENGTH_BYTES],
    key_length: usize,
}

impl WriteKey {
    pub fn bytes(&self) -> &[u8] {
        &self.data[0..self.key_length]
    }
}

impl From<Vec<u8>> for WriteKey {
    fn from(value: Vec<u8>) -> Self {
        assert!(value.len() < KEY_LENGTH_BYTES);

        let mut data = [0; KEY_LENGTH_BYTES];
        data[0..value.len()].copy_from_slice(value.as_slice());
        WriteKey {
            data: data,
            key_length: value.len(),
        }
    }
}

impl PartialOrd<Self> for WriteKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = self.data.partial_cmp(&other.data).unwrap();
        if ordering.is_eq() {
            self.key_length.partial_cmp(&other.key_length)
        } else {
            Some(ordering)
        }
    }
}

impl Ord for WriteKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}


pub trait WritableKey {
    fn to_writable_key(&self) -> WriteKey;
}

impl<T: IntoBytes> WritableKey for T {
    fn to_writable_key(&self) -> WriteKey {
        let mut data = [0; KEY_LENGTH_BYTES];
        self.into_bytes(&mut data[0..Self::BYTE_LEN]);
        WriteKey {
            data: data,
            key_length: Self::BYTE_LEN,
        }
    }
}

pub fn empty() -> Box<[u8; 0]> {
    Box::new([0; 0])
}