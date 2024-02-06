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

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ByteReference<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> ByteReference<'bytes> {
    pub fn new(bytes: &'bytes [u8]) -> ByteReference<'bytes> {
        ByteReference {
            bytes: bytes
        }
    }

    pub fn bytes(&self) -> &'bytes [u8] {
        self.bytes
    }

    pub(crate) fn truncate(mut self, length: usize) -> ByteReference<'bytes> {
        assert!(length <= self.bytes.len());
        ByteReference {
            bytes: &self.bytes[0..length]
        }
    }
}
