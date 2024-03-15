/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


#[derive(Debug, Copy, Clone)]
pub struct Long {
    bytes: [u8; Long::LENGTH],
}

impl Long {
    const LENGTH: usize = 8;

    pub fn new(bytes: [u8; Long::LENGTH]) -> Self {
        Self { bytes: bytes }
    }

    pub fn build(long: i64) -> Self {
        Self { bytes: (long ^ i64::MIN) .to_be_bytes() }
    }

    pub fn as_i64(&self) -> i64 {
        i64::from_be_bytes(self.bytes) ^ i64::MIN
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}
