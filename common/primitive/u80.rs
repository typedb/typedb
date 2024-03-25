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

use std::ops::{Add, Sub};

use serde::{Deserialize, Serialize};

///
///  TODO: we will have to see if its more efficient to always represent as a number (if most operations will be numbers)
///        or most of the time we will be serialising into byte arrays, so we should keep as byte array
///
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct U80 {
    number: u128,
}

impl U80 {
    pub const MIN: U80 = U80 { number: 0x0000_0000_0000_0000_0000_0000_0000_0000 };
    pub const MAX: U80 = U80 { number: 0x0000_0000_0000_ffff_ffff_ffff_ffff_ffff };
    pub const BYTES: usize = 10;

    pub fn new(number: u128) -> U80 {
        assert!(number < U80::MAX.number);
        U80 { number }
    }

    pub fn number(&self) -> u128 {
        self.number
    }

    pub fn to_be_bytes(&self) -> [u8; U80::BYTES] {
        let u128_bytes = self.number.to_be_bytes();
        let mut u80_bytes = [0; U80::BYTES];
        let range = &u128_bytes[6..];
        debug_assert_eq!(range.len(), U80::BYTES);
        u80_bytes.copy_from_slice(range);
        u80_bytes
    }

    pub fn from_be_bytes(bytes: &[u8]) -> U80 {
        debug_assert_eq!(bytes.len(), U80::BYTES);
        let mut u128_bytes = [0; 16];
        u128_bytes[6..].copy_from_slice(bytes);
        U80::new(u128::from_be_bytes(u128_bytes))
    }
}

impl Add for U80 {
    type Output = U80;
    fn add(self, rhs: Self) -> Self::Output {
        U80 { number: self.number + rhs.number }
    }
}

impl Sub for U80 {
    type Output = U80;
    fn sub(self, rhs: Self) -> Self::Output {
        U80 { number: self.number - rhs.number }
    }
}
