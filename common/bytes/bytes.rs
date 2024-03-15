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

use std::fmt;

pub mod byte_array;
pub mod byte_array_or_ref;
pub mod byte_reference;

// TODO: this needs to be optimised using bigger strides than a single byte!
///
/// Performs a big-endian +1 operation that errors on overflow
///
pub fn increment(bytes: &mut [u8]) -> Result<(), BytesError> {
    for byte in bytes.iter_mut().rev() {
        let (val, overflow) = byte.overflowing_add(1);
        *byte = val;
        if !overflow {
            return Ok(());
        }
    }
    return Err(BytesError { kind: BytesErrorKind::IncrementOverflow {} });
}

///
/// Performs a 'const' big-endian +1 operation that panics on overflow
///
pub const fn increment_fixed<const SIZE: usize>(mut bytes: [u8; SIZE]) -> [u8; SIZE] {
    let mut index = SIZE;
    while index > 0 {
        let (val, overflow) = bytes[index - 1].overflowing_add(1);
        bytes[index - 1] = val;
        if overflow {
            panic!("Overflow while incrementing array")
        }
        index -= 1;
    }
    bytes
}

#[derive(Debug)]
pub struct BytesError {
    pub kind: BytesErrorKind,
}

#[derive(Debug)]
pub enum BytesErrorKind {
    IncrementOverflow {},
}

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
