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

use std::fmt::{Display, Formatter};

pub mod byte_array_or_ref;
pub mod byte_array;
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


#[derive(Debug)]
pub struct BytesError {
    pub kind: BytesErrorKind,
}

#[derive(Debug)]
pub enum BytesErrorKind {
    IncrementOverflow {},
}

impl Display for BytesError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
