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

use logger::result::ResultExt;
use struct_deser_derive::StructDeser;

use crate::{DeserialisableDynamic, EncodingError, EncodingErrorKind, Serialisable, SerialisableKeyFixed};

#[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct U16 {
    #[be]
    value: u16,
}

pub struct StringBytes {
    bytes: Box<[u8]>,
}

impl StringBytes {
    pub fn encode(value: &str) -> StringBytes {
        // todo: don't want to allocate to vec here
        StringBytes { bytes: value.as_bytes().to_vec().into_boxed_slice() }
    }

    pub fn decode(&self) -> String {
        // todo: don't want to allocate to vec here
        String::from_utf8(self.bytes.to_vec())
            .map_err(|err| {
                EncodingError {
                    kind: EncodingErrorKind::FailedUFT8Decode { bytes: self.bytes.clone(), source: err.utf8_error() }
                }
            }).unwrap_or_log()
    }

    pub fn decode_ref(&self) -> &str {
        std::str::from_utf8(self.bytes.as_ref())
            .map_err(|err| {
                EncodingError {
                    kind: EncodingErrorKind::FailedUFT8Decode { bytes: self.bytes.clone(), source: err }
                }
            }).unwrap_or_log()
    }

    pub fn to_bytes(self) -> Box<[u8]> {
        self.bytes
    }
}

impl Serialisable for StringBytes {
    fn serialised_size(&self) -> usize {
        return self.bytes.len();
    }

    fn serialise_into(&self, array: &mut [u8]) {
        debug_assert_eq!(array.len(), self.bytes.len());
        array.copy_from_slice(self.bytes.as_ref())
    }
}

impl DeserialisableDynamic for StringBytes {

    fn deserialise_from(array: Box<[u8]>) -> Self {
        StringBytes {
            bytes: array
        }
    }
}