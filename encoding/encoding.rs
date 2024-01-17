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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;

use struct_deser::SerializedByteLen;
use struct_deser_derive::StructDeser;

pub mod thing;
pub mod type_;

pub const PREFIX_SIZE: usize = 1;
pub const INFIX_SIZE: usize = 1;

pub enum Prefix {
    EntityType,
    RelationType,
    AttributeType,

    TypeLabelIndex,
    LabelTypeIndex,

    Entity,
    Attribute,
}

#[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PrefixID {
    id: [u8; PREFIX_SIZE],
}

impl PrefixID {
    pub(crate) const fn size() -> usize {
        PrefixID::BYTE_LEN
    }
}

impl Prefix {
    pub const fn as_bytes(&self) -> [u8; PREFIX_SIZE] {
        match self {
            Prefix::EntityType => [0],
            Prefix::RelationType => [1],
            Prefix::AttributeType => [2],
            Prefix::TypeLabelIndex => [20],
            Prefix::LabelTypeIndex => [21],
            Prefix::Entity => [100],
            Prefix::Attribute => [101],
        }
    }

    // TODO: this is hard to maintain relative to the above - we should convert the pair into a macro
    pub const fn as_bytes_next(&self) -> [u8; PREFIX_SIZE] {
        match self {
            Prefix::EntityType => [1],
            Prefix::RelationType => [2],
            Prefix::AttributeType => [3],
            Prefix::TypeLabelIndex => [21],
            Prefix::LabelTypeIndex => [22],
            Prefix::Entity => [101],
            Prefix::Attribute => [102],
        }
    }

    pub const fn as_id(&self) -> PrefixID {
        PrefixID { id: self.as_bytes() }
    }
}

pub enum Infix {
    HasForward,
    HasBackward,
}

impl Infix {
    pub(crate) const fn as_bytes(&self) -> [u8; INFIX_SIZE] {
        match self {
            Infix::HasForward => [0],
            Infix::HasBackward => [1],
        }
    }
}

// TODO: review efficiency/style of encoding values
mod value {
    use logger::result::ResultExt;
    use struct_deser_derive::StructDeser;

    use crate::{EncodingError, EncodingErrorKind};

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
}


#[derive(Debug)]
pub struct EncodingError {
    pub kind: EncodingErrorKind,
}

#[derive(Debug)]
pub enum EncodingErrorKind {
    FailedUFT8Decode { bytes: Box<[u8]>, source: Utf8Error }
}

impl Display for EncodingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for EncodingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            EncodingErrorKind::FailedUFT8Decode { source, .. } => Some(source),
        }
    }
}