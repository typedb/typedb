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


pub mod concept {
    use std::mem::transmute;
    use wal::SequenceNumber;
    use crate::PrefixID;

    const TYPE_ID_SIZE: usize = 2;

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    struct TypeIIDSequenced {
        iid: TypeIID,
        sequence_number: SequenceNumber,
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeIID {
        prefix: PrefixID,
        id: TypeID,
    }

    impl TypeIID {
        pub fn new(prefix: PrefixID, type_id: TypeID) -> TypeIID {
            TypeIID { prefix: prefix, id: type_id }
        }

        pub fn prefix(&self) -> PrefixID {
            self.prefix
        }

        pub(crate) const fn size() -> usize {
            std::mem::size_of::<Self>()
        }

        pub fn as_bytes(&self) -> &[u8; Self::size()] {
            unsafe {
                transmute(self)
            }
        }
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeID {
        bytes: [u8; TYPE_ID_SIZE],
    }

    impl TypeID {
        pub fn new(id: [u8; TYPE_ID_SIZE]) -> TypeID {
            TypeID { bytes: id }
        }

        pub(crate) const fn size() -> usize {
            std::mem::size_of::<Self>()
        }

        pub fn as_bytes(&self) -> &[u8; Self::size()] {
            unsafe {
                transmute(self)
            }
        }
    }
}

pub mod index {
    use std::io::Read;
    use std::mem::transmute;
    use crate::{Prefix, PrefixID, value};
    use crate::type_::type_encoding::concept::TypeIID;
    use crate::value::StringBytes;

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct TypeIIDLabelIndex {
        prefix: PrefixID,
        iid: TypeIID,
    }

    impl TypeIIDLabelIndex {
        pub fn new(iid: TypeIID, label: &str) -> (TypeIIDLabelIndex, StringBytes) {
            (
                TypeIIDLabelIndex {
                    prefix: Prefix::TypeLabelIndex.as_id(),
                    iid: iid,
                },
                StringBytes::encode(label)
            )
        }

        pub(crate) const fn size() -> usize {
            std::mem::size_of::<Self>()
        }

        pub fn as_bytes(&self) -> &[u8; Self::size()] {
            unsafe {
                transmute(self)
            }
        }
    }

    pub struct LabelTypeIIDIndex {
        prefix: PrefixID,
        label: StringBytes,
    }

    impl LabelTypeIIDIndex {
        pub fn new(label: &str, iid: TypeIID) -> (LabelTypeIIDIndex, TypeIID) {
            (
                LabelTypeIIDIndex {
                    prefix: Prefix::LabelTypeIndex.as_id(),
                    label: StringBytes::encode(label),
                },
                iid
            )
        }

        pub fn to_bytes(&self) -> Box<[u8]> {
            self.prefix.as_bytes().iter()
                .chain(self.label.as_bytes().iter())
                .map(|byte| byte.clone())
                .collect::<Vec<u8>>().into_boxed_slice()
        }
    }
}

mod connection {}

