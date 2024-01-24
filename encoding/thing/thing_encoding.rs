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
    use std::mem;

    use struct_deser_derive::StructDeser;

    use crate::{EncodingSection, SerialisableKeyFixed};
    use crate::prefix::PrefixID;
    use crate::type_::type_encoding::concept::TypeID;

    const OBJECT_ID_SIZE: usize = 8;
    const ATTRIBUTE_ID_SIZE: usize = 12;

    #[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq)]
    pub struct ObjectIID {
        prefix: PrefixID,
        type_id: TypeID,
        object_id: ObjectID,
    }

    impl ObjectIID {
        pub fn new(prefix: PrefixID, type_id: TypeID, object_id: ObjectID) -> ObjectIID {
            ObjectIID { prefix: prefix, type_id: type_id, object_id: object_id }
        }
    }

    impl SerialisableKeyFixed for ObjectIID {
        fn key_section_id(&self) -> u8 {
            EncodingSection::Data.id()
        }
    }

    #[derive(StructDeser, Copy, Clone, Debug, PartialEq, Eq)]
    pub struct ObjectID {
        bytes: [u8; OBJECT_ID_SIZE],
    }

    impl ObjectID {
        pub fn from(id: u64) -> ObjectID {
            debug_assert_eq!(mem::size_of_val(&id), OBJECT_ID_SIZE);
            ObjectID { bytes: id.to_be_bytes() }
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct AttributeIID {
        prefix: PrefixID,
        type_id: TypeID,
        id: AttributeID,
    }

    impl AttributeIID {

    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    struct AttributeID {
        bytes: [u8; ATTRIBUTE_ID_SIZE],
    }
}

mod connection {
    mod has_forward {
        // const PREFIX_HAS_FORWARD_SIZE: usize = ObjectIID::size() + INFIX_SIZE + PREFIX_SIZE;
        // const PREFIX_HAS_FORWARD_TYPE_SIZE: usize = ObjectIID::size() + INFIX_SIZE + PREFIX_SIZE + TypeID::size();
        //
        // #[repr(C, packed)]
        // #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        // struct HasForwardIID {
        //     owner: ObjectIID,
        //     infix: u8,
        //     attribute: AttributeIID,
        // }
        //
        // pub fn prefix_has_forward(owner: &ObjectIID) -> [u8; PREFIX_HAS_FORWARD_SIZE] {
        //     let mut array = [0 as u8; PREFIX_HAS_FORWARD_SIZE];
        //     array[0..ObjectIID::size()].copy_from_slice(owner.as_bytes());
        //     array[ObjectIID::size()..(ObjectIID::size() + INFIX_SIZE)].copy_from_slice(&Infix::HasForward.as_bytes());
        //     array
        // }
        //
        // pub fn prefix_has_forward_type(owner: &ObjectIID, type_: &TypeID) -> [u8; PREFIX_HAS_FORWARD_TYPE_SIZE] {
        //     let mut array = [0 as u8; PREFIX_HAS_FORWARD_TYPE_SIZE];
        //     array[0..ObjectIID::size()].copy_from_slice(owner.as_bytes());
        //     array[ObjectIID::size()..(ObjectIID::size() + INFIX_SIZE)].copy_from_slice(&Infix::HasForward.as_bytes());
        //     array[(ObjectIID::size() + INFIX_SIZE)..(ObjectIID::size() + INFIX_SIZE + TypeID::size())].copy_from_slice(type_.as_bytes());
        //     array
        // }
    }

    mod has_backward {
        //
        // #[repr(C, packed)]
        // #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        // struct HasBackwardIID {
        //     attribute: AttributeIID,
        //     infix: u8,
        //     owner: ObjectIID,
        // }
    }
}