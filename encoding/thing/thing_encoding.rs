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

use storage::{Section, Storage};

pub struct ThingEncoder {}

impl ThingEncoder {
    pub fn new(storage: &mut Storage) -> ThingEncoder {
        let options = Section::new_options();
        let _ = storage.create_section("entity", 0, &options);
        let _ = storage.create_section("attribute", 10, &options);
        let _ = storage.create_section("has_forward", 100, &options);
        let _ = storage.create_section("has_backward", 101, &options);
        todo!()
    }

    pub fn load(storage: &mut Storage) -> ThingEncoder {
        todo!()
    }
}

pub mod concept {
    use std::mem::transmute;
    use wal::SequenceNumber;

    use crate::type_::type_encoding::concept::TypeID;

    const OBJECT_ID_SIZE: usize = 8;
    const ATTRIBUTE_ID_SIZE: usize = 12;

    // TODO: Sequence number should be inserted at the storage layer, and not be visible here

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    struct ObjectIIDSequenced {
        iid: ObjectIID,
        sequence_number: SequenceNumber,
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct ObjectIID {
        pub(crate) prefix: u8,
        pub(crate) type_id: TypeID,
        pub(crate) id: ObjectID,
    }

    impl ObjectIID {
        pub(crate) const fn size() -> usize {
            std::mem::size_of::<Self>()
        }

        pub(crate) fn as_bytes(&self) -> &[u8; ObjectIID::size()] {
            unsafe {
                transmute(self)
            }
        }
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    struct ObjectID {
        bytes: [u8; OBJECT_ID_SIZE],
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct AttributeIIDSequenced {
        iid: AttributeIID,
        sequence_number: SequenceNumber,
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct AttributeIID {
        prefix: u8,
        type_id: TypeID,
        id: AttributeID,
    }

    impl AttributeIID {
        const fn size() -> usize {
            std::mem::size_of::<Self>()
        }

        fn as_bytes(&self) -> &[u8; ObjectIID::size()] {
            unsafe {
                transmute(self)
            }
        }
    }

    #[repr(C, packed)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    struct AttributeID {
        bytes: [u8; ATTRIBUTE_ID_SIZE],
    }
}

mod connection {
    mod has_forward {
        use wal::SequenceNumber;
        use crate::{Infix, INFIX_SIZE, PREFIX_SIZE};
        use crate::thing::thing_encoding::concept::{ObjectIID, AttributeIID};
        use crate::type_::type_encoding::concept::TypeID;

        const PREFIX_HAS_FORWARD_SIZE: usize = ObjectIID::size() + INFIX_SIZE + PREFIX_SIZE;
        const PREFIX_HAS_FORWARD_TYPE_SIZE: usize = ObjectIID::size() + INFIX_SIZE + PREFIX_SIZE + TypeID::size();
        //
        // #[repr(C, packed)]
        // #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        // struct HasForwardIIDSequenced {
        //     id: HasForwardIID,
        //     sequence_number: SequenceNumber,
        // }
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
        use wal::SequenceNumber;
        use crate::thing::thing_encoding::concept::{ObjectIID, AttributeIID};
        //
        // #[repr(C, packed)]
        // #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        // struct HasBackwardIIDSequenced {
        //     id: HasBackwardIID,
        //     sequence_number: SequenceNumber,
        // }
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