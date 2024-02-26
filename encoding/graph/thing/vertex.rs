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
    use std::ops::Range;

    use bytes::byte_array::ByteArray;
    use bytes::byte_array_or_ref::ByteArrayOrRef;
    use bytes::byte_reference::ByteReference;
    use storage::keyspace::keyspace::KeyspaceId;
    use storage::snapshot::buffer::BUFFER_INLINE_KEY;

    use crate::{AsBytes, EncodingKeyspace, Keyable};
    use crate::graph::type_::vertex::TypeID;
    use crate::layout::prefix::PrefixID;

    const ATTRIBUTE_ID_SIZE: usize = 12;

    #[derive(Debug, PartialEq, Eq)]
    pub struct ObjectVertex<'a> {
        bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>,
    }

    impl<'a> ObjectVertex<'a> {
        const LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ObjectID::LENGTH;

        pub fn new(bytes: ByteArrayOrRef<'a, BUFFER_INLINE_KEY>) -> ObjectVertex<'a> {
            debug_assert_eq!(bytes.length(), Self::LENGTH);
            ObjectVertex { bytes: bytes }
        }

        pub fn build(prefix: &PrefixID<'_>, type_id: &TypeID<'_>, object_id: ObjectID) -> Self {
            let mut array = ByteArray::zeros(Self::LENGTH);
            array.bytes_mut()[Self::range_prefix()].copy_from_slice(prefix.bytes().bytes());
            array.bytes_mut()[Self::range_type_id()].copy_from_slice(type_id.bytes().bytes());
            array.bytes_mut()[Self::range_object_id()].copy_from_slice(object_id.bytes().bytes());
            ObjectVertex { bytes: ByteArrayOrRef::Array(array) }
        }

        pub fn prefix(&'a self) -> PrefixID<'a> {
            PrefixID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_prefix()])))
        }

        pub fn type_id(&'a self) -> TypeID<'a> {
            TypeID::new(ByteArrayOrRef::Reference(ByteReference::new(&self.bytes.bytes()[Self::range_type_id()])))
        }

        const fn range_prefix() -> Range<usize> {
            0..PrefixID::LENGTH
        }

        const fn range_type_id() -> Range<usize> {
            Self::range_prefix().end..Self::range_prefix().end + TypeID::LENGTH
        }

        const fn range_object_id() -> Range<usize> {
            Self::range_type_id().end..Self::range_type_id().end + ObjectID::LENGTH
        }
    }

    impl<'a> AsBytes<'a, BUFFER_INLINE_KEY> for ObjectVertex<'a> {
        fn bytes(&'a self) -> ByteReference<'a> {
            self.bytes.as_ref()
        }

        fn into_bytes(self) -> ByteArrayOrRef<'a, BUFFER_INLINE_KEY> {
            self.bytes
        }
    }

    impl<'a> Keyable<'a, BUFFER_INLINE_KEY> for ObjectVertex<'a> {
        fn keyspace_id(&self) -> KeyspaceId {
            // TODO: partition
            EncodingKeyspace::Data.id()
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct ObjectID<'a> {
        bytes: ByteArrayOrRef<'a, { ObjectID::LENGTH }>,
    }

    impl<'a> ObjectID<'a> {
        const LENGTH: usize = 8;

        pub fn build(id: u64) -> Self {
            debug_assert_eq!(mem::size_of_val(&id), Self::LENGTH);
            ObjectID { bytes: ByteArrayOrRef::Array(ByteArray::inline(id.to_be_bytes(), Self::LENGTH)) }
        }

        pub fn bytes(&'a self) -> ByteReference<'a> {
            self.bytes.as_ref()
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct AttributeIID<'a> {
        prefix: PrefixID<'a>,
        type_id: TypeID<'a>,
        id: AttributeID,
    }

    impl<'a> AttributeIID<'a> {}

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