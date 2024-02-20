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


use bytes::byte_array::ByteArray;
use storage::key_value::{StorageKeyArray};
use storage::keyspace::keyspace::KeyspaceId;
use storage::snapshot::buffer::{BUFFER_INLINE_KEY, BUFFER_INLINE_VALUE};

pub trait Serialisable {
    fn serialised_size(&self) -> usize;

    fn serialise_into(&self, array: &mut [u8]);
}

pub trait DeserialisableFixed {
    fn serialised_size() -> usize;

    fn deserialise_from(array: &[u8]) -> Self;
}

pub trait DeserialisableDynamic {
    fn deserialise_from(array: Box<[u8]>) -> Self;
}
//
// impl<T: IntoBytes> Serialisable for T {
//     fn serialised_size(&self) -> usize {
//         Self::BYTE_LEN
//     }
//
//     fn serialise_into(&self, array: &mut [u8]) {
//         debug_assert_eq!(array.len(), self.serialised_size());
//         self.into_bytes(array)
//     }
// }
//
// impl<T: FromBytes> DeserialisableFixed for T {
//     fn serialised_size() -> usize {
//         Self::BYTE_LEN
//     }
//
//     fn deserialise_from(array: &[u8]) -> Self {
//         debug_assert_eq!(array.len(), Self::serialised_size());
//         T::from_bytes(array)
//     }
// }

pub trait SerialisableKeyFixed: Serialisable {
    fn keyspace_id(&self) -> KeyspaceId;

    fn serialise_to_key(&self) -> StorageKeyArray<BUFFER_INLINE_KEY> {
        let mut array = ByteArray::zeros(self.serialised_size());
        self.serialise_into(array.bytes_mut());
        StorageKeyArray::new(self.keyspace_id(), array)
    }
}

pub trait SerialisableKeyDynamic: Serialisable {
    fn keyspace_id(&self) -> KeyspaceId;

    fn serialise_to_key(&self) -> StorageKeyArray<BUFFER_INLINE_KEY> {
        debug_assert!(self.serialised_size() < BUFFER_INLINE_KEY);
        let mut array = ByteArray::zeros(self.serialised_size());
        self.serialise_into(array.bytes_mut());
        StorageKeyArray::new(self.keyspace_id(), array)
    }
}
//
// pub trait SerialisableValue: Serialisable {
//     fn serialise_to_value(&self) -> StorageValueArray<BUFFER_INLINE_VALUE> {
//         let mut array = ByteArray::zeros(self.serialised_size());
//         self.serialise_into(array.bytes_mut());
//         StorageValueArray::new(array)
//     }
// }
//
// // anything serialisable can be serialised as a value
// impl<T: Serialisable> SerialisableValue for T {}
