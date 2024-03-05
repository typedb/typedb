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

use std::sync::atomic::{AtomicU16, Ordering};

use storage::MVCCStorage;

use crate::graph::type_::vertex::{TypeID, TypeVertex};
use crate::Keyable;
use crate::layout::prefix::PrefixType;

// TODO: if we always scan for the next available TypeID, we automatically recycle deleted TypeIDs?
pub struct TypeVertexGenerator {
    next_entity: AtomicU16,
    next_attribute: AtomicU16,
}

impl TypeVertexGenerator {
    const U16_LENGTH: usize = std::mem::size_of::<u16>();

    pub fn new() -> TypeVertexGenerator {
        TypeVertexGenerator {
            next_entity: AtomicU16::new(0),
            next_attribute: AtomicU16::new(0),
        }
    }

    pub fn load(storage: &MVCCStorage) -> TypeVertexGenerator {
        let next_entity: AtomicU16 = storage.get_prev_raw(
            PrefixType::EntityType.next_prefix().as_storage_key().as_reference(),
            |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            },
        ).unwrap_or_else(|| AtomicU16::new(0));
        let next_attribute: AtomicU16 = storage.get_prev_raw(
            PrefixType::AttributeType.next_prefix().as_storage_key().as_reference(),
            |_, value| {
                debug_assert_eq!(value.len(), Self::U16_LENGTH);
                let array: [u8; Self::U16_LENGTH] = value[0..Self::U16_LENGTH].try_into().unwrap();
                let val = u16::from_be_bytes(array);
                AtomicU16::new(val)
            }).unwrap_or_else(|| AtomicU16::new(0));
        TypeVertexGenerator {
            next_entity: next_entity,
            next_attribute: next_attribute,
        }
    }

    pub fn take_entity_type_vertex(&self) -> TypeVertex {
        let next = TypeID::build(self.next_entity.fetch_add(1, Ordering::Relaxed));
        TypeVertex::build(&PrefixType::EntityType.prefix(), &next)
    }

    pub fn take_attribute_type_vertex(&self) -> TypeVertex {
        let next = TypeID::build(self.next_attribute.fetch_add(1, Ordering::Relaxed));
        TypeVertex::build(&PrefixType::AttributeType.prefix(), &next)
    }
}

