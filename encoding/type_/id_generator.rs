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

use storage::Storage;
use crate::Prefix;

use crate::Prefix::{AttributeType, EntityType};
use crate::type_::type_encoding::concept::{TypeID, TypeIID};

pub struct TypeIIDGenerator {
    next_entity_id: AtomicU16,
    // TODO: how can we couple this to the variablised byte array size in TypeEncoding?
    next_attribute_id: AtomicU16, // TODO: how can we couple this to the variablised byte array size in TypeEncoding?
}

impl TypeIIDGenerator {
    pub fn new(storage: &Storage) -> TypeIIDGenerator {
        let next_entity: AtomicU16 = storage.get_prev(&EntityType.as_bytes_next()).map_or_else(
            || AtomicU16::new(0),
            |prev| {
                debug_assert_eq!(prev.len(), 2);
                let val = u16::from_be_bytes((&prev[0..2]).try_into().unwrap());
                AtomicU16::new(val)
            });
        let next_attribute: AtomicU16 = storage.get_prev(&AttributeType.as_bytes_next()).map_or_else(
            || AtomicU16::new(0),
            |prev| {
                debug_assert_eq!(prev.len(), 2);
                let val = u16::from_be_bytes((&prev[0..2]).try_into().unwrap());
                AtomicU16::new(val)
            });

        TypeIIDGenerator {
            next_entity_id: next_entity,
            next_attribute_id: next_attribute,
        }
    }

    pub fn take_entity_iid(&self) -> TypeIID {
        let next = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
        TypeIID::new(Prefix::Entity.as_id(), TypeID::new(next.to_be_bytes()))
    }

    pub fn take_attribute_iid(&self) -> TypeIID {
        let next = self.next_attribute_id.fetch_add(1, Ordering::Relaxed);
        TypeIID::new(Prefix::Attribute.as_id(), TypeID::new(next.to_be_bytes()))
    }
}

