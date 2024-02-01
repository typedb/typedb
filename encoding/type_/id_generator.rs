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
use crate::prefix::Prefix;

use crate::prefix::Prefix::{AttributeType, EntityType};
use crate::type_::type_encoding::concept::{TypeID, TypeIID};
use crate::SerialisableKeyFixed;

// TODO: if we always scan for the next available TypeID, we automatically recycle deleted TypeIDs?
pub struct TypeIIDGenerator {
    next_entity_id: AtomicU16, // TODO: how can we couple this to the variablised byte array size in TypeEncoding?
    next_attribute_id: AtomicU16, // TODO: how can we couple this to the variablised byte array size in TypeEncoding?
}

impl TypeIIDGenerator {
    pub fn new() -> TypeIIDGenerator {
        TypeIIDGenerator {
            next_entity_id: AtomicU16::new(0),
            next_attribute_id: AtomicU16::new(0),
        }
    }

    pub fn load(storage: &MVCCStorage) -> TypeIIDGenerator {
        let next_entity: AtomicU16 = storage.get_prev_direct(&EntityType.next_prefix_id().serialise_to_key())
            .map_or_else(
                || AtomicU16::new(0),
                |prev| {
                    debug_assert_eq!(prev.bytes().len(), 2);
                    let val = u16::from_be_bytes((&prev.bytes()[0..2]).try_into().unwrap());
                    AtomicU16::new(val)
                });
        let next_attribute: AtomicU16 = storage.get_prev_direct(&AttributeType.next_prefix_id().serialise_to_key())
            .map_or_else(
                || AtomicU16::new(0),
                |prev| {
                    debug_assert_eq!(prev.bytes().len(), 2);
                    let val = u16::from_be_bytes((&prev.bytes()[0..2]).try_into().unwrap());
                    AtomicU16::new(val)
                });

        TypeIIDGenerator {
            next_entity_id: next_entity,
            next_attribute_id: next_attribute,
        }
    }

    pub fn take_entity_type_iid(&self) -> TypeIID {
        let next = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
        TypeIID::new(Prefix::EntityType.type_id(), TypeID::from(next))
    }

    pub fn take_attribute_type_iid(&self) -> TypeIID {
        let next = self.next_attribute_id.fetch_add(1, Ordering::Relaxed);
        TypeIID::new(Prefix::AttributeType.type_id(), TypeID::from(next))
    }
}

