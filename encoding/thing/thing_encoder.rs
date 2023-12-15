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
 *
 */

use storage::Storage;

pub struct ThingEncoder {}

impl ThingEncoder {
    pub fn new(storage: &mut Storage) -> ThingEncoder {
        let _ = storage.create_section(entity::NAME, entity::PREFIX);
    }

    pub fn load(storage: &mut Storage) -> ThingEncoder {
        todo!()
    }
}

mod entity {
    pub(crate) const NAME: &str = "entity";
    pub(crate) const PREFIX: u8 = 0;

    struct EntityKey {
        prefix: u8,
        type_: EntityTypeID,
        id: EntityID,
        sequence_number: SequenceNumber, // zeroed out by default? Optional? reserved to avoid reallocating later
    }

    struct EntityID {
        bytes: [u8; 8], // u64?
    }
}

mod short_attribute {
    pub(crate) const NAME: &str = "short_attribute";
    pub(crate) const PREFIX: u8 = 10;

    struct ShortAttributeKey {
        prefix: u8,
        type_: AttributeTypeID,
        id: ShortAttributeID,
        sequence_number: SequenceNumber, // zeroed out by default? Optional?
    }

    struct ShortAttributeID {
        bytes: [u8; u8], // u64?
    }
}

mod has_forward {
    pub(crate) const NAME: &str = "has_forward";
    pub(crate) const PREFIX: u8 = 100;

    struct HasForwardKey {
        prefix: u8,
        from:
    }
}
