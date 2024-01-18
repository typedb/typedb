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

use std::iter::empty;

use encoding::prefix::Prefix;
use encoding::thing::thing_encoding::concept::{AttributeIID, ObjectIID};
use storage::snapshot::Snapshot;
use crate::type_manager::EntityType;

pub struct ThingManager<'txn, 'storage: 'txn> {
    snapshot: &'txn Snapshot<'storage>,
}

impl<'txn, 'storage: 'txn> ThingManager<'txn, 'storage> {

    pub fn new(&self, snapshot: &'txn Snapshot<'storage>) -> ThingManager<'txn, 'storage> {
        ThingManager {
            snapshot: snapshot
        }
    }

    fn create_entity(&self, entity_type: EntityType) -> Entity {
        if let Snapshot::Write(write_snapshot) = self.snapshot {
            // create ID
            // create IID
            // create and return Entity
        }
        panic!("Illegal state: create entity requires write snapshot")
    }

    fn get_entities(&self) -> impl Iterator<Item=Entity> {
        let prefix = Prefix::Entity.as_bytes();
        // self.snapshot.iterate_prefix(prefix).map(|(key, value)| Entity::new(ThingEncoder::decideThingIIDSmall))
        empty()
    }
}


struct Entity {
    iid: ObjectIID,
}

struct Attribute {
    iid: AttributeIID,
}
