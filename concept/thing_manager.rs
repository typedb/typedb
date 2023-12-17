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

use std::iter::empty;

use encoding::thing::thing_encoding::concept::{ThingIID, ThingIIDSmall};
use encoding::{Prefix};
use storage::key::{Key, Keyable};
use storage::snapshot::Snapshot;

struct ThingManager<'storage> {
    snapshot: &'storage Snapshot<'storage>,
}

impl<'storage> ThingManager<'storage>{

    fn create_entity(&self) -> Entity {
        todo!()
    }

    fn get_entities(&self) -> impl Iterator<Item=Entity> {
        let prefix = Prefix::ENTITY.bytes();
        self.snapshot.iterate_prefix(prefix).map(|(key, value)| Entity::new(ThingEncoder::decideThingIIDSmall))
        empty()
    }
}

trait ThingRead {

    fn thing_manager<'storage>(&'storage self) -> &'storage ThingManager;
}

trait ObjectRead: ThingRead {

    fn get_iid(&self) -> ThingIIDSmall;

    fn get_has(&self) {
        // TODO: when do we deal with key versions - in the Snapshot?
        // self.snapshot.iterate_prefix(ThingEncoder::encodeHasForward(self.get_iid())
        todo!()
    }
}

trait AttributeRead {

    fn get_iid(&self) -> ThingIID;
}

struct Entity {
    iid: ThingIIDSmall,
}

struct Attribute {
    iid: ThingIID,
}