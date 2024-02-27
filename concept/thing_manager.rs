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

use std::any::Any;
use std::iter::empty;
use std::rc::Rc;

use encoding::graph::thing::vertex::{AttributeVertex, ObjectVertex};
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::Keyable;
use encoding::layout::prefix::PrefixType;
use storage::key_value::StorageKey;
use storage::snapshot::snapshot::Snapshot;

use crate::Type;
use crate::type_manager::EntityType;

pub struct ThingManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    vertex_generator: &'txn ThingVertexGenerator,
}

impl<'txn, 'storage: 'txn> ThingManager<'txn, 'storage> {
    pub fn new(&self, snapshot: Rc<Snapshot<'storage>>, vertex_generator: &'txn ThingVertexGenerator) -> Self {
        ThingManager { snapshot: snapshot, vertex_generator: vertex_generator }
    }

    fn create_entity(&self, entity_type: EntityType) -> Entity {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let vertex = self.vertex_generator.take_entity_vertex(&entity_type.vertex().type_id());
            write_snapshot.put(vertex.as_storage_key().to_owned());
            return Entity::new(vertex);
        }
        panic!("Illegal state: create entity requires write snapshot")
    }

    fn get_entities(&self) -> () {
        let prefix = ObjectVertex::prefix_prefix(&PrefixType::Entity.prefix());
        self.snapshot.iterate_prefix(&prefix);
            ()
    }
}

struct Entity<'a> {
    vertex: ObjectVertex<'a>,
}

impl<'a> Entity<'a> {
    fn new(vertex: ObjectVertex<'a>) -> Self {
        Entity { vertex: vertex }
    }
}

struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
}

impl<'a> Attribute<'a> {
    fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex: vertex }
    }
}
