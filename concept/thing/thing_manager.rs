/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::rc::Rc;

use encoding::{Keyable, Prefixed};
use encoding::graph::thing::vertex::ObjectVertex;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::Typed;
use encoding::layout::prefix::PrefixType;
use storage::snapshot::snapshot::Snapshot;

use crate::thing::entity::{Entity, EntityIterator};
use crate::thing::ObjectAPI;
use crate::type_::entity_type::EntityType;
use crate::type_::TypeAPI;

pub struct ThingManager<'txn, 'storage: 'txn> {
    snapshot: Rc<Snapshot<'storage>>,
    vertex_generator: &'txn ThingVertexGenerator,
}

impl<'txn, 'storage: 'txn> ThingManager<'txn, 'storage> {
    pub fn new(snapshot: Rc<Snapshot<'storage>>, vertex_generator: &'txn ThingVertexGenerator) -> Self {
        ThingManager { snapshot: snapshot, vertex_generator: vertex_generator }
    }

    pub fn create_entity(&self, entity_type: &EntityType) -> Entity {
        if let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() {
            let vertex = self.vertex_generator.take_entity_vertex(&entity_type.vertex().type_id());
            write_snapshot.put(vertex.as_storage_key().to_owned_array());
            return Entity::new(vertex);
        }
        panic!("Illegal state: create entity requires write snapshot")
    }

    pub fn get_entities<'this>(&'this self) -> EntityIterator<'this, 1>  {
        let prefix = ObjectVertex::build_prefix_prefix(&PrefixType::VertexEntity.prefix_id());
        let snapshot_iterator = self.snapshot.iterate_prefix(prefix);
        EntityIterator::new(snapshot_iterator)
    }
}
