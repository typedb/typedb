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


use std::sync::atomic::AtomicU64;

use crate::graph::thing::thing_encoding::concept::ObjectIID;
use crate::graph::type_::vertex::{TypeID, TypeIdUInt};

pub struct ThingVertexGenerator {
    entity_ids: Box<[AtomicU64]>,
    relation_ids: Box<[AtomicU64]>,
    attribute_ids: Box<[AtomicU64]>,
}

impl ThingVertexGenerator {
    pub fn new() -> ThingVertexGenerator {
        ThingVertexGenerator {
            entity_ids: (0..TypeIdUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
            relation_ids: (0..TypeIdUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
            attribute_ids: (0..TypeIdUInt::MAX as usize)
                .map(|_| AtomicU64::new(0)).collect::<Vec<AtomicU64>>().into_boxed_slice(),
        }
    }

    pub fn load() -> ThingVertexGenerator {
        todo!()
    }

    pub fn take_entity_iid(&self, type_id: &TypeID) -> ObjectIID<'_> {
        // let index = type_id.as_u16() as usize;
        // let entity_id = self.entity_ids[index].fetch_add(1, Ordering::Relaxed);
        // ObjectIID::new(PrefixType::Entity.prefix(), *type_id, ObjectID::from(entity_id))
        todo!()
    }
}