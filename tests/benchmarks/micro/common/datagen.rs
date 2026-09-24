/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{any::Any, borrow::Cow};

use answer::Thing;
use concept::{
    thing::{ThingAPI, entity::Entity, relation::Relation},
    type_::{TypeAPI, entity_type::EntityType, relation_type::RelationType},
};
use encoding::{
    graph::{
        Typed,
        thing::vertex_object::{ObjectID, ObjectVertex},
        type_::vertex::{TypeID, TypeVertexEncoding},
    },
    value::value::Value,
};
use query::given_rows::GivenRowEntry;
use rand::{Rng, SeedableRng, prelude::SmallRng, thread_rng};

pub struct RandomDataGen {
    rng: SmallRng,
}

impl RandomDataGen {
    pub fn new() -> Self {
        let seed = thread_rng().r#gen();
        Self { rng: SmallRng::seed_from_u64(seed) }
    }

    pub fn string(&mut self, len: usize) -> String {
        (0..len).map(|_| self.rng.sample(rand::distributions::Alphanumeric) as char).collect()
    }

    pub fn integer_in(&mut self, min: i64, max: i64) -> i64 {
        self.rng.gen_range(min..=max)
    }

    pub fn integer(&mut self) -> i64 {
        self.integer_in(i64::MIN, i64::MAX)
    }

    pub fn entry_string(&mut self, len: usize) -> GivenRowEntry {
        GivenRowEntry::Value(Value::String(Cow::Owned(self.string(len))))
    }

    pub fn entry_integer_in(&mut self, min: i64, max: i64) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(self.integer_in(min, max)))
    }

    pub fn entry_integer(&mut self) -> GivenRowEntry {
        GivenRowEntry::Value(Value::Integer(self.integer()))
    }

    pub fn entry_entity_in(&mut self, entity_type: EntityType, min: u64, max: u64) -> GivenRowEntry {
        self.entry_entity_raw_in(entity_type.vertex().type_id_(), min, max)
    }

    pub fn entry_relation_in(&mut self, relation_type: RelationType, min: u64, max: u64) -> GivenRowEntry {
        self.entry_entity_raw_in(relation_type.vertex().type_id_(), min, max)
    }

    pub fn entry_entity_raw_in(&mut self, type_id: TypeID, min: u64, max: u64) -> GivenRowEntry {
        let instance_id = self.rng.gen_range(min..=max);
        let vertex = ObjectVertex::build_entity(type_id, ObjectID::new(instance_id));
        GivenRowEntry::Thing(Thing::Entity(Entity::new(vertex)))
    }

    pub fn entry_relation_raw_in(&mut self, type_id: TypeID, min: u64, max: u64) -> GivenRowEntry {
        let instance_id = self.rng.gen_range(min..=max);
        let vertex = ObjectVertex::build_relation(type_id, ObjectID::new(instance_id));
        GivenRowEntry::Thing(Thing::Relation(Relation::new(vertex)))
    }
}
