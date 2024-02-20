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
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use encoding::graph::thing::id_generator::ThingVertexGenerator;
use encoding::graph::thing::thing_encoding::concept::ObjectIID;
use encoding::graph::type_::vertex::TypeVertex;
use storage::key_value::StorageKeyReference;

// TODO: this benchmark needs to be restored

fn id_generation(thing_iid_generator: Arc<ThingVertexGenerator>, type_vertex: TypeVertex) -> ObjectIID {
    thing_iid_generator.take_entity_iid(&type_vertex.type_id())
}

fn id_generation_serialisation(thing_iid_generator: Arc<ThingIIDGenerator>, type_id: TypeID) -> StorageKeyReference {
    let iid = thing_iid_generator.take_entity_iid(&type_id);
    iid.serialise_to_key()
}

fn id_generation_serialisation_deserialisation(thing_iid_generator: Arc<ThingIIDGenerator>, type_id: TypeID) -> ObjectIID {
    let iid = thing_iid_generator.take_entity_iid(&type_id);
    let key = iid.serialise_to_key();
    ObjectIID::deserialise_from(key.bytes())
}

fn criterion_benchmark(c: &mut Criterion) {

    let iid_generator = Arc::new(ThingIIDGenerator::new());
    let type_id = TypeID::from(0);

    c.bench_function("id_generation", |b| b.iter(|| {
        id_generation(iid_generator.clone(), type_id)
    }));

    c.bench_function("id_generation_serialisation", |b| b.iter(|| {
        id_generation_serialisation(iid_generator.clone(), type_id)
    }));

    c.bench_function("id_generation_serialisation_deserialisation", |b| b.iter(|| {
        id_generation_serialisation_deserialisation(iid_generator.clone(), type_id)
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);