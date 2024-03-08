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

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};

use encoding::graph::thing::vertex::ObjectVertex;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex::TypeID;
use encoding::Keyable;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

fn vertex_generation(thing_iid_generator: Arc<ThingVertexGenerator>, type_id: &TypeID<'_>) -> ObjectVertex<'static> {
    thing_iid_generator.take_entity_vertex(type_id)
}

fn vertex_generation_to_key(thing_iid_generator: Arc<ThingVertexGenerator>, type_id: &TypeID<'_>) -> StorageKey<'static, { BUFFER_KEY_INLINE }> {
    thing_iid_generator.take_entity_vertex(type_id).into_storage_key()
}

fn criterion_benchmark(c: &mut Criterion) {
    let vertex_generator = Arc::new(ThingVertexGenerator::new());
    let type_id = TypeID::build(0);

    c.bench_function("vertex_generation", |b| b.iter(|| {
        vertex_generation(vertex_generator.clone(), &type_id)
    }));
    c.bench_function("vertex_generation_to_storage_key", |b| b.iter(|| {
        vertex_generation_to_key(vertex_generator.clone(), &type_id)
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);