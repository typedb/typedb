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

use encoding::graph::thing::vertex::concept::ObjectVertex;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex::TypeID;


fn vertex_generation(thing_iid_generator: Arc<ThingVertexGenerator>, type_id: &TypeID<'_>) -> ObjectVertex<'static> {
    thing_iid_generator.take_entity_vertex(type_id)
}

fn criterion_benchmark(c: &mut Criterion) {
    let vertex_generator = Arc::new(ThingVertexGenerator::new());
    let type_id = TypeID::build(0);

    c.bench_function("vertex_generation", |b| b.iter(|| {
        vertex_generation(vertex_generator.clone(), &type_id)
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);