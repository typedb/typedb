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

use criterion::{criterion_group, criterion_main, Criterion};
use durability::wal::WAL;
use encoding::{
    graph::{
        thing::{vertex_generator::ThingVertexGenerator, vertex_object::ObjectVertex},
        type_::vertex::TypeID,
    },
    EncodingKeyspace, Keyable,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, snapshot::snapshot::WriteSnapshot, MVCCStorage};
use test_utils::{create_tmp_dir, init_logging};

fn vertex_generation<D>(
    thing_vertex_generator: Arc<ThingVertexGenerator>,
    type_id: TypeID,
    write_snapshot: &WriteSnapshot<'_, D>,
) -> ObjectVertex<'static> {
    thing_vertex_generator.create_entity(type_id, write_snapshot)
}

fn vertex_generation_to_key<D>(
    thing_vertex_generator: Arc<ThingVertexGenerator>,
    type_id: TypeID,
    write_snapshot: &WriteSnapshot<'_, D>,
) -> StorageKey<'static, { BUFFER_KEY_INLINE }> {
    thing_vertex_generator.create_entity(type_id, write_snapshot).into_storage_key()
}

fn criterion_benchmark(c: &mut Criterion) {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();

    let type_id = TypeID::build(0);
    let vertex_generator = Arc::new(ThingVertexGenerator::new());

    let snapshot = storage.open_snapshot_write();
    c.bench_function("vertex_generation", |b| {
        b.iter(|| vertex_generation(vertex_generator.clone(), type_id, &snapshot))
    });

    let snapshot = storage.open_snapshot_write();
    c.bench_function("vertex_generation_to_storage_key", |b| {
        b.iter(|| vertex_generation_to_key(vertex_generator.clone(), type_id, &snapshot))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
