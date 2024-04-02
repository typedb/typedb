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

#![deny(unused_must_use)]

use std::{
    ffi::c_int,
    fs::File,
    path::Path,
    rc::Rc,
    sync::{Arc, OnceLock},
};

use concept::{
    thing::{thing_manager::ThingManager, value::Value},
    type_::{type_cache::TypeCache, type_manager::TypeManager, OwnerAPI},
};
use criterion::{criterion_group, criterion_main, profiler::Profiler, Criterion};
use durability::wal::WAL;
use encoding::{
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    value::{label::Label, value_type::ValueType},
    EncodingKeyspace,
};
use pprof::ProfilerGuard;
use rand::distributions::{Alphanumeric, DistString};
use storage::{snapshot::Snapshot, MVCCStorage};
use test_utils::{create_tmp_dir, init_logging};

static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();
static PERSON_LABEL: OnceLock<Label> = OnceLock::new();

fn write_entity_attributes(
    storage: &MVCCStorage<WAL>,
    type_vertex_generator: &TypeVertexGenerator,
    thing_vertex_generator: &ThingVertexGenerator,
    schema_cache: Arc<TypeCache>,
) {
    let snapshot = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), type_vertex_generator, Some(schema_cache)));
        let thing_manager = ThingManager::new(snapshot.clone(), thing_vertex_generator, type_manager.clone());

        let person_type = type_manager.get_entity_type(PERSON_LABEL.get().unwrap()).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(AGE_LABEL.get().unwrap()).unwrap().unwrap();
        let name_type = type_manager.get_attribute_type(NAME_LABEL.get().unwrap()).unwrap().unwrap();
        let person = thing_manager.create_entity(person_type).unwrap();

        let random_long: i64 = rand::random();
        let length: u8 = rand::random();
        let random_string: String = Alphanumeric.sample_string(&mut rand::thread_rng(), length as usize);

        let age = thing_manager.create_attribute(age_type, Value::Long(random_long)).unwrap();
        let name = thing_manager.create_attribute(name_type, Value::String(random_string.into_boxed_str())).unwrap();
        person.set_has(&thing_manager, &age).unwrap();
        person.set_has(&thing_manager, &name).unwrap();
    }

    let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { unreachable!() };
    write_snapshot.commit().unwrap();
}

fn create_schema(storage: &MVCCStorage<WAL>, type_vertex_generator: &TypeVertexGenerator) {
    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), type_vertex_generator, None));
        let age_type = type_manager.create_attribute_type(AGE_LABEL.get().unwrap(), false).unwrap();
        age_type.set_value_type(&type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(NAME_LABEL.get().unwrap(), false).unwrap();
        name_type.set_value_type(&type_manager, ValueType::String).unwrap();
        let person_type = type_manager.create_entity_type(PERSON_LABEL.get().unwrap(), false).unwrap();
        person_type.set_owns(&type_manager, age_type).unwrap();
        person_type.set_owns(&type_manager, name_type).unwrap();
    }
    let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { unreachable!() };
    write_snapshot.commit().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    AGE_LABEL.set(Label::build("age")).unwrap();
    NAME_LABEL.set(Label::build("name")).unwrap();
    PERSON_LABEL.set(Label::build("person")).unwrap();

    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    let thing_vertex_generator = ThingVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator).unwrap();

    create_schema(&storage, &type_vertex_generator);
    let schema_cache = Arc::new(TypeCache::new(&storage, storage.read_watermark()).unwrap());

    let mut group = c.benchmark_group("test writes");
    group.bench_function("thing_write", |b| {
        b.iter(|| {
            write_entity_attributes(&storage, &type_vertex_generator, &thing_vertex_generator, schema_cache.clone())
        });
    });
}

pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        Self { frequency, active_profiler: None }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(&flamegraph_path).expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            profiler.report().build().unwrap().flamegraph(flamegraph_file).expect("Error writing flamegraph");
        }
    }
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(1000))
}

criterion_group!(
    name = benches;
    config= profiled();
    targets = criterion_benchmark
);

criterion_main!(benches);

// TODO: disable profiling when running on mac, since pprof seems to crash
// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);
