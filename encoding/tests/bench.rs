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
use storage::key_value::StorageKey;

use encoding::{DeserialisableFixed, SerialisableKeyFixed};
use encoding::thing::id_generator::ThingIIDGenerator;
use encoding::thing::thing_encoding::concept::ObjectIID;
use encoding::type_::type_encoding::concept::TypeID;

fn id_generation(thing_iid_generator: Arc<ThingIIDGenerator>, type_id: TypeID) -> ObjectIID {
    thing_iid_generator.take_entity_iid(&type_id)
}

fn id_generation_serialisation(thing_iid_generator: Arc<ThingIIDGenerator>, type_id: TypeID) -> StorageKey {
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