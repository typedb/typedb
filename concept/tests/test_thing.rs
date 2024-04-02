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
    rc::Rc,
    sync::{Arc, OnceLock},
};

use concept::{
    thing::{thing_manager::ThingManager, value::Value},
    type_::{type_cache::TypeCache, type_manager::TypeManager, OwnerAPI},
};
use durability::wal::WAL;
use encoding::{
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    value::{label::Label, value_type::ValueType},
    EncodingKeyspace,
};
use storage::{snapshot::Snapshot, MVCCStorage};
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn thing_create_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator).unwrap();

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));

        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label, false).unwrap();

        let _person_1 = thing_manager.create_entity(person_type.clone());
        let _person_2 = thing_manager.create_entity(person_type.clone());
        let _person_3 = thing_manager.create_entity(person_type.clone());
        let _person_4 = thing_manager.create_entity(person_type.clone());
    }
    if let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() {
        write_snapshot.commit().unwrap();
    } else {
        unreachable!();
    }

    {
        let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager);
        let entities = thing_manager.get_entities().collect_cloned();
        assert_eq!(entities.len(), 4);
    }
}

#[test]
fn attribute_create() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator).unwrap();

    let age_label = Label::build("age");
    let name_label = Label::build("name");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let age_type = type_manager.create_attribute_type(&age_label, false).unwrap();
        age_type.set_value_type(&type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(&name_label, false).unwrap();
        name_type.set_value_type(&type_manager, ValueType::String).unwrap();

        let age_1 = thing_manager.create_attribute(age_type.clone(), Value::Long(age_value)).unwrap();
        assert_eq!(age_1.value(&thing_manager).unwrap(), Value::Long(age_value));

        let name_1 = thing_manager
            .create_attribute(name_type.clone(), Value::String(String::from(name_value).into_boxed_str()))
            .unwrap();
        assert_eq!(name_1.value(&thing_manager).unwrap(), Value::String(String::from(name_value).into_boxed_str()));
    }
    let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { unreachable!() };
    write_snapshot.commit().unwrap();

    {
        let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let attributes = thing_manager.get_attributes().collect_cloned();
        assert_eq!(attributes.len(), 2);

        let age_type = type_manager.get_attribute_type(&age_label).unwrap().unwrap();
        let ages = thing_manager.get_attributes_in(age_type).unwrap().collect_cloned();
        assert_eq!(ages.len(), 1);
        assert_eq!(ages.first().unwrap().value(&thing_manager).unwrap(), Value::Long(age_value));
    }
}

#[test]
fn has() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator).unwrap();

    let age_label = Label::build("age");
    let name_label = Label::build("name");
    let person_label = Label::build("person");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let age_type = type_manager.create_attribute_type(&age_label, false).unwrap();
        age_type.set_value_type(&type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(&name_label, false).unwrap();
        name_type.set_value_type(&type_manager, ValueType::String).unwrap();

        let person_type = type_manager.create_entity_type(&person_label, false).unwrap();

        let person_1 = thing_manager.create_entity(person_type.clone()).unwrap();
        let age_1 = thing_manager.create_attribute(age_type.clone(), Value::Long(age_value)).unwrap();
        let name_1 = thing_manager
            .create_attribute(name_type.clone(), Value::String(String::from(name_value).into_boxed_str()))
            .unwrap();

        person_1.set_has(&thing_manager, &age_1).unwrap();
        person_1.set_has(&thing_manager, &name_1).unwrap();

        let retrieved_attributes = person_1.get_has(&thing_manager).collect_cloned();
        assert_eq!(retrieved_attributes.len(), 2);
    }

    let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { unreachable!() };
    write_snapshot.commit().unwrap();

    {
        let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let attributes = thing_manager.get_attributes().collect_cloned();
        assert_eq!(attributes.len(), 2);

        let people = thing_manager.get_entities().collect_cloned();
        let person_1 = people.first().unwrap();
        let retrieved_attributes = person_1.get_has(&thing_manager).collect_cloned();
        assert_eq!(retrieved_attributes.len(), 2);
    }
}

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
        let age = thing_manager.create_attribute(age_type, Value::Long(100)).unwrap();
        let name =
            thing_manager.create_attribute(name_type, Value::String(String::from("abc").into_boxed_str())).unwrap();
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

#[test]
fn criterion_benchmark() {
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
    write_entity_attributes(&storage, &type_vertex_generator, &thing_vertex_generator, schema_cache.clone());
}
