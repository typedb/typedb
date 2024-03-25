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

use std::rc::Rc;

use concept::{
    thing::{AttributeAPI, thing_manager::ThingManager, value::Value},
    type_::type_manager::TypeManager,
};
use durability::wal::WAL;
use encoding::{
    EncodingKeyspace,
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    value::{label::Label, value_type::ValueType},
};
use storage::{MVCCStorage, snapshot::snapshot::Snapshot};
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn thing_create_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));

        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label, false);

        let _person_1 = thing_manager.create_entity(&person_type);
        let _person_2 = thing_manager.create_entity(&person_type);
        let _person_3 = thing_manager.create_entity(&person_type);
        let _person_4 = thing_manager.create_entity(&person_type);
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
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let age_label = Label::build("age");
    let name_label = Label::build("name");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let age_type = type_manager.create_attribute_type(&age_label, false);
        age_type.set_value_type(&type_manager, ValueType::Long);
        let name_type = type_manager.create_attribute_type(&name_label, false);
        name_type.set_value_type(&type_manager, ValueType::String);

        let age_1 = thing_manager.create_attribute(&age_type, Value::Long(age_value)).unwrap();
        assert_eq!(age_1.value(&thing_manager), Value::Long(age_value));

        let name_1 = thing_manager
            .create_attribute(&name_type, Value::String(String::from(name_value).into_boxed_str()))
            .unwrap();
        assert_eq!(name_1.value(&thing_manager), Value::String(String::from(name_value).into_boxed_str()));
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

        let age_type = type_manager.get_attribute_type(&age_label).unwrap();
        let ages = thing_manager.get_attributes_in(age_type).collect_cloned();
        assert_eq!(ages.len(), 1);
        assert_eq!(ages.first().unwrap().value(&thing_manager), Value::Long(age_value));
    }
}
