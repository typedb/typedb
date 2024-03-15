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

use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use concept::thing::{AttributeAPI};
use concept::thing::value::Value;
use encoding::create_keyspaces;
use encoding::graph::thing::vertex_generator::ThingVertexGenerator;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::value::label::Label;
use storage::MVCCStorage;
use storage::snapshot::snapshot::Snapshot;
use test_utils::{create_tmp_dir, delete_dir, init_logging};

#[test]
fn thing_create_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    create_keyspaces(&mut storage);
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
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
        let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager);
        let entities = thing_manager.get_entities().collect_cloned();
        assert_eq!(entities.len(), 4);
    }

    delete_dir(storage_path)
}

#[test]
fn attribute_create() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::new(Rc::from("storage"), &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    create_keyspaces(&mut storage);
    TypeManager::initialise_types(&mut storage, &type_vertex_generator);

    let mut snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label, false);
        let age_label = Label::build("age");
        let age_type = type_manager.create_attribute_type(&age_label, false);

        let age_value: i64 = 10;
        let age_1 = thing_manager.create_attribute(&age_type, Value::Long(age_value)).unwrap();
        assert_eq!(age_1.value(&thing_manager), Value::Long(age_value));

    }
    if let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() {
        write_snapshot.commit().unwrap();
    } else {
        assert!(false);
    }

    {
        let snapshot: Rc<Snapshot<'_>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager);
        let entities = thing_manager.get_entities().collect_cloned();

        // TODO: get attribute and check value
    }

    delete_dir(storage_path)
}
