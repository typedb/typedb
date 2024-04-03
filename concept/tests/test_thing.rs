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

#[test]
fn role_player_no_duplicates() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::initialise_types(&mut storage, &type_vertex_generator).unwrap();

    let employment_label = Label::build("employment");
    let employee_role = "employee";
    let employer_role = "employer";
    let person_label = Label::build("person");
    let company_label = Label::build("company");

    let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None).unwrap());
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let employment_type = type_manager.create_relation_type(&employment_label, false).unwrap();
        let employee_type = employment_type.create_relates(&type_manager, employee_role).unwrap().role();
        let employer_type = employment_type.create_relates(&type_manager, employer_role).unwrap().role();
        let person_type = type_manager.create_entity_type(&person_label, false).unwrap();
        let company_type = type_manager.create_entity_type(&company_label, false).unwrap();
        person_type.set_plays(&type_manager, employee_type.clone()).unwrap();
        company_type.set_plays(&type_manager, employee_type.clone()).unwrap();

        let person_1 = thing_manager.create_entity(person_type.clone()).unwrap();
        let company_1 = thing_manager.create_entity(company_type.clone()).unwrap();
        let company_2 = thing_manager.create_entity(company_type.clone()).unwrap();
        let company_3 = thing_manager.create_entity(company_type.clone()).unwrap();

        let employment_1 = thing_manager.create_relation(employment_type.clone()).unwrap();
        employment_1.add_player(&thing_manager, employee_type.clone(), Object::Entity(person_1.as_reference()));
        employment_1.add_player(&thing_manager, employer_type.clone(), Object::Entity(company_1.as_reference()));

        let employment_2 = thing_manager.create_relation(employment_type.clone()).unwrap();
        employment_2.add_player(&thing_manager, employee_type.clone(), Object::Entity(person_1.as_reference()));
        employment_2.add_player(&thing_manager, employer_type.clone(), Object::Entity(company_2.as_reference()));
        employment_2.add_player(&thing_manager, employer_type.clone(), Object::Entity(company_3.as_reference()));

        assert_eq!(employment_1.get_players(&thing_manager).count(), 2);
        assert_eq!(employment_2.get_players(&thing_manager).count(), 3);

        assert_eq!(person_1.get_relations(&thing_manager).count(), 2);
        assert_eq!(company_1.get_relations(&thing_manager).count(), 1);
        assert_eq!(company_2.get_relations(&thing_manager).count(), 1);
        assert_eq!(company_3.get_relations(&thing_manager).count(), 1);
    }

    let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { unreachable!() };
    write_snapshot.commit().unwrap();
    {
        let snapshot: Rc<Snapshot<'_, WAL>> = Rc::new(Snapshot::Read(storage.open_snapshot_read()));
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let entities = thing_manager.get_entities().collect_cloned();
        assert_eq!(entities.len(), 4);
        let relations = thing_manager.get_relations().collect_cloned();
        assert_eq!(relations.len(), 2);

        let players_0 = relations[0].get_players(&thing_manager).count();
        if players_0 == 2 {
            assert_eq!(relations[1].get_players(&thing_manager).count(), 3);
        } else {
            assert_eq!(relations[1].get_players(&thing_manager).count(), 2);
        }

        let person_1= entities.iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&person_label).unwrap().unwrap())
            .unwrap();

        assert_eq!(person_1.get_relations(&thing_manager).count(), 2);
        assert_eq!(person_1.get_indexed_players(&thing_manager).count(), 3);
    }
}
