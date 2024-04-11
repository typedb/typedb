/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::borrow::Cow;
use std::rc::Rc;

use concept::{
    thing::{thing_manager::ThingManager, value::Value},
    type_::type_manager::TypeManager,
};
use concept::thing::object::Object;
use concept::thing::ObjectAPI;
use concept::type_::PlayerAPI;
use durability::wal::WAL;
use encoding::{
    EncodingKeyspace,
    graph::{thing::vertex_generator::ThingVertexGenerator, type_::vertex_generator::TypeVertexGenerator},
    value::{label::Label, value_type::ValueType},
};
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot};
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn thing_create_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::<'_, WriteSnapshot<'_, WAL>>::initialise_types(&mut storage, &type_vertex_generator);
    let snapshot: Rc<WriteSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_write());
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));

        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&person_label, false);

        let _person_1 = thing_manager.create_entity(person_type.clone());
        let _person_2 = thing_manager.create_entity(person_type.clone());
        let _person_3 = thing_manager.create_entity(person_type.clone());
        let _person_4 = thing_manager.create_entity(person_type.clone());
    }
    if let write_snapshot = Rc::try_unwrap(snapshot).ok().unwrap() {
        write_snapshot.commit().unwrap();
    }

    {
        let snapshot: Rc<ReadSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_read());
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
    TypeManager::<'_, WriteSnapshot<'_, WAL>>::initialise_types(&mut storage, &type_vertex_generator);

    let age_label = Label::build("age");
    let name_label = Label::build("name");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let snapshot: Rc<WriteSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_write());
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let age_type = type_manager.create_attribute_type(&age_label, false);
        age_type.set_value_type(&type_manager, ValueType::Long);
        let name_type = type_manager.create_attribute_type(&name_label, false);
        name_type.set_value_type(&type_manager, ValueType::String);

        let mut age_1 = thing_manager.create_attribute(age_type.clone(), Value::Long(age_value)).unwrap();
        assert_eq!(age_1.value(&thing_manager).unwrap(), Value::Long(age_value));

        let mut name_1 = thing_manager
            .create_attribute(name_type.clone(), Value::String(Cow::Owned(String::from(name_value).into_boxed_str())))
            .unwrap();
        assert_eq!(name_1.value(&thing_manager).unwrap(), Value::String(Cow::Owned(String::from(name_value).into_boxed_str())));
    }
    let write_snapshot = Rc::try_unwrap(snapshot).ok().unwrap();
    write_snapshot.commit().unwrap();

    {
        let snapshot: Rc<ReadSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_read());
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());
        let attributes = thing_manager.get_attributes().collect_cloned();
        assert_eq!(attributes.len(), 2);

        let age_type = type_manager.get_attribute_type(&age_label).unwrap().unwrap();
        let mut ages = thing_manager.get_attributes_in(age_type).unwrap().collect_cloned();
        assert_eq!(ages.len(), 1);
        assert_eq!(ages.first_mut().unwrap().value(&thing_manager).unwrap(), Value::Long(age_value));
    }
}

#[test]
fn has() {
    init_logging();
    let storage_path = create_tmp_dir();
    let mut storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();
    let type_vertex_generator = TypeVertexGenerator::new();
    TypeManager::<'_, WriteSnapshot<'_, WAL>>::initialise_types(&mut storage, &type_vertex_generator);

    let age_label = Label::build("age");
    let name_label = Label::build("name");
    let person_label = Label::build("person");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let snapshot: Rc<WriteSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_write());
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let age_type = type_manager.create_attribute_type(&age_label, false);
        age_type.set_value_type(&type_manager, ValueType::Long);
        let name_type = type_manager.create_attribute_type(&name_label, false);
        name_type.set_value_type(&type_manager, ValueType::String);

        let person_type = type_manager.create_entity_type(&person_label, false);

        let person_1 = thing_manager.create_entity(person_type.clone()).unwrap();
        let age_1 = thing_manager.create_attribute(age_type.clone(), Value::Long(age_value)).unwrap();
        let name_1 = thing_manager
            .create_attribute(name_type.clone(), Value::String(Cow::Owned(String::from(name_value).into_boxed_str())))
            .unwrap();

        person_1.set_has(&thing_manager, age_1);
        person_1.set_has(&thing_manager, name_1);

        let retrieved_attributes = person_1.get_has(&thing_manager).collect_cloned();
        assert_eq!(retrieved_attributes.len(), 2);
    }

    let write_snapshot = Rc::try_unwrap(snapshot).ok().unwrap();
    write_snapshot.commit().unwrap();

    {
        let snapshot: Rc<ReadSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_read());
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
    TypeManager::<'_, WriteSnapshot<'_, WAL>>::initialise_types(&mut storage, &type_vertex_generator);

    let employment_label = Label::build("employment");
    let employee_role = "employee";
    let employer_role = "employer";
    let person_label = Label::build("person");
    let company_label = Label::build("company");

    let snapshot: Rc<WriteSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_write());
    {
        let thing_vertex_generator = ThingVertexGenerator::new();
        let type_manager = Rc::new(TypeManager::new(snapshot.clone(), &type_vertex_generator, None));
        let thing_manager = ThingManager::new(snapshot.clone(), &thing_vertex_generator, type_manager.clone());

        let employment_type = type_manager.create_relation_type(&employment_label, false);
        employment_type.create_relates(&type_manager, employee_role);
        let employee_type = employment_type.get_relates_role(&type_manager, employee_role).unwrap().unwrap().role();
        employment_type.create_relates(&type_manager, employer_role);
        let employer_type = employment_type.get_relates_role(&type_manager, employer_role).unwrap().unwrap().role();
        let person_type = type_manager.create_entity_type(&person_label, false);
        let company_type = type_manager.create_entity_type(&company_label, false);
        person_type.set_plays(&type_manager, employee_type.clone());
        company_type.set_plays(&type_manager, employee_type.clone());

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

        assert_eq!(person_1.get_indexed_players(&thing_manager).count(), 3);
    }

    let write_snapshot = Rc::try_unwrap(snapshot).ok().unwrap();
    write_snapshot.commit().unwrap();
    {
        let snapshot: Rc<ReadSnapshot<'_, WAL>> = Rc::new(storage.open_snapshot_read());
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

        let person_1 = entities.iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&person_label).unwrap().unwrap())
            .unwrap();

        assert_eq!(person_1.get_relations(&thing_manager).count(), 2);
        assert_eq!(person_1.get_indexed_players(&thing_manager).count(), 3);
    }
}
