/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use concept::{
    thing::{
        object::{Object, ObjectAPI},
        thing_manager::ThingManager,
        value::Value,
    },
    type_::{
        annotation::{AnnotationCardinality, AnnotationDistinct, AnnotationIndependent},
        attribute_type::AttributeTypeAnnotation,
        owns::OwnsAnnotation,
        role_type::RoleTypeAnnotation,
        type_manager::TypeManager,
        Ordering, OwnerAPI, PlayerAPI,
    },
};
use durability::wal::WAL;
use encoding::{
    graph::{
        definition::{definition_key_generator::DefinitionKeyGenerator, r#struct::StructDefinition},
        thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    value::{label::Label, value_type::ValueType, value_struct::StructValue},
    AsBytes, EncodingKeyspace,
};
use encoding::value::value_struct::FieldValue;
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn thing_create_iterate() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));

        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let person_label = Label::build("person");
        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();

        let _person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_3 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let _person_4 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager);
        let entities = thing_manager.get_entities(&snapshot).collect_cloned();
        assert_eq!(entities.len(), 4);
    }
}

#[test]
fn attribute_create() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let age_label = Label::build("age");
    let name_label = Label::build("name");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label, false).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
        age_type
            .set_annotation(&mut snapshot, &type_manager, AttributeTypeAnnotation::Independent(AnnotationIndependent))
            .unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label, false).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();
        name_type
            .set_annotation(&mut snapshot, &type_manager, AttributeTypeAnnotation::Independent(AnnotationIndependent))
            .unwrap();

        let mut age_1 =
            thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(age_value)).unwrap();
        assert_eq!(age_1.get_value(&snapshot, &thing_manager).unwrap(), Value::Long(age_value));

        let mut name_1 = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed(name_value)))
            .unwrap();
        assert_eq!(name_1.get_value(&snapshot, &thing_manager).unwrap(), Value::String(Cow::Borrowed(name_value)));

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let attributes = thing_manager.get_attributes(&snapshot).collect_cloned();
        assert_eq!(attributes.len(), 2);

        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();
        let mut ages = thing_manager.get_attributes_in(&snapshot, age_type).unwrap().collect_cloned();
        assert_eq!(ages.len(), 1);
        assert_eq!(ages.first_mut().unwrap().get_value(&snapshot, &thing_manager).unwrap(), Value::Long(age_value));
    }
}

#[test]
fn has() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let age_label = Label::build("age");
    let name_label = Label::build("name");
    let person_label = Label::build("person");

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label, false).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
        age_type
            .set_annotation(&mut snapshot, &type_manager, AttributeTypeAnnotation::Independent(AnnotationIndependent))
            .unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label, false).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();
        name_type
            .set_annotation(&mut snapshot, &type_manager, AttributeTypeAnnotation::Independent(AnnotationIndependent))
            .unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();

        let person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let age_1 = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(age_value)).unwrap();
        let name_1 = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned(String::from(name_value))))
            .unwrap();

        person_1.set_has_unordered(&mut snapshot, &thing_manager, age_1).unwrap();
        person_1.set_has_unordered(&mut snapshot, &thing_manager, name_1).unwrap();

        let retrieved_attributes_count = person_1.get_has_unordered(&snapshot, &thing_manager).count();
        assert_eq!(retrieved_attributes_count, 2);

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let attributes = thing_manager.get_attributes(&snapshot).collect_cloned();
        assert_eq!(attributes.len(), 2);

        let people = thing_manager.get_entities(&snapshot).collect_cloned();
        let person_1 = people.first().unwrap();
        let retrieved_attributes_count = person_1.get_has_unordered(&snapshot, &thing_manager).count();
        assert_eq!(retrieved_attributes_count, 2);
    }
}

#[test]
fn attribute_cleanup_on_concurrent_detach() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let age_label = Label::build("age");
    let name_label = Label::build("name");
    let person_label = Label::build("person");

    let age_value: i64 = 10;
    let name_alice_value: &str = "Alice";
    let name_bob_value: &str = "Bob";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label, false).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, ValueType::Long).unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label, false).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, ValueType::String).unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();
        let owns_age =
            person_type.set_owns(&mut snapshot, &type_manager, age_type.clone(), Ordering::Unordered).unwrap();
        owns_age.set_annotation(&mut snapshot, &type_manager, OwnsAnnotation::Distinct(AnnotationDistinct)).unwrap();

        person_type.set_owns(&mut snapshot, &type_manager, name_type.clone(), Ordering::Unordered).unwrap();

        let alice = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let bob = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let age = thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(age_value)).unwrap();
        let name_alice = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed(name_alice_value)))
            .unwrap();
        let name_bob = thing_manager
            .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Owned(String::from(name_bob_value))))
            .unwrap();

        alice.set_has_unordered(&mut snapshot, &thing_manager, age.as_reference()).unwrap();
        alice.set_has_unordered(&mut snapshot, &thing_manager, name_alice).unwrap();
        bob.set_has_unordered(&mut snapshot, &thing_manager, age).unwrap();
        bob.set_has_unordered(&mut snapshot, &thing_manager, name_bob).unwrap();
        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();

    // two concurrent snapshots delete the independent ownerships
    let mut snapshot_1: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let mut snapshot_2: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();

    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let name_type = type_manager.get_attribute_type(&snapshot_1, &name_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot_1, &age_label).unwrap().unwrap();

        let entities = thing_manager.get_entities(&snapshot_1).collect_cloned();
        let bob = entities
            .iter()
            .find(|entity| {
                entity
                    .has_attribute(
                        &snapshot_1,
                        &thing_manager,
                        name_type.clone(),
                        Value::String(Cow::Borrowed(name_bob_value)),
                    )
                    .unwrap()
            })
            .unwrap();

        let mut ages = thing_manager.get_attributes_in(&snapshot_1, age_type.clone()).unwrap().collect_cloned();
        let age = ages
            .iter_mut()
            .filter_map(|attr| {
                if attr.get_value(&snapshot_1, &thing_manager).unwrap().unwrap_long() == age_value {
                    Some(attr.as_reference())
                } else {
                    None
                }
            })
            .next()
            .unwrap();
        bob.unset_has_unordered(&mut snapshot_1, &thing_manager, age).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot_1);
        assert!(finalise_result.is_ok());
    }

    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let name_type = type_manager.get_attribute_type(&snapshot_2, &name_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot_2, &age_label).unwrap().unwrap();

        let entities = thing_manager.get_entities(&snapshot_2).collect_cloned();
        let alice = entities
            .iter()
            .find(|entity| {
                entity
                    .has_attribute(
                        &snapshot_2,
                        &thing_manager,
                        name_type.clone(),
                        Value::String(Cow::Borrowed(name_alice_value)),
                    )
                    .unwrap()
            })
            .unwrap();

        let mut ages = thing_manager.get_attributes_in(&snapshot_2, age_type.clone()).unwrap().collect_cloned();
        let age = ages
            .iter_mut()
            .filter_map(|attr| {
                if attr.get_value(&snapshot_2, &thing_manager).unwrap().unwrap_long() == age_value {
                    Some(attr.as_reference())
                } else {
                    None
                }
            })
            .next()
            .unwrap();
        alice.unset_has_unordered(&mut snapshot_2, &thing_manager, age).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot_2);
        assert!(finalise_result.is_ok());
    }

    snapshot_1.commit().unwrap();
    snapshot_2.commit().unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();

        let attributes = thing_manager.get_attributes_in(&snapshot, age_type).unwrap().collect_cloned();
        assert_eq!(attributes.len(), 0);
    }
}

#[test]
fn role_player_distinct() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let employment_label = Label::build("employment");
    let employee_role = "employee";
    let employer_role = "employer";
    let person_label = Label::build("person");
    let company_label = Label::build("company");

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

        let employment_type = type_manager.create_relation_type(&mut snapshot, &employment_label, false).unwrap();
        employment_type.create_relates(&mut snapshot, &type_manager, employee_role, Ordering::Unordered).unwrap();
        let employee_type =
            employment_type.get_relates_role(&snapshot, &type_manager, employee_role).unwrap().unwrap().role();
        employee_type
            .set_annotation(&mut snapshot, &type_manager, RoleTypeAnnotation::Distinct(AnnotationDistinct))
            .unwrap();
        employment_type.create_relates(&mut snapshot, &type_manager, employer_role, Ordering::Unordered).unwrap();
        let employer_type =
            employment_type.get_relates_role(&snapshot, &type_manager, employer_role).unwrap().unwrap().role();
        employer_type
            .set_annotation(&mut snapshot, &type_manager, RoleTypeAnnotation::Distinct(AnnotationDistinct))
            .unwrap();
        employer_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                RoleTypeAnnotation::Cardinality(AnnotationCardinality::new(1, Some(2))),
            )
            .unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label, false).unwrap();
        let company_type = type_manager.create_entity_type(&mut snapshot, &company_label, false).unwrap();
        person_type.set_plays(&mut snapshot, &type_manager, employee_type.clone()).unwrap();
        company_type.set_plays(&mut snapshot, &type_manager, employee_type.clone()).unwrap();

        let person_1 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
        let company_1 = thing_manager.create_entity(&mut snapshot, company_type.clone()).unwrap();
        let company_2 = thing_manager.create_entity(&mut snapshot, company_type.clone()).unwrap();
        let company_3 = thing_manager.create_entity(&mut snapshot, company_type.clone()).unwrap();

        let employment_1 = thing_manager.create_relation(&mut snapshot, employment_type.clone()).unwrap();
        employment_1
            .add_player(&mut snapshot, &thing_manager, employee_type.clone(), Object::Entity(person_1.as_reference()))
            .unwrap();
        employment_1
            .add_player(&mut snapshot, &thing_manager, employer_type.clone(), Object::Entity(company_1.as_reference()))
            .unwrap();

        let employment_2 = thing_manager.create_relation(&mut snapshot, employment_type.clone()).unwrap();
        employment_2
            .add_player(&mut snapshot, &thing_manager, employee_type.clone(), Object::Entity(person_1.as_reference()))
            .unwrap();
        employment_2
            .add_player(&mut snapshot, &thing_manager, employer_type.clone(), Object::Entity(company_2.as_reference()))
            .unwrap();
        employment_2
            .add_player(&mut snapshot, &thing_manager, employer_type.clone(), Object::Entity(company_3.as_reference()))
            .unwrap();

        assert_eq!(employment_1.get_players(&snapshot, &thing_manager).count(), 2);
        assert_eq!(employment_2.get_players(&snapshot, &thing_manager).count(), 3);

        assert_eq!(person_1.get_relations(&snapshot, &thing_manager).count(), 2);
        assert_eq!(company_1.get_relations(&snapshot, &thing_manager).count(), 1);
        assert_eq!(company_2.get_relations(&snapshot, &thing_manager).count(), 1);
        assert_eq!(company_3.get_relations(&snapshot, &thing_manager).count(), 1);

        assert_eq!(person_1.get_indexed_players(&snapshot, &thing_manager).count(), 3);

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
        let entities = thing_manager.get_entities(&snapshot).collect_cloned();
        assert_eq!(entities.len(), 4);
        let relations = thing_manager.get_relations(&snapshot).collect_cloned();
        assert_eq!(relations.len(), 2);

        let players_0 = relations[0].get_players(&snapshot, &thing_manager).count();
        if players_0 == 2 {
            assert_eq!(relations[1].get_players(&snapshot, &thing_manager).count(), 3);
        } else {
            assert_eq!(relations[1].get_players(&snapshot, &thing_manager).count(), 2);
        }

        let person_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &person_label).unwrap().unwrap())
            .unwrap();

        assert_eq!(person_1.get_relations(&snapshot, &thing_manager).count(), 2);
        assert_eq!(person_1.get_indexed_players(&snapshot, &thing_manager).count(), 3);
    }
}

#[test]
fn role_player_duplicates() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let list_label = Label::build("list");
    let entry_role_label = "entry";
    let owner_role_label = "owner";
    let resource_label = Label::build("resource");
    let group_label = Label::build("group");

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

        let list_type = type_manager.create_relation_type(&mut snapshot, &list_label, false).unwrap();
        list_type.create_relates(&mut snapshot, &type_manager, entry_role_label, Ordering::Unordered).unwrap();
        let entry_type =
            list_type.get_relates_role(&snapshot, &type_manager, entry_role_label).unwrap().unwrap().role();
        entry_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                RoleTypeAnnotation::Cardinality(AnnotationCardinality::new(0, Some(4))), // must be small to allow index to kick in
            )
            .unwrap();
        list_type.create_relates(&mut snapshot, &type_manager, owner_role_label, Ordering::Unordered).unwrap();
        let owner_type =
            list_type.get_relates_role(&snapshot, &type_manager, owner_role_label).unwrap().unwrap().role();

        let resource_type = type_manager.create_entity_type(&mut snapshot, &resource_label, false).unwrap();
        let group_type = type_manager.create_entity_type(&mut snapshot, &group_label, false).unwrap();
        resource_type.set_plays(&mut snapshot, &type_manager, entry_type.clone()).unwrap();
        group_type.set_plays(&mut snapshot, &type_manager, owner_type.clone()).unwrap();

        let group_1 = thing_manager.create_entity(&mut snapshot, group_type.clone()).unwrap();
        let resource_1 = thing_manager.create_entity(&mut snapshot, resource_type.clone()).unwrap();

        let list_1 = thing_manager.create_relation(&mut snapshot, list_type.clone()).unwrap();
        list_1
            .add_player(&mut snapshot, &thing_manager, owner_type.clone(), Object::Entity(group_1.as_reference()))
            .unwrap();
        list_1
            .add_player(&mut snapshot, &thing_manager, entry_type.clone(), Object::Entity(resource_1.as_reference()))
            .unwrap();
        list_1
            .add_player(&mut snapshot, &thing_manager, entry_type.clone(), Object::Entity(resource_1.as_reference()))
            .unwrap();

        let player_counts: u64 =
            list_1.get_players(&snapshot, &thing_manager).map_static(|res| res.unwrap().1).into_iter().sum();
        assert_eq!(player_counts, 3);

        let group_relations_count: u64 = group_1
            .get_relations(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(resource_relations_count, 2);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_players(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(group_1_indexed_count, 2);
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_players(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(resource_1_indexed_count, 2);

        let group_relations_count: u64 = group_1
            .get_relations(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(group_relations_count, 1);

        let finalise_result = thing_manager.finalise(&mut snapshot);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit().unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.open_snapshot_read();
        let type_manager =
            Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
        let thing_vertex_generator = ThingVertexGenerator::new();
        let thing_manager = ThingManager::new(Arc::new(thing_vertex_generator), type_manager.clone());
        let entities = thing_manager.get_entities(&snapshot).collect_cloned();
        assert_eq!(entities.len(), 2);
        let relations = thing_manager.get_relations(&snapshot).collect_cloned();
        assert_eq!(relations.len(), 1);

        let list_1 = relations.first().unwrap();
        let player_counts: u64 =
            list_1.get_players(&snapshot, &thing_manager).map_static(|res| res.unwrap().1).into_iter().sum();
        assert_eq!(player_counts, 3);

        let group_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &group_label).unwrap().unwrap())
            .unwrap();

        let resource_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &resource_label).unwrap().unwrap())
            .unwrap();

        let group_relations_count: u64 = group_1
            .get_relations(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(resource_relations_count, 2);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_players(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(group_1_indexed_count, 2);
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_players(&snapshot, &thing_manager)
            .map_static(|res| {
                let (_, _, _, count) = res.unwrap();
                count
            })
            .into_iter()
            .sum();
        assert_eq!(resource_1_indexed_count, 2);
    }
}

#[test]
fn struct_create() {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    TypeManager::<WriteSnapshot<WALClient>>::initialise_types(
        storage.clone(),
        definition_key_generator.clone(),
        type_vertex_generator.clone(),
    )
    .unwrap();

    let thing_vertex_generator = Arc::new(ThingVertexGenerator::new());
    let type_manager =
        Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), None));
    let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());

    let attr_0_label = Label::build("attr_0");
    let struct_0_name = "struct_0";
    let fields_0: HashMap<String, (ValueType, bool)> = HashMap::from([
        ("s0_f0_l0".to_owned(), (ValueType::Long, false)),
        ("s0_f1_s0".to_owned(), (ValueType::String, false)),
    ]);
    let definition_0 = StructDefinition::define(struct_0_name.to_owned(), fields_0);

    let instance_0_fields = HashMap::from([
        ("s0_f0_l0".to_owned(), FieldValue::Long(123)),
        ("s0_f1_s0".to_owned(), FieldValue::String(Cow::Owned("abc".to_owned()))),
    ]);
    let struct_0_key = {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let struct_0_key = type_manager.create_struct(&mut snapshot, definition_0.clone()).unwrap();
        let attr_0_type = type_manager.create_attribute_type(&mut snapshot, &attr_0_label, false).unwrap();
        attr_0_type.set_value_type(&mut snapshot, &type_manager, ValueType::Struct(struct_0_key.clone())).unwrap();
        snapshot.commit().unwrap();
        struct_0_key
    };

    let instance_0 =
        StructValue::try_translate_fields(struct_0_key.clone(), definition_0.clone(), instance_0_fields).unwrap();
    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let mut attr_0_type = type_manager.get_attribute_type(&snapshot, &attr_0_label).unwrap().unwrap();
        attr_0_type
            .set_annotation(&mut snapshot, &type_manager, AttributeTypeAnnotation::Independent(AnnotationIndependent))
            .unwrap();
        let attr_0_value_type = attr_0_type.get_value_type(&snapshot, &type_manager).unwrap().unwrap();
        let struct_0_key = match attr_0_value_type {
            ValueType::Struct(struct_0_key) => struct_0_key,
            v => panic!("Unexpected value type: {:?}", v),
        };
        let read_key = type_manager.get_struct_definition_key(&snapshot, &struct_0_name).unwrap().unwrap();
        let read_definition = type_manager.get_struct_definition(&snapshot, struct_0_key.clone()).unwrap();
        assert_eq!(struct_0_key.bytes(), read_key.bytes());
        assert_eq!(definition_0, *read_definition);
        thing_manager
            .create_attribute(&mut snapshot, attr_0_type, Value::Struct(Cow::Owned(instance_0.clone())))
            .unwrap();
        snapshot.commit().unwrap();
    }

    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attr_0_type = type_manager.get_attribute_type(&snapshot, &attr_0_label).unwrap().unwrap();
        let attr_vec = thing_manager.get_attributes_in(&snapshot, attr_0_type.clone()).unwrap().collect_cloned();
        let mut attr_0 = attr_vec.get(0).unwrap().clone();
        let value_0 = attr_0.get_value(&snapshot, &thing_manager).unwrap();
        match value_0 {
            Value::Struct(v) => {
                assert_eq!(instance_0, *v);
            }
            _ => assert!(false, "Wrong data type"),
        }
        snapshot.commit().unwrap();
    }
}
