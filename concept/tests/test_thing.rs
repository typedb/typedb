/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{borrow::Cow, collections::HashMap, ops::Bound};

use concept::{
    error::ConceptReadError,
    thing::{
        attribute::Attribute,
        entity::Entity,
        object::{Object, ObjectAPI},
        relation::Relation,
        ThingAPI,
    },
    type_::{
        annotation::{AnnotationCardinality, AnnotationDistinct, AnnotationIndependent, AnnotationUnique},
        attribute_type::AttributeTypeAnnotation,
        object_type::ObjectType,
        owns::OwnsAnnotation,
        relates::RelatesAnnotation,
        type_manager::TypeManager,
        Ordering, OwnerAPI, PlayerAPI,
    },
};
use encoding::{
    error::EncodingError,
    graph::definition::definition_key::DefinitionKey,
    value::{
        label::Label,
        value::Value,
        value_struct::StructValue,
        value_type::{ValueType, ValueTypeCategory},
    },
};
use itertools::Itertools;
use resource::profile::{CommitProfile, StorageCounters};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WritableSnapshot, WriteSnapshot},
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

#[test]
fn thing_create_iterate() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let person_label = Label::build("person", None);
        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();

        let _person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let _person_2 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let _person_3 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let _person_4 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone(), None);
        let entities_count = thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).count();
        assert_eq!(entities_count, 4);
    }
}

#[test]
fn attribute_create() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let age_label = Label::build("age", None);
    let name_label = Label::build("name", None);

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
        age_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        name_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();

        let age_1 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(age_value)).unwrap();
        assert_eq!(
            age_1.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap(),
            Value::Integer(age_value)
        );

        let name_1 =
            thing_manager.create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(name_value))).unwrap();
        assert_eq!(
            name_1.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap(),
            Value::String(Cow::Borrowed(name_value))
        );

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let attributes_count = thing_manager.get_attributes(&snapshot, StorageCounters::DISABLED).unwrap().count();
        assert_eq!(attributes_count, 2);

        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();
        let mut ages: Vec<_> = thing_manager
            .get_attributes_in(&snapshot, age_type, StorageCounters::DISABLED)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(ages.len(), 1);
        assert_eq!(
            ages.first_mut().unwrap().get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap(),
            Value::Integer(age_value)
        );
    }
}

#[test]
fn has() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let age_label = Label::build("age", None);
    let name_label = Label::build("name", None);
    let person_label = Label::build("person", None);

    let age_value: i64 = 10;
    let name_value: &str = "TypeDB Fan";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
        age_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        name_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
        person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered).unwrap();

        let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let age_1 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(age_value)).unwrap();
        let name_1 = thing_manager
            .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned(String::from(name_value))))
            .unwrap();

        person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_1, StorageCounters::DISABLED).unwrap();
        person_1.set_has_unordered(&mut snapshot, &thing_manager, &name_1, StorageCounters::DISABLED).unwrap();

        let retrieved_attributes_count =
            person_1.get_has_unordered(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().count();
        assert_eq!(retrieved_attributes_count, 2);

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone(), None);
        let attributes_count = thing_manager.get_attributes(&snapshot, StorageCounters::DISABLED).unwrap().count();
        assert_eq!(attributes_count, 2);

        let people: Vec<Entity> =
            thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).try_collect().unwrap();
        let person_1 = people.first().unwrap();
        let retrieved_attributes_count =
            person_1.get_has_unordered(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().count();
        assert_eq!(retrieved_attributes_count, 2);
    }
}

#[test]
fn get_has_reverse_in_range() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let age_label = Label::build("age", None);
    let name_label = Label::build("name", None);
    let person_label = Label::build("person", None);
    let company_label = Label::build("company", None);

    let age_value_10: i64 = 10;
    let age_value_11: i64 = 11;
    let inlineable_name: &str = "TypeDB";
    let uninlinable_name: &str = "TypeDB Incorporated In America";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
        age_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        name_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
        let person_owns_age =
            person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
        person_owns_age
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
            )
            .unwrap();
        let person_owns_name =
            person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered).unwrap();
        person_owns_name
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
            )
            .unwrap();

        let company_type = type_manager.create_entity_type(&mut snapshot, &company_label).unwrap();
        let company_owns_age =
            company_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
        company_owns_age
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
            )
            .unwrap();
        let company_owns_name = company_type
            .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered)
            .unwrap();
        company_owns_name
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
            )
            .unwrap();

        let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let company_1 = thing_manager.create_entity(&mut snapshot, company_type).unwrap();
        let age_10 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(age_value_10)).unwrap();
        let age_11 = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(age_value_11)).unwrap();
        let name_inline = thing_manager
            .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(inlineable_name)))
            .unwrap();
        let name_hashed = thing_manager
            .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(uninlinable_name)))
            .unwrap();

        person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();
        person_1.set_has_unordered(&mut snapshot, &thing_manager, &age_11, StorageCounters::DISABLED).unwrap();
        person_1.set_has_unordered(&mut snapshot, &thing_manager, &name_inline, StorageCounters::DISABLED).unwrap();
        person_1.set_has_unordered(&mut snapshot, &thing_manager, &name_hashed, StorageCounters::DISABLED).unwrap();

        company_1.set_has_unordered(&mut snapshot, &thing_manager, &age_10, StorageCounters::DISABLED).unwrap();
        company_1.set_has_unordered(&mut snapshot, &thing_manager, &age_11, StorageCounters::DISABLED).unwrap();
        company_1.set_has_unordered(&mut snapshot, &thing_manager, &name_inline, StorageCounters::DISABLED).unwrap();
        company_1.set_has_unordered(&mut snapshot, &thing_manager, &name_hashed, StorageCounters::DISABLED).unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let person_type = type_manager.get_entity_type(&snapshot, &person_label).unwrap().unwrap();
        let company_type = type_manager.get_entity_type(&snapshot, &company_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();

        let age_owners_start_value_inclusive = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Included(Value::Integer(age_value_10)), Bound::Unbounded),
                &(Bound::Included(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(age_owners_start_value_inclusive.count(), 4);

        let age_owners_start_value_exclusive = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Excluded(Value::Integer(age_value_10)), Bound::Unbounded),
                &(Bound::Included(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(age_owners_start_value_exclusive.count(), 2);

        let age_owners_start_value_inclusive_end_value_exclusive = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Included(Value::Integer(age_value_10)), Bound::Excluded(Value::Integer(age_value_11))),
                &(Bound::Included(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(age_owners_start_value_inclusive_end_value_exclusive.count(), 2);

        let age_owners_start_value_excluded_end_value_exclusive = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Excluded(Value::Integer(age_value_10)), Bound::Excluded(Value::Integer(age_value_11))),
                &(Bound::Included(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(age_owners_start_value_excluded_end_value_exclusive.count(), 0);

        let age_owners_start_value_included_start_type_excluded = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Included(Value::Integer(age_value_10)), Bound::Unbounded),
                &(Bound::Excluded(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        // should skip age10-person, and return age10-company + age11-person + age11-company
        assert_eq!(age_owners_start_value_included_start_type_excluded.count(), 3);

        let age_owners_start_value_excluded_start_type_excluded = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Excluded(Value::Integer(age_value_10)), Bound::Unbounded),
                &(Bound::Excluded(ObjectType::Entity(person_type)), Bound::Unbounded),
                StorageCounters::DISABLED,
            )
            .unwrap();
        // should skip age10-person, age10-company, and the impl should be able to work out the next prefix is age(10+1)-(person+1), and return only age11-company
        assert_eq!(age_owners_start_value_excluded_start_type_excluded.count(), 1);

        let age_owners_end_value_included_end_type_excluded = thing_manager
            .get_has_reverse_in_range(
                &snapshot,
                age_type,
                &(Bound::Unbounded, Bound::Included(Value::Integer(age_value_11))),
                &(Bound::Excluded(ObjectType::Entity(person_type)), Bound::Excluded(ObjectType::Entity(company_type))),
                StorageCounters::DISABLED,
            )
            .unwrap();
        // should construct open start age* (making Excluded person type irrelevant), and end before age11-company, returning only age10-person + age10-company + age11-person
        assert_eq!(age_owners_end_value_included_end_type_excluded.count(), 3);
    }
}

#[test]
fn attribute_cleanup_on_concurrent_detach() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let age_label = Label::build("age", None);
    let name_label = Label::build("name", None);
    let person_label = Label::build("person", None);

    let age_value: i64 = 10;
    let name_alice_value: &str = "Alice";
    let name_bob_value: &str = "Bob";

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let age_type = type_manager.create_attribute_type(&mut snapshot, &age_label).unwrap();
        age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
        let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label).unwrap();
        name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
        let owns_age =
            person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Ordered).unwrap();
        owns_age
            .set_annotation(&mut snapshot, &type_manager, &thing_manager, OwnsAnnotation::Distinct(AnnotationDistinct))
            .unwrap();

        let person_name =
            person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Ordered).unwrap();
        person_name.set_ordering(&mut snapshot, &type_manager, &thing_manager, Ordering::Unordered).unwrap();

        let alice = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let bob = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let age = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer(age_value)).unwrap();
        let name_alice = thing_manager
            .create_attribute(&mut snapshot, name_type, Value::String(Cow::Borrowed(name_alice_value)))
            .unwrap();
        let name_bob = thing_manager
            .create_attribute(&mut snapshot, name_type, Value::String(Cow::Owned(String::from(name_bob_value))))
            .unwrap();

        alice
            .set_has_ordered(&mut snapshot, &thing_manager, age_type, vec![age.clone()], StorageCounters::DISABLED)
            .unwrap();
        alice.set_has_unordered(&mut snapshot, &thing_manager, &name_alice, StorageCounters::DISABLED).unwrap();
        bob.set_has_ordered(&mut snapshot, &thing_manager, age_type, vec![age], StorageCounters::DISABLED).unwrap();
        bob.set_has_unordered(&mut snapshot, &thing_manager, &name_bob, StorageCounters::DISABLED).unwrap();
        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    // two concurrent snapshots delete the independent ownerships
    let mut snapshot_1: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let mut snapshot_2: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();

    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let name_type = type_manager.get_attribute_type(&snapshot_1, &name_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot_1, &age_label).unwrap().unwrap();

        let bob = thing_manager
            .get_entities(&snapshot_1, StorageCounters::DISABLED)
            .find(|entity| {
                entity
                    .as_ref()
                    .unwrap()
                    .has_attribute_with_value(
                        &snapshot_1,
                        &thing_manager,
                        name_type,
                        Value::String(Cow::Borrowed(name_bob_value)),
                        StorageCounters::DISABLED,
                    )
                    .unwrap()
            })
            .unwrap()
            .unwrap();

        bob.unset_has_ordered(&mut snapshot_1, &thing_manager, age_type, StorageCounters::DISABLED).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot_1, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }

    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let name_type = type_manager.get_attribute_type(&snapshot_2, &name_label).unwrap().unwrap();
        let age_type = type_manager.get_attribute_type(&snapshot_2, &age_label).unwrap().unwrap();

        let alice = thing_manager
            .get_entities(&snapshot_2, StorageCounters::DISABLED)
            .find(|entity| {
                entity
                    .as_ref()
                    .unwrap()
                    .has_attribute_with_value(
                        &snapshot_2,
                        &thing_manager,
                        name_type,
                        Value::String(Cow::Borrowed(name_alice_value)),
                        StorageCounters::DISABLED,
                    )
                    .unwrap()
            })
            .unwrap()
            .unwrap();

        let mut ages: Vec<_> = thing_manager
            .get_attributes_in(&snapshot_2, age_type, StorageCounters::DISABLED)
            .unwrap()
            .try_collect()
            .unwrap();
        let age_position = ages
            .iter()
            .position(|attr| {
                attr.get_value(&snapshot_2, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_integer()
                    == age_value
            })
            .unwrap();
        ages.remove(age_position);
        alice.set_has_ordered(&mut snapshot_2, &thing_manager, age_type, ages, StorageCounters::DISABLED).unwrap();

        let finalise_result = thing_manager.finalise(&mut snapshot_2, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot_1.commit(&mut CommitProfile::DISABLED).unwrap();
    snapshot_2.commit(&mut CommitProfile::DISABLED).unwrap();

    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let age_type = type_manager.get_attribute_type(&snapshot, &age_label).unwrap().unwrap();

        let attributes_count =
            thing_manager.get_attributes_in(&snapshot, age_type, StorageCounters::DISABLED).unwrap().count();
        assert_eq!(attributes_count, 0);
    }
}

#[test]
fn role_player_distinct() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let employment_label = Label::build("employment", None);
    let employee_role = "employee";
    let employer_role = "employer";
    let person_label = Label::build("person", None);
    let company_label = Label::build("company", None);

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let employment_type = type_manager.create_relation_type(&mut snapshot, &employment_label).unwrap();
        employment_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, employee_role, Ordering::Ordered)
            .unwrap();
        let employee_relates =
            employment_type.get_relates_role_name(&snapshot, &type_manager, employee_role).unwrap().unwrap();
        employee_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Distinct(AnnotationDistinct),
            )
            .unwrap();
        employee_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(2))),
            )
            .unwrap();
        let employee_type = employee_relates.role();

        employment_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, employer_role, Ordering::Ordered)
            .unwrap();
        let employer_relates =
            employment_type.get_relates_role_name(&snapshot, &type_manager, employer_role).unwrap().unwrap();
        employer_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Distinct(AnnotationDistinct),
            )
            .unwrap();
        employer_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(1, Some(2))),
            )
            .unwrap();
        let employer_type = employer_relates.role();

        let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
        let company_type = type_manager.create_entity_type(&mut snapshot, &company_label).unwrap();
        person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, employee_type).unwrap();
        company_type.set_plays(&mut snapshot, &type_manager, &thing_manager, employer_type).unwrap();

        let person_1 = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
        let company_1 = thing_manager.create_entity(&mut snapshot, company_type).unwrap();
        let company_2 = thing_manager.create_entity(&mut snapshot, company_type).unwrap();
        let company_3 = thing_manager.create_entity(&mut snapshot, company_type).unwrap();

        let employment_1 = thing_manager.create_relation(&mut snapshot, employment_type).unwrap();
        employment_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                employee_type,
                Object::Entity(person_1),
                StorageCounters::DISABLED,
            )
            .unwrap();
        employment_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                employer_type,
                Object::Entity(company_1),
                StorageCounters::DISABLED,
            )
            .unwrap();

        let employment_2 = thing_manager.create_relation(&mut snapshot, employment_type).unwrap();
        employment_2
            .add_player(
                &mut snapshot,
                &thing_manager,
                employee_type,
                Object::Entity(person_1),
                StorageCounters::DISABLED,
            )
            .unwrap();
        employment_2
            .add_player(
                &mut snapshot,
                &thing_manager,
                employer_type,
                Object::Entity(company_2),
                StorageCounters::DISABLED,
            )
            .unwrap();
        employment_2
            .add_player(
                &mut snapshot,
                &thing_manager,
                employer_type,
                Object::Entity(company_3),
                StorageCounters::DISABLED,
            )
            .unwrap();

        assert_eq!(employment_1.get_players(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 2);
        assert_eq!(employment_2.get_players(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 3);

        assert_eq!(person_1.get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 2);
        assert_eq!(company_1.get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 1);
        assert_eq!(company_2.get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 1);
        assert_eq!(company_3.get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 1);

        assert_eq!(
            person_1
                .get_indexed_relations(&snapshot, &thing_manager, employment_type, StorageCounters::DISABLED)
                .unwrap()
                .count(),
            3
        );

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);

        let employment_type = type_manager.get_relation_type(&snapshot, &employment_label).unwrap().unwrap();

        let entities: Vec<Entity> =
            thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(entities.len(), 4);
        let relations: Vec<Relation> =
            thing_manager.get_relations(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(relations.len(), 2);

        let players_0 = relations[0].get_players(&snapshot, &thing_manager, StorageCounters::DISABLED).count();
        if players_0 == 2 {
            assert_eq!(relations[1].get_players(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 3);
        } else {
            assert_eq!(relations[1].get_players(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 2);
        }

        let person_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &person_label).unwrap().unwrap())
            .unwrap();

        assert_eq!(person_1.get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED).count(), 2);
        assert_eq!(
            person_1
                .get_indexed_relations(&snapshot, &thing_manager, employment_type, StorageCounters::DISABLED)
                .unwrap()
                .count(),
            3
        );
    }
}

#[test]
fn role_player_duplicates_unordered() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let collection_label = Label::build("collection", None);
    let entry_role_label = "entry";
    let owner_role_label = "owner";
    let resource_label = Label::build("resource", None);
    let group_label = Label::build("group", None);

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.create_relation_type(&mut snapshot, &collection_label).unwrap();
        collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, entry_role_label, Ordering::Unordered)
            .unwrap();
        let entry_relates =
            collection_type.get_relates_role_name(&snapshot, &type_manager, entry_role_label).unwrap().unwrap();
        entry_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(4))), // must be small to allow index to kick in
            )
            .unwrap();
        let entry_type = entry_relates.role();
        collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, owner_role_label, Ordering::Unordered)
            .unwrap();

        let owner_type =
            collection_type.get_relates_role_name(&snapshot, &type_manager, owner_role_label).unwrap().unwrap().role();

        let resource_type = type_manager.create_entity_type(&mut snapshot, &resource_label).unwrap();
        let group_type = type_manager.create_entity_type(&mut snapshot, &group_label).unwrap();
        resource_type.set_plays(&mut snapshot, &type_manager, &thing_manager, entry_type).unwrap();
        group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, owner_type).unwrap();

        let group_1 = thing_manager.create_entity(&mut snapshot, group_type).unwrap();
        let resource_1 = thing_manager.create_entity(&mut snapshot, resource_type).unwrap();

        let collection_1 = thing_manager.create_relation(&mut snapshot, collection_type).unwrap();
        collection_1
            .add_player(&mut snapshot, &thing_manager, owner_type, Object::Entity(group_1), StorageCounters::DISABLED)
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();

        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
        assert_eq!(player_counts, 2);

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 1);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_1_indexed_count, 1);
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_1_indexed_count, 1);

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.get_relation_type(&snapshot, &collection_label).unwrap().unwrap();

        let entities: Vec<Entity> =
            thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(entities.len(), 2);
        let relations: Vec<Relation> =
            thing_manager.get_relations(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(relations.len(), 1);

        let collection_1 = relations.first().unwrap();
        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
        assert_eq!(player_counts, 2);

        let group_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &group_label).unwrap().unwrap())
            .unwrap();

        let resource_1 = entities
            .iter()
            .find(|entity| entity.type_() == type_manager.get_entity_type(&snapshot, &resource_label).unwrap().unwrap())
            .unwrap();

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 1);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_1_indexed_count, 1);
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_1_indexed_count, 1);
    }
}

#[test]
fn role_player_duplicates_ordered_default_card() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let collection_label = Label::build("collection", None);
    let entry_role_label = "entry";
    let owner_role_label = "owner";
    let resource_label = Label::build("resource", None);
    let group_label = Label::build("group", None);

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.create_relation_type(&mut snapshot, &collection_label).unwrap();
        let entry_relates = collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, entry_role_label, Ordering::Ordered)
            .unwrap();
        entry_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(3))), // must be small to allow index to kick in
            )
            .unwrap();
        let entry_type = entry_relates.role();
        // This relates card is default!
        collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, owner_role_label, Ordering::Ordered)
            .unwrap();
        let owner_type =
            collection_type.get_relates_role_name(&snapshot, &type_manager, owner_role_label).unwrap().unwrap().role();

        let resource_type = type_manager.create_entity_type(&mut snapshot, &resource_label).unwrap();
        let group_type = type_manager.create_entity_type(&mut snapshot, &group_label).unwrap();
        resource_type.set_plays(&mut snapshot, &type_manager, &thing_manager, entry_type).unwrap();
        group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, owner_type).unwrap();

        let group_1 = thing_manager.create_entity(&mut snapshot, group_type).unwrap();
        let resource_1 = thing_manager.create_entity(&mut snapshot, resource_type).unwrap();

        let collection_1 = thing_manager.create_relation(&mut snapshot, collection_type).unwrap();
        collection_1
            .add_player(&mut snapshot, &thing_manager, owner_type, Object::Entity(group_1), StorageCounters::DISABLED)
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();

        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
        assert_eq!(player_counts, 3);

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 2);

        let result =
            group_1.get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED);
        assert!(result.is_err());

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.get_relation_type(&snapshot, &collection_label).unwrap().unwrap();
        let entities: Vec<Entity> =
            thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(entities.len(), 2);
        let relations: Vec<Relation> =
            thing_manager.get_relations(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(relations.len(), 1);

        let collection_1 = relations.first().unwrap();
        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
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
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 2);

        let result =
            group_1.get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED);
        assert!(result.is_err());
        let result =
            resource_1.get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED);
        assert!(result.is_err());
    }
}

#[test]
fn role_player_duplicates_ordered_small_card() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let collection_label = Label::build("collection", None);
    let entry_role_label = "entry";
    let owner_role_label = "owner";
    let resource_label = Label::build("resource", None);
    let group_label = Label::build("group", None);

    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    {
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.create_relation_type(&mut snapshot, &collection_label).unwrap();
        let entry_relates = collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, entry_role_label, Ordering::Ordered)
            .unwrap();
        entry_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(2))), // must be small to allow index to kick in
            )
            .unwrap();
        let entry_type = entry_relates.role();
        let owner_relates = collection_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, owner_role_label, Ordering::Ordered)
            .unwrap();
        owner_relates
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                RelatesAnnotation::Cardinality(AnnotationCardinality::new(0, Some(2))), // must be small to allow index to kick in
            )
            .unwrap();
        let owner_type =
            collection_type.get_relates_role_name(&snapshot, &type_manager, owner_role_label).unwrap().unwrap().role();

        let resource_type = type_manager.create_entity_type(&mut snapshot, &resource_label).unwrap();
        let group_type = type_manager.create_entity_type(&mut snapshot, &group_label).unwrap();
        resource_type.set_plays(&mut snapshot, &type_manager, &thing_manager, entry_type).unwrap();
        group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, owner_type).unwrap();

        let group_1 = thing_manager.create_entity(&mut snapshot, group_type).unwrap();
        let resource_1 = thing_manager.create_entity(&mut snapshot, resource_type).unwrap();

        let collection_1 = thing_manager.create_relation(&mut snapshot, collection_type).unwrap();
        collection_1
            .add_player(&mut snapshot, &thing_manager, owner_type, Object::Entity(group_1), StorageCounters::DISABLED)
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();
        collection_1
            .add_player(
                &mut snapshot,
                &thing_manager,
                entry_type,
                Object::Entity(resource_1),
                StorageCounters::DISABLED,
            )
            .unwrap();

        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
        assert_eq!(player_counts, 3);

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 2);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_1_indexed_count, 2, "Expected index to work");
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_1_indexed_count, 2, "Expected index to work");

        let group_relations_count: u64 = group_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);

        let finalise_result = thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED);
        assert!(finalise_result.is_ok());
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, thing_manager) = load_managers(storage.clone(), None);
        let collection_type = type_manager.get_relation_type(&snapshot, &collection_label).unwrap().unwrap();
        let entities: Vec<Entity> =
            thing_manager.get_entities(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(entities.len(), 2);
        let relations: Vec<Relation> =
            thing_manager.get_relations(&snapshot, StorageCounters::DISABLED).map(|result| result.unwrap()).collect();
        assert_eq!(relations.len(), 1);

        let collection_1 = relations.first().unwrap();
        let player_counts: u64 = collection_1
            .get_players(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| res.unwrap().1)
            .sum();
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
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_relations_count, 1);
        let resource_relations_count: u64 = resource_1
            .get_relations_roles(&snapshot, &thing_manager, StorageCounters::DISABLED)
            .map(|res| {
                let (_, _, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_relations_count, 2);

        let group_1_indexed_count: u64 = group_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(group_1_indexed_count, 2, "Expected index to work");
        let resource_1_indexed_count: u64 = resource_1
            .get_indexed_relations(&snapshot, &thing_manager, collection_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|res| {
                let (_, count) = res.unwrap();
                count
            })
            .sum();
        assert_eq!(resource_1_indexed_count, 2, "Expected index to work");
    }
}

#[test]
fn attribute_string_write_read_delete() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let attr_label = Label::build("test_string_attr", None);
    let short_string = "short".to_owned();
    let long_string = "this string is 33 characters long".to_owned();
    let attr_type = {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attr_type = type_manager.create_attribute_type(&mut snapshot, &attr_label).unwrap();
        attr_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        attr_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();

        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        attr_type
    };

    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        thing_manager
            .create_attribute(&mut snapshot, attr_type, Value::String(Cow::Borrowed(short_string.as_str())))
            .unwrap();
        thing_manager
            .create_attribute(&mut snapshot, attr_type, Value::String(Cow::Borrowed(long_string.as_str())))
            .unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    };

    // read them back by type
    {
        let snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attrs: Vec<Attribute> = thing_manager
            .get_attributes_in(&snapshot, attr_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|result| result.unwrap())
            .collect();
        let attr_values: Vec<String> = attrs
            .into_iter()
            .map(|attr| {
                (*attr.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string())
                    .to_owned()
            })
            .collect();
        assert!(attr_values.contains(&short_string));
        assert!(attr_values.contains(&long_string));
    }

    // read them back by value and delete
    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let read_short_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(short_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            short_string,
            read_short_string.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string()
        );
        let read_long_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(long_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            long_string,
            read_long_string.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string()
        );

        read_short_string.delete(&mut snapshot, &thing_manager, StorageCounters::DISABLED).unwrap();
        read_long_string.delete(&mut snapshot, &thing_manager, StorageCounters::DISABLED).unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    }

    // read them back by value with None results
    {
        let snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let read_short_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(short_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(None, read_short_string);
        let read_long_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(long_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(None, read_long_string);
    }
}

#[test]
fn attribute_string_write_read_delete_with_has() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let owner_label = Label::build("test_owner", None);
    let attr_label = Label::build("test_string_attr", None);
    let short_string = "short".to_owned();
    let long_string = "this string is 33 characters long".to_owned();
    let (owner_type, attr_type) = {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let owner_type = type_manager.create_entity_type(&mut snapshot, &owner_label).unwrap();
        let attr_type = type_manager.create_attribute_type(&mut snapshot, &attr_label).unwrap();
        attr_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        let owns =
            owner_type.set_owns(&mut snapshot, &type_manager, &thing_manager, attr_type, Ordering::Unordered).unwrap();
        owns.set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            OwnsAnnotation::Cardinality(AnnotationCardinality::new(0, None)),
        )
        .unwrap();
        owns.set_annotation(&mut snapshot, &type_manager, &thing_manager, OwnsAnnotation::Unique(AnnotationUnique))
            .unwrap();

        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        (owner_type, attr_type)
    };

    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let owner = thing_manager.create_entity(&mut snapshot, owner_type).unwrap();
        let short_attr = thing_manager
            .create_attribute(&mut snapshot, attr_type, Value::String(Cow::Borrowed(short_string.as_str())))
            .unwrap();
        let long_attr = thing_manager
            .create_attribute(&mut snapshot, attr_type, Value::String(Cow::Borrowed(long_string.as_str())))
            .unwrap();
        owner.set_has_unordered(&mut snapshot, &thing_manager, &short_attr, StorageCounters::DISABLED).unwrap();
        owner.set_has_unordered(&mut snapshot, &thing_manager, &long_attr, StorageCounters::DISABLED).unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    };

    // read them back by type
    {
        let snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attrs: Vec<Attribute> = thing_manager
            .get_attributes_in(&snapshot, attr_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|result| result.unwrap())
            .collect();
        let attr_values: Vec<String> = attrs
            .into_iter()
            .map(|attr| {
                (*attr.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string())
                    .to_owned()
            })
            .collect();
        assert!(attr_values.contains(&short_string));
        assert!(attr_values.contains(&long_string));
    }

    // read them back by value and delete
    {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let read_short_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(short_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            short_string,
            read_short_string.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string()
        );
        let read_long_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(long_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            long_string,
            read_long_string.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap().unwrap_string()
        );

        read_short_string.delete(&mut snapshot, &thing_manager, StorageCounters::DISABLED).unwrap();
        read_long_string.delete(&mut snapshot, &thing_manager, StorageCounters::DISABLED).unwrap();

        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    }

    // read them back by value with None results
    {
        let snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let read_short_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(short_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(None, read_short_string);
        let read_long_string = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::String(Cow::Borrowed(long_string.as_str())),
                StorageCounters::DISABLED,
            )
            .unwrap();
        assert_eq!(None, read_long_string);
    }
}

#[test]
fn attribute_struct_write_read() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let attr_label = Label::build("struct_test_attr", None);
    let struct_name = "struct_test_test".to_owned();
    let fields: HashMap<String, (ValueType, bool)> = HashMap::from([
        ("f0l".to_owned(), (ValueType::Integer, false)),
        ("f1s".to_owned(), (ValueType::String, false)),
    ]);

    let instance_fields = HashMap::from([
        ("f0l".to_owned(), Value::Integer(123)),
        ("f1s".to_owned(), Value::String(Cow::Owned("abc".to_owned()))),
    ]);
    let struct_key = {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let struct_key = define_struct(&mut snapshot, &type_manager, struct_name, fields);
        let attr_type = type_manager.create_attribute_type(&mut snapshot, &attr_label).unwrap();
        attr_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();
        attr_type
            .set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Struct(struct_key.clone()))
            .unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        struct_key
    };

    let (attr_created, struct_value) = {
        let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attr_type = type_manager.get_attribute_type(&snapshot, &attr_label).unwrap().unwrap();
        let struct_value = StructValue::build(
            struct_key.clone(),
            type_manager.get_struct_definition(&snapshot, struct_key.clone()).unwrap().clone(),
            instance_fields,
        )
        .unwrap();

        let attr_value_type = attr_type.get_value_type_without_source(&snapshot, &type_manager).unwrap().unwrap();
        assert_eq!(ValueTypeCategory::Struct, attr_value_type.category());
        let attr_instance = thing_manager
            .create_attribute(&mut snapshot, attr_type, Value::Struct(Cow::Owned(struct_value.clone())))
            .unwrap();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        (attr_instance, struct_value)
    };

    {
        let snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
        let attr_type = type_manager.get_attribute_type(&snapshot, &attr_label).unwrap().unwrap();
        let attr_vec: Vec<Attribute> = thing_manager
            .get_attributes_in(&snapshot, attr_type, StorageCounters::DISABLED)
            .unwrap()
            .map(|result| result.unwrap())
            .collect();
        let attr = attr_vec.first().unwrap().clone();
        let value_0 = attr.get_value(&snapshot, &thing_manager, StorageCounters::DISABLED).unwrap();
        match value_0 {
            Value::Struct(v) => assert_eq!(struct_value, *v),
            _ => panic!("Wrong data type"),
        }

        let attr_by_id = thing_manager
            .get_attribute_with_value(
                &snapshot,
                attr_type,
                Value::Struct(Cow::Borrowed(&struct_value)),
                StorageCounters::DISABLED,
            )
            .unwrap()
            .unwrap();
        assert_eq!(attr, attr_by_id);
        assert_eq!(attr_created, attr);
        snapshot.close_resources();
    }
}

#[test]
fn read_attribute_struct_by_field() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let attr_label = Label::build("index_test_attr", None);
    let nested_struct_spec =
        ("nested_test_struct".to_owned(), HashMap::from([("nested_string".to_owned(), (ValueType::String, false))]));

    let nested_struct_key = {
        let mut snapshot = storage.clone().open_snapshot_write();
        let nested_struct_key = define_struct(&mut snapshot, &type_manager, nested_struct_spec.0, nested_struct_spec.1);
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        nested_struct_key
    };

    let struct_spec = (
        "index_test_struct".to_owned(),
        HashMap::from([("f_nested".to_owned(), (ValueType::Struct(nested_struct_key.clone()), false))]),
    );
    let (attr_type, struct_key, struct_def) = {
        let mut snapshot = storage.clone().open_snapshot_write();
        let struct_key = define_struct(&mut snapshot, &type_manager, struct_spec.0, struct_spec.1);
        let attr_type = type_manager.create_attribute_type(&mut snapshot, &attr_label).unwrap();
        attr_type
            .set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Struct(struct_key.clone()))
            .unwrap();
        attr_type
            .set_annotation(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                AttributeTypeAnnotation::Independent(AnnotationIndependent),
            )
            .unwrap();
        let struct_def = type_manager.get_struct_definition(&snapshot, struct_key.clone()).unwrap();
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        (attr_type, struct_key.clone(), struct_def)
    };

    // Create value
    let field_values = ["abc", "xyz_but_long_enough_to_hash"];
    {
        let mut snapshot = storage.clone().open_snapshot_write();
        let mut attrs = Vec::new();
        for val in field_values {
            let nested_struct_value =
                StructValue::new(nested_struct_key.clone(), HashMap::from([(0, Value::String(Cow::Borrowed(val)))]));
            let outer_struct_value = StructValue::new(
                struct_key.clone(),
                HashMap::from([(0, Value::Struct(Cow::Owned(nested_struct_value)))]),
            );
            let attr = thing_manager
                .create_attribute(&mut snapshot, attr_type, Value::Struct(Cow::Borrowed(&outer_struct_value)))
                .unwrap();
            attrs.push(attr);
        }

        for (val, attr) in std::iter::zip(field_values, attrs.iter()) {
            let field_path = type_manager
                .resolve_struct_field(&snapshot, &["f_nested", "nested_string"], struct_def.clone())
                .unwrap();
            let attr_by_field_iterator = thing_manager
                .get_attributes_by_struct_field(
                    &snapshot,
                    attr_type,
                    field_path,
                    Value::String(Cow::Borrowed(val)),
                    StorageCounters::DISABLED,
                )
                .unwrap();
            let mut attr_by_field: Vec<Attribute> = Vec::new();
            for res in attr_by_field_iterator {
                attr_by_field.push(res.as_ref().unwrap().clone());
            }
            assert_eq!(1, attr_by_field.len());
            assert_eq!(attr, attr_by_field.first().unwrap());
        }
        snapshot.close_resources();
    };
}

#[test]
fn attribute_struct_errors() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, _thing_manager) = load_managers(storage.clone(), None);

    let (struct_key, nested_struct_key) = {
        let mut snapshot = storage.clone().open_snapshot_write();
        let nested_struct_spec = (
            "nested_test_struct".to_owned(),
            HashMap::from([("nested_string".to_owned(), (ValueType::String, false))]),
        );
        let nested_struct_key = define_struct(&mut snapshot, &type_manager, nested_struct_spec.0, nested_struct_spec.1);

        let struct_spec = (
            "errors_test_struct".to_owned(),
            HashMap::from([("f_nested".to_owned(), (ValueType::Struct(nested_struct_key.clone()), false))]),
        );
        let struct_key = define_struct(&mut snapshot, &type_manager, struct_spec.0, struct_spec.1);
        snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
        (struct_key, nested_struct_key)
    };

    {
        let snapshot = storage.clone().open_snapshot_write();
        let struct_def = type_manager.get_struct_definition(&snapshot, struct_key.clone()).unwrap();
        assert!(matches!(
            *type_manager.resolve_struct_field(&snapshot, &["non-existant"], struct_def.clone()).unwrap_err(),
            ConceptReadError::Encoding { source: EncodingError::StructFieldUnresolvable { .. } }
        ));
        assert!(matches!(
            *type_manager
                .resolve_struct_field(
                    &snapshot,
                    &["f_nested", "nested_string", "but-strings-arent-structs"],
                    struct_def.clone()
                )
                .unwrap_err(),
            ConceptReadError::Encoding { source: EncodingError::IndexingIntoNonStructField { .. } }
        ));
        assert!(matches!(
            *type_manager.resolve_struct_field(&snapshot, &[], struct_def.clone()).unwrap_err(),
            ConceptReadError::Encoding { source: EncodingError::StructPathIncomplete { .. } }
        ));
    };

    {
        let snapshot = storage.clone().open_snapshot_write();
        let struct_def = type_manager.get_struct_definition(&snapshot, struct_key.clone()).unwrap();
        type_manager.get_struct_definition(&snapshot, nested_struct_key.clone()).unwrap();
        {
            let err = StructValue::build(
                struct_key.clone(),
                struct_def.clone(),
                HashMap::from([("f_nested".to_owned(), Value::Integer(0))]),
            )
            .unwrap_err();
            assert!(
                err.len() == 1 && matches!(err.first().unwrap(), EncodingError::StructFieldValueTypeMismatch { .. })
            );
        }
        {
            let err = StructValue::build(struct_key.clone(), struct_def.clone(), HashMap::from([])).unwrap_err();
            assert!(err.len() == 1 && matches!(err.first().unwrap(), EncodingError::StructMissingRequiredField { .. }));
        }
        snapshot.close_resources();
    }
}

pub fn define_struct(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    name: String,
    definitions: HashMap<String, (ValueType, bool)>,
) -> DefinitionKey {
    let struct_key = type_manager.create_struct(snapshot, name).unwrap();
    for (name, (value_type, optional)) in definitions {
        type_manager.create_struct_field(snapshot, struct_key.clone(), &name, value_type, optional).unwrap();
    }
    struct_key
}
