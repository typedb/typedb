/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{collections::BTreeMap, sync::Arc};

use concept::{
    thing::{object::ObjectAPI, statistics::Statistics, thing_manager::ThingManager, ThingAPI},
    type_::{
        annotation::AnnotationCardinality, relates::RelatesAnnotation, ObjectTypeAPI, Ordering, OwnerAPI, PlayerAPI,
    },
};
use encoding::value::{label::Label, value::Value, value_type::ValueType};
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    sequence_number::SequenceNumber,
    snapshot::{CommittableSnapshot, ReadableSnapshot},
    MVCCStorage,
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

macro_rules! assert_statistics_eq {
    ($lhs:expr, $rhs:expr) => {
        let Statistics {
            sequence_number: lhs_sequence_number,
            total_thing_count: lhs_total_thing_count,
            total_entity_count: lhs_total_entity_count,
            total_relation_count: lhs_total_relation_count,
            total_attribute_count: lhs_total_attribute_count,
            total_role_count: lhs_total_role_count,
            total_has_count: lhs_total_has_count,
            entity_counts: mut lhs_entity_counts,
            relation_counts: mut lhs_relation_counts,
            attribute_counts: mut lhs_attribute_counts,
            role_counts: lhs_role_counts,
            has_attribute_counts: lhs_has_attribute_counts,
            attribute_owner_counts: lhs_attribute_owner_counts,
            role_player_counts: lhs_role_player_counts,
            relation_role_counts: lhs_relation_role_counts,
            relation_role_player_counts: lhs_relation_role_player_counts,
            player_role_relation_counts: lhs_player_role_relation_counts,
            links_index_counts: lhs_player_index_counts,
            ..
        } = $lhs;
        lhs_entity_counts.retain(|_, v| *v > 0);
        lhs_relation_counts.retain(|_, v| *v > 0);
        lhs_attribute_counts.retain(|_, v| *v > 0);

        let Statistics {
            sequence_number: rhs_sequence_number,
            total_thing_count: rhs_total_thing_count,
            total_entity_count: rhs_total_entity_count,
            total_relation_count: rhs_total_relation_count,
            total_attribute_count: rhs_total_attribute_count,
            total_role_count: rhs_total_role_count,
            total_has_count: rhs_total_has_count,
            entity_counts: mut rhs_entity_counts,
            relation_counts: mut rhs_relation_counts,
            attribute_counts: mut rhs_attribute_counts,
            role_counts: rhs_role_counts,
            has_attribute_counts: rhs_has_attribute_counts,
            attribute_owner_counts: rhs_attribute_owner_counts,
            role_player_counts: rhs_role_player_counts,
            relation_role_counts: rhs_relation_role_counts,
            relation_role_player_counts: rhs_relation_role_player_counts,
            player_role_relation_counts: rhs_player_role_relation_counts,
            links_index_counts: rhs_player_index_counts,
            ..
        } = $rhs;
        rhs_entity_counts.retain(|_, v| *v > 0);
        rhs_relation_counts.retain(|_, v| *v > 0);
        rhs_attribute_counts.retain(|_, v| *v > 0);

        assert_eq!(
            (
                lhs_sequence_number,
                (lhs_total_thing_count, lhs_total_entity_count, lhs_total_relation_count, lhs_total_attribute_count),
                (lhs_total_role_count, lhs_total_has_count),
                (lhs_entity_counts, lhs_relation_counts, lhs_attribute_counts, lhs_role_counts),
                (lhs_has_attribute_counts, lhs_attribute_owner_counts),
                (lhs_role_player_counts, lhs_relation_role_counts, lhs_player_index_counts),
                (lhs_relation_role_player_counts, lhs_player_role_relation_counts),
            ),
            (
                rhs_sequence_number,
                (rhs_total_thing_count, rhs_total_entity_count, rhs_total_relation_count, rhs_total_attribute_count),
                (rhs_total_role_count, rhs_total_has_count),
                (rhs_entity_counts, rhs_relation_counts, rhs_attribute_counts, rhs_role_counts),
                (rhs_has_attribute_counts, rhs_attribute_owner_counts),
                (rhs_role_player_counts, rhs_relation_role_counts, rhs_player_index_counts),
                (rhs_relation_role_player_counts, rhs_player_role_relation_counts),
            )
        );
    };
}

fn read_statistics(storage: Arc<MVCCStorage<WALClient>>, thing_manager: &ThingManager) -> Statistics {
    let snapshot = storage.clone().open_snapshot_read();

    let mut statistics = Statistics::new(snapshot.open_sequence_number());

    let mut entity_iter = thing_manager.get_entities(&snapshot);
    while let Some(entity) = entity_iter.next() {
        let entity = entity.unwrap();
        statistics.total_entity_count += 1;
        *statistics.entity_counts.entry(entity.type_()).or_default() += 1;
        let owner_type = entity.type_().into_owned_object_type();
        let mut has_iter = entity.get_has_unordered(&snapshot, thing_manager);
        while let Some(has) = has_iter.next() {
            let (attribute, count) = has.unwrap();
            *statistics
                .has_attribute_counts
                .entry(owner_type.clone())
                .or_default()
                .entry(attribute.type_())
                .or_default() += count;
            *statistics
                .attribute_owner_counts
                .entry(attribute.type_())
                .or_default()
                .entry(owner_type.clone())
                .or_default() += count;
        }
    }

    let mut relation_iter = thing_manager.get_relations(&snapshot);
    while let Some(relation) = relation_iter.next() {
        let relation = relation.unwrap();
        statistics.total_relation_count += 1;
        *statistics.relation_counts.entry(relation.type_()).or_default() += 1;
        let owner_type = relation.type_().into_owned_object_type();
        let mut has_iter = relation.get_has_unordered(&snapshot, thing_manager);
        while let Some(has) = has_iter.next() {
            let (attribute, count) = has.unwrap();
            *statistics
                .has_attribute_counts
                .entry(owner_type.clone())
                .or_default()
                .entry(attribute.type_())
                .or_default() += count;
            *statistics
                .attribute_owner_counts
                .entry(attribute.type_())
                .or_default()
                .entry(owner_type.clone())
                .or_default() += count;
        }
        let mut relates_iter = relation.get_players(&snapshot, thing_manager);
        let mut this_relation_players = BTreeMap::<_, u64>::new();
        while let Some(relates) = relates_iter.next() {
            let (roleplayer, count) = relates.unwrap();
            let role = roleplayer.role_type();
            let player = roleplayer.player();
            *statistics.role_counts.entry(role.clone()).or_default() += count;
            *statistics.relation_role_counts.entry(relation.type_()).or_default().entry(role.clone()).or_default() +=
                count;
            *statistics.role_player_counts.entry(player.type_()).or_default().entry(role.clone()).or_default() += count;
            *statistics
                .relation_role_player_counts
                .entry(relation.type_())
                .or_default()
                .entry(role.clone())
                .or_default()
                .entry(player.type_())
                .or_default() += count;
            *statistics
                .player_role_relation_counts
                .entry(player.type_())
                .or_default()
                .entry(role.clone())
                .or_default()
                .entry(relation.type_())
                .or_default() += count;
            *this_relation_players.entry(player.type_()).or_default() += 1;
        }
        for (player_1, count_1) in &this_relation_players {
            for (player_2, count_2) in &this_relation_players {
                let link_count = if player_1 == player_2 { count_1 * (count_2 - 1) } else { count_1 * count_2 };
                if link_count == 0 {
                    continue;
                }
                *statistics
                    .links_index_counts
                    .entry(player_1.clone())
                    .or_default()
                    .entry(player_2.clone())
                    .or_default() += link_count;
            }
        }
    }

    let mut attribute_iter = thing_manager.get_attributes(&snapshot).unwrap();
    while let Some(attribute) = attribute_iter.next() {
        let attribute = attribute.unwrap();
        statistics.total_attribute_count += 1;
        *statistics.attribute_counts.entry(attribute.type_()).or_default() += 1;
    }

    statistics.total_thing_count =
        statistics.total_entity_count + statistics.total_relation_count + statistics.total_attribute_count;
    statistics.total_has_count = statistics.has_attribute_counts.values().map(|map| map.len() as u64).sum();
    statistics.total_role_count = statistics.role_counts.values().sum();

    statistics
}

#[test]
fn create_entity() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let person_label = Label::build("person");

    let mut snapshot = storage.clone().open_snapshot_schema();
    let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
    thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    let commit_sequence_number = snapshot.commit().unwrap().unwrap();

    let mut manually_tracked = Statistics::new(commit_sequence_number);
    manually_tracked.total_thing_count += 1;
    manually_tracked.total_entity_count += 1;
    *manually_tracked.entity_counts.entry(person_type).or_default() += 1;

    let mut synchronised = Statistics::new(SequenceNumber::MIN);
    synchronised.may_synchronise(&storage).unwrap();

    assert_statistics_eq!(synchronised, read_statistics(storage, &thing_manager));
}

#[test]
fn delete_twice() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let person_label = Label::build("person");

    let mut snapshot = storage.clone().open_snapshot_schema();
    let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
    let person = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    let create_commit_seq = snapshot.commit().unwrap().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write_at(create_commit_seq);
    person.clone().delete(&mut snapshot, &thing_manager).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    snapshot.commit().unwrap().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write_at(create_commit_seq);
    person.clone().delete(&mut snapshot, &thing_manager).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    snapshot.commit().unwrap().unwrap();

    let mut synchronised = Statistics::new(SequenceNumber::MIN);
    synchronised.may_synchronise(&storage).unwrap();

    assert_statistics_eq!(synchronised, read_statistics(storage, &thing_manager));
}

#[test]
fn put_has_twice() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let person_label = Label::build("person");
    let name_label = Label::build("name");

    let mut snapshot = storage.clone().open_snapshot_schema();
    let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &name_label).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type.clone(), Ordering::Unordered).unwrap();
    let person = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let name = thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String("alice".into())).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    let create_commit_seq = snapshot.commit().unwrap().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write_at(create_commit_seq);
    person.set_has_unordered(&mut snapshot, &thing_manager, name.as_reference()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    snapshot.commit().unwrap().unwrap();

    let mut synchronised = Statistics::new(SequenceNumber::MIN);
    synchronised.may_synchronise(&storage).unwrap();

    let mut snapshot = storage.clone().open_snapshot_write_at(create_commit_seq);
    person.set_has_unordered(&mut snapshot, &thing_manager, name.as_reference()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    snapshot.commit().unwrap_err(); // Can't concurrently modify the same 'has'

    synchronised.sequence_number = synchronised.sequence_number + 1;

    assert_statistics_eq!(synchronised, read_statistics(storage, &thing_manager));
}

#[test]
fn put_plays() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);

    let person_label = Label::build("person");
    let friendship_label = Label::build("friendship");
    let friend_role_name = "friend";

    let mut snapshot = storage.clone().open_snapshot_schema();
    let person_type = type_manager.create_entity_type(&mut snapshot, &person_label).unwrap();
    let friendship_type = type_manager.create_relation_type(&mut snapshot, &friendship_label).unwrap();
    let friend_relates = friendship_type
        .create_relates(&mut snapshot, &type_manager, &thing_manager, friend_role_name, Ordering::Unordered)
        .unwrap();
    let friend_role = friend_relates.role();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, friend_role.clone()).unwrap();
    friend_relates
        .set_annotation(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            RelatesAnnotation::Cardinality(AnnotationCardinality::new(1, Some(4))),
        )
        .unwrap();
    let person = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    let friendship = thing_manager.create_relation(&mut snapshot, friendship_type.clone()).unwrap();
    friendship.add_player(&mut snapshot, &thing_manager, friend_role.clone(), person.into_owned_object()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    let create_commit_seq = snapshot.commit().unwrap().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write_at(create_commit_seq);
    let person_2 = thing_manager.create_entity(&mut snapshot, person_type.clone()).unwrap();
    friendship.add_player(&mut snapshot, &thing_manager, friend_role.clone(), person_2.into_owned_object()).unwrap();
    thing_manager.finalise(&mut snapshot).unwrap();
    snapshot.commit().unwrap().unwrap();

    let mut synchronised = Statistics::new(SequenceNumber::MIN);
    synchronised.may_synchronise(&storage).unwrap();

    assert_statistics_eq!(synchronised, read_statistics(storage, &thing_manager));
}
