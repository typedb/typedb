/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

// A write snapshot reuses the RocksDB iterators it creates on first use. A RocksDB iterator only sees data up to its
// creation, so entries committed afterwards are still walked over, one by one, by every later seek that does not stop
// at a visible key first. Role player lookups (`get_role_players`) seek with a prefix shorter than the links keyspace's
// optimised prefix, so they use an unbounded iterator and can walk over everything committed since the snapshot's
// first lookup. With concurrent writers this makes relation inserts and commit validation take seconds.

use std::{sync::Arc, time::Instant};

use concept::{
    thing::{entity::Entity, object::Object, thing_manager::ThingManager},
    type_::{Ordering, PlayerAPI, relation_type::RelationType, role_type::RoleType},
};
use encoding::value::label::Label;
use resource::profile::{CommitProfile, StorageCounters};
use rocksdb::{PerfContext, PerfMetric, PerfStatsLevel, perf::set_perf_stats};
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WriteSnapshot},
};
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PLAYERS: usize = 100;
const CONCURRENT_RELATIONS: usize = 10_000;
const COMMIT_BATCH: usize = 1_000;
const MEASURED_RELATIONS: usize = 10;
const MAX_SKIPPED_ENTRIES: u64 = 1_000;

#[derive(Clone, Copy)]
struct Schema {
    friendship: RelationType,
    role_a: RoleType,
    role_b: RoleType,
}

fn create_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_schema();
    let person = type_manager.create_entity_type(&mut snapshot, &Label::build("person", None)).unwrap();
    let friendship = type_manager.create_relation_type(&mut snapshot, &Label::build("friendship", None)).unwrap();
    for role in ["a", "b"] {
        let relates = friendship
            .create_relates(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                role,
                Ordering::Unordered,
                StorageCounters::DISABLED,
            )
            .unwrap();
        person
            .set_plays(&mut snapshot, &type_manager, &thing_manager, relates.role(), StorageCounters::DISABLED)
            .unwrap();
    }
    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

fn add_friendship(
    snapshot: &mut WriteSnapshot<WALClient>,
    thing_manager: &ThingManager,
    schema: Schema,
    players: &[Entity],
    index: usize,
) {
    let relation = thing_manager.create_relation(snapshot, schema.friendship).unwrap();
    for (offset, role) in [schema.role_a, schema.role_b].into_iter().enumerate() {
        let player = players[(index + offset) % players.len()];
        relation.add_player(snapshot, thing_manager, role, Object::Entity(player), StorageCounters::DISABLED).unwrap();
    }
}

/// Inserts relations into `snapshot` and finalises it, returning how many entries RocksDB skipped because they were
/// committed after the snapshot's iterators were created.
fn skipped_entries_inserting_relations(
    snapshot: &mut WriteSnapshot<WALClient>,
    thing_manager: &ThingManager,
    schema: Schema,
    players: &[Entity],
) -> u64 {
    set_perf_stats(PerfStatsLevel::EnableCount);
    let mut perf_context = PerfContext::default();
    perf_context.reset();
    for index in 0..MEASURED_RELATIONS {
        add_friendship(snapshot, thing_manager, schema, players, index);
    }
    thing_manager.finalise(snapshot, StorageCounters::DISABLED).unwrap();
    let skipped = perf_context.metric(PerfMetric::InternalRecentSkippedCount);
    set_perf_stats(PerfStatsLevel::Disable);
    skipped
}

#[test]
#[ignore = "reproduces the stale pooled iterator stall; enable once snapshot iterators are refreshed or bounded"]
fn relation_writes_do_not_scan_entries_committed_after_the_snapshot_first_read() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    create_schema(storage.clone());
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.snapshot_watermark()));

    let schema = {
        let snapshot = storage.clone().open_snapshot_read();
        let friendship = type_manager.get_relation_type(&snapshot, &Label::build("friendship", None)).unwrap().unwrap();
        let role_a = friendship.get_relates_role_name(&snapshot, &type_manager, "a").unwrap().unwrap().role();
        let role_b = friendship.get_relates_role_name(&snapshot, &type_manager, "b").unwrap().unwrap().role();
        Schema { friendship, role_a, role_b }
    };
    let person = type_manager.get_entity_type(&storage.clone().open_snapshot_read(), &Label::build("person", None));
    let person = person.unwrap().unwrap();

    let players: Vec<Entity> = {
        let mut snapshot = storage.clone().open_snapshot_write();
        let players = (0..PLAYERS).map(|_| thing_manager.create_entity(&mut snapshot, person).unwrap()).collect();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
        players
    };

    // The first role player lookup creates this snapshot's pooled iterator over the links keyspace.
    let mut early_snapshot = storage.clone().open_snapshot_write();
    add_friendship(&mut early_snapshot, &thing_manager, schema, &players, 0);

    // Other transactions commit relations after that iterator was created.
    for batch in 0..CONCURRENT_RELATIONS / COMMIT_BATCH {
        let mut snapshot = storage.clone().open_snapshot_write();
        for index in 0..COMMIT_BATCH {
            add_friendship(&mut snapshot, &thing_manager, schema, &players, batch * COMMIT_BATCH + index);
        }
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap();
        snapshot.commit(&mut CommitProfile::disabled()).unwrap();
    }

    // Control: a snapshot opened after those commits creates its iterators afterwards and skips nothing.
    let mut late_snapshot = storage.clone().open_snapshot_write();
    let late_start = Instant::now();
    let late_skipped = skipped_entries_inserting_relations(&mut late_snapshot, &thing_manager, schema, &players);
    let late_elapsed = late_start.elapsed();

    let early_start = Instant::now();
    let early_skipped = skipped_entries_inserting_relations(&mut early_snapshot, &thing_manager, schema, &players);
    let early_elapsed = early_start.elapsed();

    let summary = format!(
        "inserting {MEASURED_RELATIONS} relations and finalising after {CONCURRENT_RELATIONS} relations were committed \
         concurrently: snapshot opened before those commits skipped {early_skipped} newer entries in {early_elapsed:?}; \
         snapshot opened after them skipped {late_skipped} in {late_elapsed:?}"
    );
    assert!(late_skipped <= MAX_SKIPPED_ENTRIES, "control snapshot unexpectedly skipped entries: {summary}");
    assert!(early_skipped <= MAX_SKIPPED_ENTRIES, "stale pooled iterators walked over newer commits: {summary}");
}
