/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    array,
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
    thread,
    thread::{sleep, JoinHandle},
    time::{Duration, Instant},
};

use answer::variable_value::VariableValue;
use concept::{
    thing::thing_manager::ThingManager,
    type_::{type_manager::TypeManager, Ordering, OwnerAPI, PlayerAPI},
};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value_type::ValueType},
};
use executor::{pipeline::stage::StageIterator, ExecutionInterrupt};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{error::QueryError, query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, WritableSnapshot},
    MVCCStorage,
};
use test_utils::init_logging;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

static PERSON_LABEL: OnceLock<Label> = OnceLock::new();
static GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_MEMBER_LABEL: OnceLock<Label> = OnceLock::new();
static MEMBERSHIP_GROUP_LABEL: OnceLock<Label> = OnceLock::new();
static AGE_LABEL: OnceLock<Label> = OnceLock::new();
static NAME_LABEL: OnceLock<Label> = OnceLock::new();
fn setup_database(storage: &mut Arc<MVCCStorage<WALClient>>) {
    setup_concept_storage(storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot = storage.clone().open_snapshot_write();
    let person_type = type_manager.create_entity_type(&mut snapshot, PERSON_LABEL.get().unwrap()).unwrap();
    let group_type = type_manager.create_entity_type(&mut snapshot, GROUP_LABEL.get().unwrap()).unwrap();

    let membership_type = type_manager.create_relation_type(&mut snapshot, MEMBERSHIP_LABEL.get().unwrap()).unwrap();
    let relates_member = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_MEMBER_LABEL.get().unwrap().name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let membership_member_type = relates_member.role();
    let relates_group = membership_type
        .create_relates(
            &mut snapshot,
            &type_manager,
            &thing_manager,
            MEMBERSHIP_GROUP_LABEL.get().unwrap().name().as_str(),
            Ordering::Unordered,
        )
        .unwrap();
    let membership_group_type = relates_group.role();

    let age_type = type_manager.create_attribute_type(&mut snapshot, AGE_LABEL.get().unwrap()).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let name_type = type_manager.create_attribute_type(&mut snapshot, NAME_LABEL.get().unwrap()).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();

    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered).unwrap();
    person_type.set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered).unwrap();
    person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_member_type).unwrap();
    group_type.set_plays(&mut snapshot, &type_manager, &thing_manager, membership_group_type).unwrap();

    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

fn execute_insert<Snapshot: WritableSnapshot + 'static>(
    snapshot: Snapshot,
    type_manager: &TypeManager,
    thing_manager: Arc<ThingManager>,
    query_manager: QueryManager,
    query_str: &str,
) -> Result<(Vec<HashMap<String, VariableValue<'static>>>, Snapshot), (Box<QueryError>, Snapshot)> {
    let typeql_insert = typeql::parse_query(query_str).unwrap().into_pipeline();
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);

    let pipeline = query_manager
        .prepare_write_pipeline(snapshot, type_manager, thing_manager, &function_manager, &typeql_insert, query_str)
        .map_err(|(snapshot, err)| (err, snapshot))?;
    let outputs = pipeline.rows_positions().unwrap().clone();
    let (iter, ctx) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).map_err(|(typedb_source, ctx)| {
            (
                Box::new(QueryError::WritePipelineExecution { source_query: query_str.to_string(), typedb_source }),
                Arc::into_inner(ctx.snapshot).unwrap(),
            )
        })?;
    let batch = match iter.collect_owned() {
        Ok(batch) => batch,
        Err(typedb_source) => {
            return Err((
                Box::new(QueryError::WritePipelineExecution { source_query: query_str.to_string(), typedb_source }),
                Arc::into_inner(ctx.snapshot).unwrap(),
            ));
        }
    };
    let mut collected = Vec::with_capacity(batch.len());
    let mut row_iterator = batch.into_iterator();
    while let Some(row) = row_iterator.next() {
        let mut translated_row = HashMap::with_capacity(outputs.len());
        for (name, pos) in &outputs {
            translated_row.insert(name.clone(), row.get(*pos).clone().into_owned());
        }
        collected.push(translated_row);
    }
    Ok((collected, Arc::into_inner(ctx.snapshot).unwrap()))
}

fn multi_threaded_inserts() {
    PERSON_LABEL.set(Label::new_static("person")).unwrap();
    GROUP_LABEL.set(Label::new_static("group")).unwrap();
    MEMBERSHIP_LABEL.set(Label::new_static("membership")).unwrap();
    MEMBERSHIP_MEMBER_LABEL.set(Label::new_static_scoped("member", "membership", "membership:member")).unwrap();
    MEMBERSHIP_GROUP_LABEL.set(Label::new_static_scoped("group", "membership", "membership:group")).unwrap();
    AGE_LABEL.set(Label::new_static("age")).unwrap();
    NAME_LABEL.set(Label::new_static("name")).unwrap();
    init_logging();

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_database(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), Some(storage.snapshot_watermark()));
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    const NUM_THREADS: usize = 32;
    const INTERNAL_ITERS: usize = 1000;
    let start_signal_rw_lock = Arc::new(RwLock::new(()));
    let write_guard = start_signal_rw_lock.write().unwrap();
    let join_handles: [JoinHandle<()>; NUM_THREADS] = array::from_fn(|_| {
        let storage_cloned = storage.clone();
        let type_manager_cloned = type_manager.clone();
        let thing_manager_cloned = thing_manager.clone();
        let rw_lock_cloned = start_signal_rw_lock.clone();
        let query_manager_cloned = query_manager.clone();
        thread::spawn(move || {
            drop(rw_lock_cloned.read().unwrap());
            for _ in 0..INTERNAL_ITERS {
                let snapshot = storage_cloned.clone().open_snapshot_write();
                let age: u32 = rand::random();
                let (_row, snapshot) = execute_insert(
                    snapshot,
                    &type_manager_cloned,
                    thing_manager_cloned.clone(),
                    query_manager_cloned.clone(),
                    &format!("insert $p isa person, has age {age};"),
                )
                .unwrap();
                snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
            }
        })
    });
    println!("Sleeping 1s before starting threads");
    sleep(Duration::from_secs(1));
    println!("Start!");
    let start = Instant::now();
    drop(write_guard); // Start
    for join_handle in join_handles {
        join_handle.join().unwrap()
    }
    let time_taken_ms = start.elapsed().as_millis();
    println!(
        "{NUM_THREADS} threads * {INTERNAL_ITERS} iters took: {time_taken_ms} ms = {} inserts/s",
        (NUM_THREADS * INTERNAL_ITERS * 1000) / time_taken_ms as usize
    );

    {
        let snapshot = storage.clone().open_snapshot_read();
        let person_type = type_manager.get_entity_type(&snapshot, &Label::parse_from("person", None)).unwrap().unwrap();
        assert_eq!(
            NUM_THREADS * INTERNAL_ITERS,
            Iterator::count(thing_manager.get_entities_in(&snapshot, person_type, StorageCounters::DISABLED))
        );
        snapshot.close_resources();
    }
}

fn main() {
    multi_threaded_inserts();
}
