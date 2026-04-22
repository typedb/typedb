/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use executor::ExecutionInterrupt;
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::CommitProfile;
use storage::snapshot::CommittableSnapshot;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn setup_and_run(schema: &str, insert: &str, match_query: &str) -> usize {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);

    // Schema
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));

    // Insert
    let snapshot = storage.clone().open_snapshot_write();
    let insert_pipeline = typeql::parse_query(insert).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_write_pipeline(
            snapshot,
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &insert_pipeline,
            insert,
        )
        .unwrap();
    let (_iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    Arc::into_inner(context.snapshot).unwrap().commit(&mut CommitProfile::DISABLED).unwrap();

    // Match
    let snapshot = Arc::new(storage.clone().open_snapshot_read());
    let match_pipeline = typeql::parse_query(match_query).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot,
            &type_manager,
            thing_manager,
            &function_manager,
            &match_pipeline,
            match_query,
        )
        .unwrap();
    let (iterator, _) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect::<Result<Vec<_>, _>>().unwrap().len()
}

#[test]
fn links_relation_is_own_player() {
    // match $r links (member: $r);
    // Only loop_self (which links itself as member) should match.
    // loop_other links loop_self as member, so it should NOT match.
    let count = setup_and_run(
        r#"define
            relation loop relates member @card(0..);
            loop plays loop:member;
        "#,
        r#"insert
            $loop_self isa loop;
            $loop_other isa loop;
            $loop_self links (member: $loop_self);
            $loop_other links (member: $loop_self);
        "#,
        "match $r links (member: $r);",
    );
    assert_eq!(count, 1);
}

#[test]
fn indexed_relation_same_player_both_roles() {
    // match $casting links (actor: $person, character: $person);
    // casting_same (person_c plays both actor and character) should match.
    // casting_mixed (person_a as actor, person_b as character) should NOT match.
    let count = setup_and_run(
        r#"define
            entity person plays casting:actor, plays casting:character;
            relation casting relates actor, relates character;
        "#,
        r#"insert
            $person_a isa person;
            $person_b isa person;
            $person_c isa person;
            $casting_mixed isa casting, links (actor: $person_a, character: $person_b);
            $casting_same  isa casting, links (actor: $person_c, character: $person_c);
        "#,
        "match $casting links (actor: $person, character: $person);",
    );
    assert_eq!(count, 1);
}
