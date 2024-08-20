/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::{thing::statistics::Statistics, type_::type_manager::TypeManager};
use query::query_manager::QueryManager;
use storage::{
    durability_client::WALClient, sequence_number::SequenceNumber, snapshot::CommittableSnapshot, MVCCStorage,
};

use crate::common::{load_managers, setup_storage};

mod common;

fn define_schema(storage: &Arc<MVCCStorage<WALClient>>, type_manager: &TypeManager) {
    let mut snapshot = storage.clone().open_snapshot_schema();
    let query_manager = QueryManager::new();

    let query_str = r#"
    define
    attribute name value string;
    relation friendship relates friend;
    entity person owns name, plays friendship:friend;
    "#;
    let schema_query = typeql::parse_query(query_str).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, schema_query).unwrap();
    snapshot.commit().unwrap();
}

#[test]
fn insert_match_insert_pipeline() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, _thing_manager, function_manager) = load_managers(storage.clone());
    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    define_schema(&storage, &type_manager);
    let query_manager = QueryManager::new();
    let mut snapshot = storage.clone().open_snapshot_write();
    let query = typeql::parse_query(
        r#"
        insert
            $p1 isa person, has name "John";
            $p2 isa person, has name "James";
        match
            $p_either isa person; $n isa name;
        insert
             $p_either has $n;
    "#,
    )
    .unwrap()
    .into_pipeline();
    query_manager
        .execute_pipeline(
            &mut snapshot,
            &type_manager,
            &function_manager,
            &statistics,
            &IndexedAnnotatedFunctions::empty(),
            &query,
        )
        .unwrap()
}
#[test]
fn insert_insert_pipeline() {
    let (_tmp_dir, storage) = setup_storage();
    let (type_manager, _thing_manager, function_manager) = load_managers(storage.clone());
    let mut statistics = Statistics::new(SequenceNumber::new(0));
    statistics.may_synchronise(&storage).unwrap();

    define_schema(&storage, &type_manager);
    let query_manager = QueryManager::new();
    let mut snapshot = storage.clone().open_snapshot_write();
    let query = typeql::parse_query(
        r#"
        insert
            $p1 isa person, has name "John";
            $p2 isa person, has name "James";
        insert
            (friend: $p1, friend: $p2) isa friendship;
    "#,
    )
    .unwrap()
    .into_pipeline();
    query_manager
        .execute_pipeline(
            &mut snapshot,
            &type_manager,
            &function_manager,
            &statistics,
            &IndexedAnnotatedFunctions::empty(),
            &query,
        )
        .unwrap()
}
