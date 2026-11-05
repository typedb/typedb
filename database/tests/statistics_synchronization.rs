/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{sync::Arc, thread};

use concept::thing::statistics::Statistics;
use database::{
    Database,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, TransactionRead, TransactionSchema, TransactionWrite},
};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use query::given_rows::GivenRowsSimple;
use resource::profile::StorageCounters;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_database::{create_db, manager, open_db};

const DB_NAME: &str = "stats-sync";
const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name, owns age @card(0..);
    attribute blah @independent, value string;
"#;

const NUM_THREADS: usize = 20;
const BATCHES_PER_THREAD: usize = 20;
const OPS_PER_BATCH: usize = 10;

#[test]
fn statistics_synchronization_under_concurrent_load() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();

    {
        let (_, database) = create_db(&tmp_dir, DB_NAME);

        let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
        let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
        let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
        result.unwrap();
        let (mut profile, intent) = tx.finalise();
        intent.unwrap().commit(profile.commit_profile()).unwrap();

        let mut handles = Vec::with_capacity(NUM_THREADS);
        for _ in 0..NUM_THREADS {
            let database = database.clone();
            let handle = thread::spawn(move || {
                for local_batch_id in 0..BATCHES_PER_THREAD {
                    let batch_id = local_batch_id;
                    run_insert_batch(&database, batch_id);
                    run_delete_batch(&database, batch_id);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // dbm and database dropped here; IntervalRunner threads shut down synchronously on drop.

    let (_dbm, database) = open_db(&tmp_dir, DB_NAME);
    let metrics = database.get_metrics();
    let tx = TransactionRead::open(database, <_>::default()).unwrap();
    let statistics = Statistics::read(&*tx.snapshot, &tx.thing_manager, StorageCounters::DISABLED).unwrap();

    assert_eq!(metrics.data.entity_count, statistics.total_entity_count, "entity_count after reboot");
    assert_eq!(metrics.data.attribute_count, statistics.total_attribute_count, "attribute_count after reboot");
    assert_eq!(metrics.data.has_count, statistics.total_has_count, "has_count after reboot");
    assert_eq!(metrics.data.relation_count, 0, "relation_count after reboot");
}

#[test]
fn finalised_import_keeps_statistics_after_reopen() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let total_persons = BATCHES_PER_THREAD * OPS_PER_BATCH;

    {
        let dbm = manager(&tmp_dir);
        let staged = dbm.prepare_imported_database(DB_NAME.to_string()).unwrap();

        let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
        let tx = TransactionSchema::open(staged.clone(), TransactionOptions::default()).unwrap();
        let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
        result.unwrap();
        let (mut profile, intent) = tx.finalise();
        intent.unwrap().commit(profile.commit_profile()).unwrap();

        for batch_id in 0..BATCHES_PER_THREAD {
            run_insert_batch(&staged, batch_id);
        }
        drop(staged);
        dbm.finalise_imported_database(DB_NAME).unwrap();

        let metrics = dbm.database(DB_NAME).unwrap().get_metrics();
        assert_eq!(metrics.data.entity_count, total_persons as u64, "entity_count after finalise");
        assert_eq!(metrics.data.has_count, 2 * total_persons as u64, "has_count after finalise");
    }

    let (_dbm, database) = open_db(&tmp_dir, DB_NAME);
    let metrics = database.get_metrics();
    assert_eq!(metrics.data.entity_count, total_persons as u64, "entity_count after reboot");
    assert_eq!(metrics.data.has_count, 2 * total_persons as u64, "has_count after reboot");
}

fn run_insert_batch(database: &Arc<Database<WALClient>>, batch_id: usize) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    for i in 0..OPS_PER_BATCH {
        let id = batch_id * OPS_PER_BATCH + i;
        let query_str = format!(r#"insert $p isa person, has name "person_{id}", has age {id}; $_ isa blah "{id}";"#);
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            Arc::new(pipeline),
            None::<GivenRowsSimple>,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

fn run_delete_batch(database: &Arc<Database<WALClient>>, batch_id: usize) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    for i in 0..OPS_PER_BATCH {
        let id = batch_id * OPS_PER_BATCH + i;
        let query_str = format!(r#"match $p isa blah "{id}"; delete $p;"#);
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            Arc::new(pipeline),
            None::<GivenRowsSimple>,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}
