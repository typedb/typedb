/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionSchema, TransactionWrite},
};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, init_logging};

const DB_NAME: &str = "stats-recovery";
const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name @key, owns age;
"#;

const NUM_THREADS: usize = 8;
const BATCHES_PER_THREAD: usize = 20;
const OPS_PER_BATCH: usize = 100;

#[test]
fn statistics_synchronization_under_concurrent_load() {
    init_logging();
    let tmp_dir = create_tmp_dir();
    let total_batches = NUM_THREADS * BATCHES_PER_THREAD;
    let total_persons = total_batches * OPS_PER_BATCH;

    let metrics_before_reboot = {
        let dbm = DatabaseManager::new(&tmp_dir).unwrap();
        dbm.put_database(DB_NAME).unwrap();
        let database = dbm.database(DB_NAME).unwrap();

        let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
        let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
        let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
        result.unwrap();
        tx.commit().1.unwrap();

        let next_batch = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let database = database.clone();
                let next_batch = next_batch.clone();
                thread::spawn(move || {
                    loop {
                        let batch_id = next_batch.fetch_add(1, Ordering::Relaxed);
                        if batch_id >= total_batches {
                            break;
                        }
                        run_insert_batch(&database, batch_id);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Let the in-process statistics updater catch up before sampling pre-reboot stats.
        thread::sleep(Duration::from_millis(500));
        database.get_metrics()
    };

    // dbm and database dropped here; IntervalRunner threads shut down synchronously on drop.

    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    let database = dbm.database(DB_NAME).unwrap();
    let metrics_after_reboot = database.get_metrics();

    assert_eq!(
        metrics_after_reboot.data.entity_count, total_persons as u64,
        "post-reboot entity_count does not match the bulk-loaded persons"
    );
    assert_eq!(
        metrics_after_reboot.data.relation_count, 0,
        "post-reboot relation_count is non-zero but the schema has no relations"
    );

    assert_eq!(
        metrics_after_reboot.data.entity_count, metrics_before_reboot.data.entity_count,
        "entity_count changed across reboot"
    );
    assert_eq!(
        metrics_after_reboot.data.attribute_count, metrics_before_reboot.data.attribute_count,
        "attribute_count changed across reboot"
    );
    assert_eq!(
        metrics_after_reboot.data.has_count, metrics_before_reboot.data.has_count,
        "has_count changed across reboot"
    );
    assert_eq!(
        metrics_after_reboot.data.relation_count, metrics_before_reboot.data.relation_count,
        "relation_count changed across reboot"
    );
}

fn run_insert_batch(database: &Arc<Database<WALClient>>, batch_id: usize) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    for i in 0..OPS_PER_BATCH {
        let id = batch_id * OPS_PER_BATCH + i;
        let age = (id % 100) as u32;
        let query_str = format!(r#"insert $p isa person, has name "person_{id}", has age {age};"#);
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            pipeline,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    tx.commit().1.unwrap();
}
