/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc, thread};

use database::{
    Database,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CleanupRecord, CommitIntent, TransactionSchema, TransactionWrite},
};
use diagnostics::metrics::FsyncMetrics;
use durability::{DurabilityService, wal::WAL};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use query::given_rows::GivenRowsSimple;
use storage::{
    durability_client::{DurabilityRecord, WALClient},
    record::CommitRecord,
    sequence_number::SequenceNumber,
};
use test_utils::{create_tmp_storage_dir, init_logging};
use test_utils_database::create_db;

const DB_NAME: &str = "cleanup-records";
const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name @key, owns age;
"#;

const NUM_THREADS: u32 = 20;
const WRITES_PER_THREAD: u32 = 20;

#[test]
fn statistics_synchronization_under_concurrent_load() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    {
        let (_dbm, database) = create_db(&tmp_dir, DB_NAME);

        let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
        let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
        let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
        result.unwrap();
        let (mut profile, intent) = tx.finalise();
        intent.unwrap().commit(profile.commit_profile()).unwrap();

        let mut handles = Vec::new();
        for thread_id in 0..NUM_THREADS {
            let database = database.clone();
            let handle = thread::spawn(move || {
                for write in 0..WRITES_PER_THREAD {
                    let id = thread_id * WRITES_PER_THREAD + write;
                    run_insert(&database, id);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        Commit,
        Both,
    }
    let wal = WAL::load(tmp_dir.join(DB_NAME), FsyncMetrics::new()).unwrap();
    let mut scratch = HashMap::new();
    for item in wal.iter_any_from(SequenceNumber::MIN).unwrap() {
        let raw_record = item.unwrap();
        let sequence_number = raw_record.sequence_number;
        if raw_record.record_type == CommitRecord::RECORD_TYPE {
            if scratch.insert(sequence_number, Seen::Commit).is_some() {
                panic!("Saw a commit record {} twice", sequence_number.number())
            }
        } else if raw_record.record_type == CleanupRecord::RECORD_TYPE {
            let sequence_number = CleanupRecord::deserialise_from(&mut &*raw_record.bytes).unwrap().sequence_number;
            if let Some(flag) = scratch.get_mut(&sequence_number) {
                assert_eq!(*flag, Seen::Commit, "Saw a cleanup record for {} twice", sequence_number.number());
                *flag = Seen::Both;
            } else {
                panic!("Saw a cleanup record for {} before its commit record", sequence_number.number())
            }
        }
    }
}

fn run_insert(database: &Arc<Database<WALClient>>, id: u32) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let query_str = format!(r#"insert $p isa person, has name "person_{id}", has age {id};"#);
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
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}
