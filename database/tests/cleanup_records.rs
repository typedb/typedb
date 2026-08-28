/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc, thread};

use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CleanupRecord, CommitIntent, TransactionSchema, TransactionWrite},
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::FsyncMetrics};
use durability::{DurabilityService, wal::WAL};
use executor::ExecutionInterrupt;
use options::{MvccCleanupStrategy, QueryOptions, TransactionOptions, byte_size::ByteSize};
use query::given_rows::GivenRowsSimple;
use storage::{
    durability_client::{DurabilityRecord, WALClient},
    record::CommitRecord,
    sequence_number::SequenceNumber,
};
use test_utils::{create_tmp_storage_dir, init_logging};

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
        let dbm = DatabaseManager::new(
            &tmp_dir,
            Arc::new(DiagnosticsManager::new_disabled()),
            ByteSize::mb(64),
            ByteSize::mb(64),
            database::database_manager::ImportOwnership::Exclusive,
            MvccCleanupStrategy::Disabled,
        )
        .unwrap();
        dbm.put_database(DB_NAME).unwrap();
        let database = dbm.database(DB_NAME).unwrap();

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
    enum Flag {
        SeenCommit,
        SeenBoth,
    }
    let wal = WAL::load(tmp_dir.join(DB_NAME), FsyncMetrics::new()).unwrap();
    let mut scratch = HashMap::new();
    for item in wal.iter_any_from(SequenceNumber::MIN).unwrap() {
        let raw_record = item.unwrap();
        let sequence_number = raw_record.sequence_number;
        if raw_record.record_type == CommitRecord::RECORD_TYPE {
            if scratch.insert(sequence_number, Flag::SeenCommit).is_some() {
                panic!("Saw a commit record {} twice", sequence_number.number())
            }
        } else if raw_record.record_type == CleanupRecord::RECORD_TYPE {
            if let Some(flag) = scratch.get_mut(&sequence_number) {
                assert_eq!(*flag, Flag::SeenCommit, "Saw a cleanup record for {} twice", sequence_number.number());
                *flag = Flag::SeenBoth;
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
