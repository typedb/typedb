/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs, path::Path, sync::Arc, thread};

use concept::thing::statistics::deltas::CommitDeltas;
use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, TransactionSchema, TransactionWrite},
};
use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::FsyncMetrics};
use durability::{DurabilityRecordType, DurabilitySequenceNumber, DurabilityService, wal::WAL};
use executor::ExecutionInterrupt;
use options::{MvccCleanupStrategy, QueryOptions, TransactionOptions, byte_size::ByteSize};
use query::given_rows::GivenRowsSimple;
use storage::{
    durability_client::{DurabilityClient, DurabilityRecord, WALClient},
    record::{CommitRecord, StatusRecord},
};
use test_utils::{create_tmp_storage_dir, init_logging};

const DB_NAME: &str = "stats-recovery";
const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name @key, owns age;
"#;

const NUM_THREADS: usize = 20;
const BATCHES_PER_THREAD: usize = 20;
const OPS_PER_BATCH: usize = 10;

#[test]
fn statistics_synchronization_under_concurrent_load() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let total_batches = NUM_THREADS * BATCHES_PER_THREAD;
    let total_persons = total_batches * OPS_PER_BATCH;
    // Each person is given a unique name and a unique age, so attributes never
    // dedupe across writers and ground-truth counts come straight from the configs.
    let total_attributes = 2 * total_persons;
    let total_has = 2 * total_persons;

    {
        let (_dbm, database) = create_db(&tmp_dir);

        let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
        let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
        let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
        result.unwrap();
        let (mut profile, intent) = tx.finalise();
        intent.unwrap().commit(profile.commit_profile()).unwrap();

        let mut handles = Vec::with_capacity(NUM_THREADS);
        for thread_id in 0..NUM_THREADS {
            let database = database.clone();
            let handle = thread::spawn(move || {
                for local_batch_id in 0..BATCHES_PER_THREAD {
                    let batch_id = thread_id * BATCHES_PER_THREAD + local_batch_id;
                    run_insert_batch(&database, batch_id);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
    }
    // dbm and database dropped here; IntervalRunner threads shut down synchronously on drop.

    let (_dbm, database) = open_db(&tmp_dir);
    let metrics = database.get_metrics();

    assert_eq!(metrics.data.entity_count, total_persons as u64, "entity_count after reboot");
    assert_eq!(metrics.data.attribute_count, total_attributes as u64, "attribute_count after reboot");
    assert_eq!(metrics.data.has_count, total_has as u64, "has_count after reboot");
    assert_eq!(metrics.data.relation_count, 0, "relation_count after reboot");
}

#[test]
fn test_wal_contains_only_commit_records() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let (dbm, database) = create_db(&tmp_dir);

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    run_insert_batch(&database, 0);
    drop(database);
    drop(dbm);

    replay_wal_with_only(&tmp_dir.join(DB_NAME), &[CommitRecord::RECORD_TYPE], &[StatusRecord::RECORD_TYPE]);

    let (_dbm, database) = open_db(&tmp_dir);
    let metrics = database.get_metrics();
    assert_eq!(metrics.data.entity_count, OPS_PER_BATCH as u64, "entity_count after reboot (only commits)");
}

#[test]
fn test_wal_commit_and_delta_records() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let (dbm, database) = create_db(&tmp_dir);

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    run_insert_batch(&database, 0);
    drop(database);
    drop(dbm);

    replay_wal_with_only(
        &tmp_dir.join(DB_NAME),
        &[CommitRecord::RECORD_TYPE],
        &[StatusRecord::RECORD_TYPE, CommitDeltas::RECORD_TYPE],
    );

    let (_dbm, database) = open_db(&tmp_dir);
    let metrics = database.get_metrics();
    assert_eq!(metrics.data.entity_count, OPS_PER_BATCH as u64, "entity_count after reboot (commits + deltas)");
}

#[test]
fn test_wal_commits_then_deltas() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let (dbm, database) = create_db(&tmp_dir);

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    run_insert_batch(&database, 0);
    drop(database);
    drop(dbm);

    replay_wal_with_only(&tmp_dir.join(DB_NAME), &[CommitRecord::RECORD_TYPE], &[StatusRecord::RECORD_TYPE]);

    let (dbm, database) = open_db(&tmp_dir);
    run_insert_batch(&database, 1);
    drop(database);
    drop(dbm);

    replay_wal_with_only(
        &tmp_dir.join(DB_NAME),
        &[CommitRecord::RECORD_TYPE],
        &[StatusRecord::RECORD_TYPE, CommitDeltas::RECORD_TYPE],
    );

    let (_dbm, database) = open_db(&tmp_dir);
    let metrics = database.get_metrics();
    assert_eq!(
        metrics.data.entity_count,
        (OPS_PER_BATCH * 2) as u64,
        "entity_count after reboot (commits then deltas)"
    );
}

#[test]
fn test_wal_missing_middle_deltas() {
    init_logging();
    let tmp_dir = create_tmp_storage_dir();
    let (dbm, database) = create_db(&tmp_dir);

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();

    run_insert_batch(&database, 0);
    run_insert_batch(&database, 1);
    run_insert_batch(&database, 2);
    drop(database);
    drop(dbm);

    replay_wal_with_only(
        &tmp_dir.join(DB_NAME),
        &[CommitRecord::RECORD_TYPE],
        &[StatusRecord::RECORD_TYPE, CommitDeltas::RECORD_TYPE],
    );
    remove_from_wal(&tmp_dir.join(DB_NAME), DurabilitySequenceNumber::new(3), CommitDeltas::RECORD_TYPE);

    let (_dbm, database) = open_db(&tmp_dir);
    let metrics = database.get_metrics();
    assert_eq!(
        metrics.data.entity_count,
        (OPS_PER_BATCH * 3) as u64,
        "entity_count after reboot (missing middle deltas)"
    );
}

fn create_db(storage_dir: &Path) -> (Arc<DatabaseManager>, Arc<Database<WALClient>>) {
    let dbm = DatabaseManager::new(
        storage_dir,
        Arc::new(DiagnosticsManager::new_disabled()),
        ByteSize::mb(64),
        ByteSize::mb(64),
        database::database_manager::ImportOwnership::Exclusive,
        MvccCleanupStrategy::Disabled,
    )
    .unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();
    (dbm, database)
}

fn open_db(storage_dir: &Path) -> (Arc<DatabaseManager>, Arc<Database<WALClient>>) {
    let dbm = DatabaseManager::new(
        storage_dir,
        Arc::new(DiagnosticsManager::new_disabled()),
        ByteSize::mb(64),
        ByteSize::mb(64),
        database::database_manager::ImportOwnership::Exclusive,
        MvccCleanupStrategy::Disabled,
    )
    .unwrap();
    let database = dbm.database(DB_NAME).unwrap();
    (dbm, database)
}

fn run_insert_batch(database: &Arc<Database<WALClient>>, batch_id: usize) {
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    for i in 0..OPS_PER_BATCH {
        let id = batch_id * OPS_PER_BATCH + i;
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
    }
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

fn replay_wal_with_only(
    db_dir: &Path,
    sequenced_record_types: &[DurabilityRecordType],
    unsequenced_record_types: &[DurabilityRecordType],
) {
    let source_wal = WALClient::new(WAL::load(db_dir, FsyncMetrics::disabled()).unwrap());

    let tmp_dir = create_tmp_storage_dir();
    fs::create_dir_all(tmp_dir.join("wal")).unwrap();
    let mut target_wal = WAL::load(&tmp_dir, FsyncMetrics::disabled()).unwrap();

    for &record_type in std::iter::chain(sequenced_record_types, unsequenced_record_types) {
        target_wal.register_record_type(record_type, &format!("<record {record_type}>"));
    }

    for record in source_wal.iter_from(DurabilitySequenceNumber::new(0)).unwrap() {
        let record = record.unwrap();
        if sequenced_record_types.contains(&record.record_type) {
            target_wal.sequenced_write(record.record_type, &record.bytes).unwrap();
        }
        if unsequenced_record_types.contains(&record.record_type) {
            target_wal.unsequenced_write(record.record_type, &record.bytes).unwrap();
        }
    }

    drop(source_wal);
    drop(target_wal);

    fs::remove_dir_all(db_dir.join(WAL::WAL_DIR_NAME)).unwrap();
    fs::rename(tmp_dir.join(WAL::WAL_DIR_NAME), db_dir.join(WAL::WAL_DIR_NAME)).unwrap();
}

fn remove_from_wal(db_dir: &Path, sequence_number: DurabilitySequenceNumber, record_type: DurabilityRecordType) {
    let source_wal = WALClient::new(WAL::load(db_dir, FsyncMetrics::disabled()).unwrap());

    let tmp_dir = create_tmp_storage_dir();
    fs::create_dir_all(tmp_dir.join("wal")).unwrap();
    let mut target_wal = WAL::load(&tmp_dir, FsyncMetrics::disabled()).unwrap();

    let mut prev = DurabilitySequenceNumber::new(0);
    for record in source_wal.iter_from(DurabilitySequenceNumber::new(0)).unwrap() {
        let record = record.unwrap();
        target_wal.register_record_type(record.record_type, &format!("<record {record_type}>"));
        if record.record_type == record_type && record.sequence_number == sequence_number {
            continue;
        }
        if record.sequence_number == prev {
            target_wal.unsequenced_write(record.record_type, &record.bytes).unwrap();
        } else {
            target_wal.sequenced_write(record.record_type, &record.bytes).unwrap();
        }
        prev = record.sequence_number
    }

    drop(source_wal);
    drop(target_wal);

    fs::remove_dir_all(db_dir.join(WAL::WAL_DIR_NAME)).unwrap();
    fs::rename(tmp_dir.join(WAL::WAL_DIR_NAME), db_dir.join(WAL::WAL_DIR_NAME)).unwrap();
}
