/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Concurrency / isolation regression tests.
//!
//! Each test opens two write transactions whose snapshots were taken before either commits, runs
//! conflicting (or deliberately non-conflicting) writes in each, then commits both and asserts on
//! the outcomes. This is the canonical pattern for exercising commit-time isolation locks: lock-key
//! contention surfaces only when both transactions reach commit with overlapping write sets, so the
//! ordering of `open / write / write / commit / commit` matters.

use std::sync::Arc;

use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, DataCommitError, TransactionSchema, TransactionWrite},
};
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use storage::durability_client::WALClient;
use test_utils::{TempDir, init_logging};

const DB_NAME: &str = "isolation-test";

fn fresh_database() -> (TempDir, Arc<Database<WALClient>>) {
    init_logging();
    let tmp_dir = test_utils::create_tmp_storage_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();
    (tmp_dir, database)
}

fn commit_schema(database: Arc<Database<WALClient>>, schema: &str) {
    let parsed = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database, TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, parsed, schema.to_string());
    result.unwrap();
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

fn commit_write_query(database: Arc<Database<WALClient>>, query: &str) {
    let mut tx = TransactionWrite::open(database, TransactionOptions::default()).unwrap();
    tx = run_write(tx, query);
    let (mut profile, intent) = tx.finalise();
    intent.unwrap().commit(profile.commit_profile()).unwrap();
}

fn open_write(database: Arc<Database<WALClient>>) -> TransactionWrite<WALClient> {
    TransactionWrite::open(database, TransactionOptions::default()).unwrap()
}

fn run_write(tx: TransactionWrite<WALClient>, query: &str) -> TransactionWrite<WALClient> {
    let pipeline = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let (tx, result) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        pipeline,
        query.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    result.unwrap_or_else(|err| panic!("write query failed: {query:?}: {err:?}"));
    tx
}

fn try_commit(tx: TransactionWrite<WALClient>) -> Result<(), DataCommitError> {
    let (mut profile, intent) = tx.finalise();
    intent.and_then(|intent| intent.commit(profile.commit_profile()))
}

/// Assert exactly one of two commit outcomes succeeded and the other failed. Returns the failure
/// for the caller to inspect.
fn assert_exactly_one_failed(a: Result<(), DataCommitError>, b: Result<(), DataCommitError>) -> DataCommitError {
    match (a, b) {
        (Ok(()), Err(err)) | (Err(err), Ok(())) => err,
        (Ok(()), Ok(())) => panic!("expected one commit to fail with isolation conflict; both succeeded"),
        (Err(a), Err(b)) => panic!("expected exactly one commit to fail; both failed: a={a:?} b={b:?}"),
    }
}

fn assert_isolation_conflict(err: &DataCommitError) {
    let formatted = format!("{err:?}");
    assert!(
        formatted.contains("Isolation") || formatted.contains("isolation conflict"),
        "expected an isolation conflict error, got: {formatted}"
    );
}

//////////////////////////////////////////
// Concurrent insert/delete of the same //
// identical entity and relation        //
//////////////////////////////////////////

#[test]
fn concurrent_inserts_of_identical_type_entity_and_relation_both_succeed() {
    // Inserts of fresh entities/relations of identical type from concurrent transactions both
    // succeed: each transaction allocates its own IIDs, so the lock keys never overlap.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person;
            relation friendship, relates friend @card(0..);
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, "insert $p isa person; $f isa friendship;");
    tx2 = run_write(tx2, "insert $p isa person; $f isa friendship;");

    try_commit(tx1).expect("tx1 commit");
    try_commit(tx2).expect("tx2 commit");
}

#[test]
fn concurrent_deletes_of_identical_entity_one_fails() {
    let (_tmp, database) = fresh_database();
    commit_schema(database.clone(), "define entity person, owns id @key; attribute id, value integer;");
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; delete $p;"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; delete $p;"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}

#[test]
fn concurrent_deletes_of_identical_relation_one_fails() {
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, plays friendship:friend, owns id @key;
            relation friendship, relates friend @card(0..), owns rid @key;
            attribute id, value integer;
            attribute rid, value integer;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $a isa person, has id 1;
            $b isa person, has id 2;
            $f isa friendship, links (friend: $a, friend: $b), has rid 100;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $f isa friendship, has rid 100; delete $f;"#);
    tx2 = run_write(tx2, r#"match $f isa friendship, has rid 100; delete $f;"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}

/////////////////////////////////
// Concurrent owns cardinality //
/////////////////////////////////

#[test]
fn concurrent_has_writes_violating_owns_cardinality_one_fails() {
    // Owner cardinality is 0..1 for `name`. Two concurrent has-writes from different snapshots,
    // each adding a name to the same person, would push the owner to 2 names if both committed -
    // exceeding the upper bound. The cardinality commit lock must serialise them and reject one.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..1);
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; insert $p has name "alice";"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; insert $p has name "bob";"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}

#[test]
fn concurrent_has_writes_with_bounded_owns_cardinality_always_contend_even_when_within_bound() {
    // Owner cardinality is 0..2: combined writes would stay within the bound, so the constraint
    // is not actually violated. But the cardinality commit lock is intentionally coarse - it is
    // keyed by (owner, owns(attribute_type)) without inspecting current counts, because each
    // transaction validates against its own snapshot and would otherwise both pass through and
    // commit a sum that violates the bound. So both writes contend on the same lock and one is
    // rejected as an isolation conflict, even though the resulting state would be valid.
    //
    // This test pins that behaviour: if it ever changes (e.g., to a finer-grained scheme that
    // permits non-violating concurrent writes), this test will fail and prompt review of the
    // cardinality lock-key composition.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..2);
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; insert $p has name "alice";"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; insert $p has name "bob";"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}

#[test]
fn concurrent_has_writes_to_different_owners_with_bounded_cardinality_both_succeed() {
    // Bounded cardinality on owns, but the two concurrent writes target *different* owners. The
    // cardinality lock is keyed by owner + owns, so the two transactions take disjoint locks and
    // never contend.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..1);
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $a isa person, has id 1;
            $b isa person, has id 2;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; insert $p has name "alice";"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 2; insert $p has name "bob";"#);

    try_commit(tx1).expect("tx1 commit");
    try_commit(tx2).expect("tx2 commit");
}

#[test]
fn concurrent_has_writes_without_owns_cardinality_both_succeed() {
    // `@card(0..)` is treated as unchecked - no validation is required and no commit lock is
    // taken. Concurrent has-writes against the same owner and attribute type therefore never
    // contend, regardless of how many entries each transaction adds.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..);
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; insert $p has name "alice";"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; insert $p has name "bob";"#);

    try_commit(tx1).expect("tx1 commit");
    try_commit(tx2).expect("tx2 commit");
}

///////////////////////////////
// Concurrent @unique on has //
///////////////////////////////

#[test]
fn concurrent_has_writes_violating_unique_one_fails() {
    // @unique requires the (attribute_value, owner_type) pair to map to a single owner instance.
    // Two persons each acquiring `has email "x"` in concurrent snapshots would result in two
    // owners of the same value-of-attribute - violating @unique. The unique commit lock must
    // serialise them.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns email @unique;
            attribute id, value integer;
            attribute email, value string;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $a isa person, has id 1;
            $b isa person, has id 2;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; insert $p has email "shared@example.com";"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 2; insert $p has email "shared@example.com";"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}

////////////////////////////
// Concurrent @key on has //
////////////////////////////

#[test]
fn concurrent_has_writes_violating_key_one_fails() {
    // @key = @card(1..1) + @unique. Two new entities each claiming the same key value race; the
    // commit lock on the key value must reject one.
    let (_tmp, database) = fresh_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns ref @key;
            attribute ref, value integer;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"insert $p isa person, has ref 1;"#);
    tx2 = run_write(tx2, r#"insert $p isa person, has ref 1;"#);

    let err = assert_exactly_one_failed(try_commit(tx1), try_commit(tx2));
    assert_isolation_conflict(&err);
}
