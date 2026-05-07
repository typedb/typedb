/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{
    Database,
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{CommitIntent, DataCommitError, TransactionSchema, TransactionWrite},
};
use encoding::graph::thing::vertex_attribute::StringAttributeID;
use executor::ExecutionInterrupt;
use options::{QueryOptions, TransactionOptions};
use storage::{
    StorageCommitError, durability_client::WALClient, isolation_manager::IsolationConflict, snapshot::SnapshotError,
};
use test_utils::{TempDir, init_logging};

const DB_NAME: &str = "isolation-test";

fn create_reset_database() -> (TempDir, Arc<Database<WALClient>>) {
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

fn get_only_commit_error(a: Result<(), DataCommitError>, b: Result<(), DataCommitError>) -> DataCommitError {
    match (a, b) {
        (Ok(()), Err(err)) | (Err(err), Ok(())) => err,
        (Ok(()), Ok(())) => panic!("expected one commit to fail with isolation conflict; both succeeded"),
        (Err(a), Err(b)) => panic!("expected exactly one commit to fail; both failed: a={a:?} b={b:?}"),
    }
}

fn get_isolation_conflict(err: &DataCommitError) -> &IsolationConflict {
    match err {
        DataCommitError::SnapshotError {
            typedb_source: SnapshotError::Commit { typedb_source: StorageCommitError::Isolation { conflict, .. } },
        } => conflict,
        other => panic!(
            "expected DataCommitError::SnapshotError(SnapshotError::Commit(StorageCommitError::Isolation)), got: {other:?}"
        ),
    }
}

//////////////////////////////////////////
// Concurrent insert/delete of the same //
// identical entity and relation        //
//////////////////////////////////////////

#[test]
fn concurrent_inserts_of_new_entity_and_relation_both_succeed() {
    let (_tmp, database) = create_reset_database();
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
    let (_tmp, database) = create_reset_database();
    commit_schema(database.clone(), "define entity person, owns id @key; attribute id, value integer;");
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; delete $p;"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; delete $p;"#);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_deletes_of_identical_relation_one_fails() {
    let (_tmp, database) = create_reset_database();
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

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

/////////////////////////////////
// Concurrent owns cardinality //
/////////////////////////////////

#[test]
fn concurrent_has_writes_with_owns_cardinality_one_fails() {
    let (_tmp, database) = create_reset_database();
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

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    get_isolation_conflict(&err);
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_deletes_violating_owns_cardinality_lower_bound_one_fails() {
    // Mirror of the upper-bound test: owner cardinality is 1..2 for `name`. The owner starts
    // with two names. Two concurrent has-deletes from different snapshots, each removing one of
    // them, would drop the owner to 0 names if both committed - falling below the lower bound.
    // The cardinality commit lock must serialise them and reject one.
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(1..2);
            attribute id, value integer;
            attribute name @independent, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1, has name "alice", has name "bob";"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $p isa person, has id 1; $n isa name "alice"; delete has $n of $p;"#);
    tx2 = run_write(tx2, r#"match $p isa person, has id 1; $n isa name "bob"; delete has $n of $p;"#);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_with_bounded_owns_cardinality_always_contend_even_when_within_bound() {
    // Owner cardinality is 0..2: combined writes would stay within the bound, so the constraint
    // is not actually violated. But the cardinality commit lock is intentionally coarse - it is
    // keyed by (owner, owns(attribute_type)) without inspecting current counts, because each
    // transaction validates against its own snapshot and would otherwise both pass through and
    // commit a sum that violates the bound. So both writes contend on the same lock and one is
    // rejected as an isolation conflict, even though the resulting state would be valid.
    let (_tmp, database) = create_reset_database();
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

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_to_different_owners_with_bounded_cardinality_both_succeed() {
    let (_tmp, database) = create_reset_database();
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
fn concurrent_has_writes_violating_inherited_owns_cardinality_one_fails() {
    // Cardinality constraint is declared on the parent's owns. Two concurrent writes on a child
    // type's instance must still serialise on the (instance, owns) commit lock - the constraint
    // is sourced from the parent owns and the lock-key composition uses the constraint's source
    // attribute type, so subtypes inherit both the constraint and the lock identity.
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..1);
            entity student sub person;
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $s isa student, has id 1;"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $s isa student, has id 1; insert $s has name "alice";"#);
    tx2 = run_write(tx2, r#"match $s isa student, has id 1; insert $s has name "bob";"#);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_with_unlimited_cardinality_both_succeed() {
    let (_tmp, database) = create_reset_database();
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
    let (_tmp, database) = create_reset_database();
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

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_violating_inherited_unique_across_subtypes_one_fails() {
    // @unique is declared on the parent (`person owns email @unique`). Two concurrent writes
    // each insert the same email value but on *different* subtype instances (student, teacher).
    // The unique commit lock is keyed by `unique_constraint.source().owner().vertex()` and
    // `source().attribute().vertex()` - both resolve to the parent's owns - so the lock key is
    // identical regardless of which subtype the runtime instance has. One transaction must fail.
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns email @unique;
            entity student sub person;
            entity teacher sub person;
            attribute id, value integer;
            attribute email, value string;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $s isa student, has id 1;
            $t isa teacher, has id 2;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"match $s isa student, has id 1; insert $s has email "shared@example.com";"#);
    tx2 = run_write(tx2, r#"match $t isa teacher, has id 2; insert $t has email "shared@example.com";"#);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

////////////////////////////
// Concurrent @key on has //
////////////////////////////

#[test]
fn concurrent_has_writes_violating_inline_key_one_fails() {
    let (_tmp, database) = create_reset_database();
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

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_violating_hashed_string_key_one_fails() {
    // String @key with a value longer than the inline threshold (16 bytes)
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns ref @key;
            attribute ref, value string;
        "#,
    );

    let key_value = "a-string-key-long-enough-to-force-hashed-encoding";
    debug_assert!(
        key_value.len() > StringAttributeID::INLINE_OR_PREFIXED_HASH_LENGTH,
        "must exceed StringAttributeID inline threshold to hit the hashed path"
    );
    let query = format!(r#"insert $p isa person, has ref "{key_value}";"#);

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, &query);
    tx2 = run_write(tx2, &query);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_has_writes_violating_inherited_key_across_subtypes_one_fails() {
    // @key is declared on the parent (`person owns ref @key`) and inherited by both subtypes.
    // Two concurrent inserts each create a different subtype (student, teacher) but with the
    // same key value. The conflict is on the parent type's interpretation of (type + key value):
    // the unique commit lock keyed by the source Owns serialises them and rejects one.
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns ref @key;
            entity student sub person;
            entity teacher sub person;
            attribute ref, value integer;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(tx1, r#"insert $s isa student, has ref 1;"#);
    tx2 = run_write(tx2, r#"insert $t isa teacher, has ref 1;"#);

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

////////////////////////////////////////
// Concurrent relates/plays @card     //
////////////////////////////////////////

#[test]
fn concurrent_player_links_with_bounded_relates_cardinality_one_fails() {
    // The padding player is needed so `cleanup_relations` doesn't auto-delete an empty
    // friendship at setup-commit time
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, plays friendship:friend;
            relation friendship, relates friend @card(0..2), owns rid @key;
            attribute id, value integer;
            attribute rid, value integer;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $pad isa person, has id 0;
            $a isa person, has id 1;
            $b isa person, has id 2;
            $f isa friendship, links (friend: $pad), has rid 100;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(
        tx1,
        r#"match $f isa friendship, has rid 100; $p isa person, has id 1; insert $f links (friend: $p);"#,
    );
    tx2 = run_write(
        tx2,
        r#"match $f isa friendship, has rid 100; $p isa person, has id 2; insert $f links (friend: $p);"#,
    );

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

#[test]
fn concurrent_player_links_with_bounded_plays_cardinality_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, plays friendship:friend @card(0..1);
            relation friendship, relates friend @card(0..), owns rid @key;
            attribute id, value integer;
            attribute rid, value integer;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $pad1 isa person, has id 1001;
            $pad2 isa person, has id 1002;
            $p isa person, has id 1;
            $f isa friendship, links (friend: $pad1), has rid 100;
            $g isa friendship, links (friend: $pad2), has rid 200;
        "#,
    );

    let mut tx1 = open_write(database.clone());
    let mut tx2 = open_write(database.clone());
    tx1 = run_write(
        tx1,
        r#"match $f isa friendship, has rid 100; $p isa person, has id 1; insert $f links (friend: $p);"#,
    );
    tx2 = run_write(
        tx2,
        r#"match $g isa friendship, has rid 200; $p isa person, has id 1; insert $g links (friend: $p);"#,
    );

    let err = get_only_commit_error(try_commit(tx1), try_commit(tx2));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::ExclusiveLock);
}

////////////////////////////////////////////////
// Concurrent write/delete crossover          //
////////////////////////////////////////////////

#[test]
fn concurrent_has_write_and_owner_delete_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..);
            attribute id, value integer;
            attribute name, value string;
        "#,
    );
    commit_write_query(database.clone(), r#"insert $p isa person, has id 1;"#);

    let mut tx_delete = open_write(database.clone());
    let mut tx_write = open_write(database.clone());
    tx_delete = run_write(tx_delete, r#"match $p isa person, has id 1; delete $p;"#);
    tx_write = run_write(tx_write, r#"match $p isa person, has id 1; insert $p has name "alice";"#);

    let err = get_only_commit_error(try_commit(tx_delete), try_commit(tx_write));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::RequireDeletedKey);
}

// TODO: ideally, this wouldn't fail. In fact, if we were to insert the attribute rather than match-insert it,
//       this should always succeed as we always create the attribtue
#[test]
fn concurrent_has_write_and_attribute_delete_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..);
            attribute id, value integer;
            attribute name @independent, value string;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $p isa person, has id 1;
            $n isa name "alice";
        "#,
    );

    let mut tx_delete = open_write(database.clone());
    let mut tx_write = open_write(database.clone());
    tx_delete = run_write(tx_delete, r#"match $n isa name "alice"; delete $n;"#);
    tx_write = run_write(tx_write, r#"match $p isa person, has id 1; $n isa name "alice"; insert $p has $n;"#);

    let err = get_only_commit_error(try_commit(tx_delete), try_commit(tx_write));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::RequireDeletedKey);
}

// Companion of the test above for the alternative (no-match) write formulation called out in the
// TODO: tx_write uses `insert $p has name "alice"` without first `match`-ing the existing
// attribute. Each insert independently materialises (or finds) the attribute by its value-encoded
// vertex, so the write does not "require" the existing key. Pinning the actual behaviour here -
// if a future change makes pure-insert succeed (the noted ideal), this test will fail and prompt
// updating the assertion.
#[test]
fn concurrent_has_insert_without_match_and_attribute_delete_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, owns name @card(0..);
            attribute id, value integer;
            attribute name @independent, value string;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $p isa person, has id 1;
            $n isa name "alice";
        "#,
    );

    let mut tx_delete = open_write(database.clone());
    let mut tx_write = open_write(database.clone());
    tx_delete = run_write(tx_delete, r#"match $n isa name "alice"; delete $n;"#);
    tx_write = run_write(tx_write, r#"match $p isa person, has id 1; insert $p has name "alice";"#);

    let err = get_only_commit_error(try_commit(tx_delete), try_commit(tx_write));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::RequireDeletedKey);
}

#[test]
fn concurrent_player_link_and_relation_delete_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, plays friendship:friend;
            relation friendship, relates friend @card(0..), owns rid @key;
            attribute id, value integer;
            attribute rid, value integer;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $pad isa person, has id 1001;
            $p isa person, has id 1;
            $f isa friendship, links (friend: $pad), has rid 100;
        "#,
    );

    let mut tx_delete = open_write(database.clone());
    let mut tx_write = open_write(database.clone());
    tx_delete = run_write(tx_delete, r#"match $f isa friendship, has rid 100; delete $f;"#);
    tx_write = run_write(
        tx_write,
        r#"match $f isa friendship, has rid 100; $p isa person, has id 1; insert $f links (friend: $p);"#,
    );

    let err = get_only_commit_error(try_commit(tx_delete), try_commit(tx_write));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::RequireDeletedKey);
}

#[test]
fn concurrent_player_link_and_player_delete_one_fails() {
    let (_tmp, database) = create_reset_database();
    commit_schema(
        database.clone(),
        r#"define
            entity person, owns id @key, plays friendship:friend;
            relation friendship, relates friend @card(0..), owns rid @key;
            attribute id, value integer;
            attribute rid, value integer;
        "#,
    );
    commit_write_query(
        database.clone(),
        r#"insert
            $pad isa person, has id 1001;
            $p isa person, has id 1;
            $f isa friendship, links (friend: $pad), has rid 100;
        "#,
    );

    let mut tx_delete = open_write(database.clone());
    let mut tx_write = open_write(database.clone());
    tx_delete = run_write(tx_delete, r#"match $p isa person, has id 1; delete $p;"#);
    tx_write = run_write(
        tx_write,
        r#"match $f isa friendship, has rid 100; $p isa person, has id 1; insert $f links (friend: $p);"#,
    );

    let err = get_only_commit_error(try_commit(tx_delete), try_commit(tx_write));
    assert_eq!(get_isolation_conflict(&err), &IsolationConflict::RequireDeletedKey);
}
