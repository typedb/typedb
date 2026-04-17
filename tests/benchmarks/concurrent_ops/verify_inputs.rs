/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// Small-scale correctness check for the inputs-stage bulk-load queries used by
// the concurrent_ops benchmark. Exercises each write pattern against a real
// database and reads back the results to assert the expected data was written.

use std::sync::Arc;

use database::{
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::{pipeline::stage::StageIterator, ExecutionInterrupt};
use options::{QueryOptions, TransactionOptions};
use query::query_manager::PipelinePayload;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, TempDir};

const DB_NAME: &str = "verify-inputs";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    attribute score value double;
    entity person owns name, owns age, owns score @card(0..);
    relation friendship relates friend @card(0..);
    person plays friendship:friend;
"#;

const INSERT_QUERY: &str =
    r#"inputs $n: string, $a: integer; insert $p isa person, has name == $n, has age == $a;"#;

const UPDATE_QUERY: &str =
    r#"inputs $n: string, $s: double; match $p isa person, has name == $n; insert $p has score == $s;"#;

const RELATION_QUERY: &str =
    r#"inputs $an: string, $bn: string; match $a isa person, has name == $an; $b isa person, has name == $bn; insert friendship (friend: $a, friend: $b);"#;

fn quoted(s: &str) -> String {
    format!(r#""{s}""#)
}

fn setup() -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();
    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, r) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    r.unwrap();
    tx.commit().1.unwrap();
    (tmp_dir, database)
}

fn run_write(
    database: &Arc<Database<WALClient>>,
    query: &str,
    inputs: Vec<Vec<Option<String>>>,
) {
    let parsed = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let payload = PipelinePayload { parsed, inputs: Some(inputs) };
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, r) = execute_write_query_in_write(
        tx,
        QueryOptions::default_grpc(),
        payload,
        query.to_string(),
        ExecutionInterrupt::new_uninterruptible(),
    );
    r.unwrap();
    tx.commit().1.unwrap();
}

fn count_rows(database: &Arc<Database<WALClient>>, query_str: &str) -> usize {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone(),
            type_manager,
            thing_manager.clone(),
            function_manager,
            PipelinePayload::from(query),
            query_str,
        )
        .unwrap();
    let (rows, _ctx) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    rows.collect_owned().unwrap().len()
}

fn verify_insert() {
    let (_tmp, database) = setup();
    let inputs = vec![
        vec![Some(quoted("alice")), Some("30".to_string())],
        vec![Some(quoted("bob")), Some("42".to_string())],
        vec![Some(quoted("carol")), Some("25".to_string())],
    ];
    run_write(&database, INSERT_QUERY, inputs);

    let total = count_rows(&database, "match $p isa person; select $p;");
    assert_eq!(total, 3, "expected 3 persons, got {total}");

    // Each person should have one name and one age
    let with_name = count_rows(&database, "match $p isa person, has name $n; select $p;");
    assert_eq!(with_name, 3, "expected 3 persons with names, got {with_name}");

    let with_age = count_rows(&database, "match $p isa person, has age $a; select $p;");
    assert_eq!(with_age, 3, "expected 3 persons with ages, got {with_age}");

    // Spot check specific values
    let alice_30 = count_rows(
        &database,
        r#"match $p isa person, has name "alice", has age 30; select $p;"#,
    );
    assert_eq!(alice_30, 1, "expected alice age=30 to exist");
    let bob_42 = count_rows(
        &database,
        r#"match $p isa person, has name "bob", has age 42; select $p;"#,
    );
    assert_eq!(bob_42, 1, "expected bob age=42 to exist");
    eprintln!("verify_insert: OK (3 persons, names + ages match inputs)");
}

fn verify_update() {
    let (_tmp, database) = setup();
    // First seed two persons
    let seed = vec![
        vec![Some(quoted("dave")), Some("40".to_string())],
        vec![Some(quoted("erin")), Some("33".to_string())],
    ];
    run_write(&database, INSERT_QUERY, seed);

    // Apply scores via UPDATE_QUERY (match + insert)
    let updates = vec![
        vec![Some(quoted("dave")), Some("12.5".to_string())],
        vec![Some(quoted("erin")), Some("99.0".to_string())],
    ];
    run_write(&database, UPDATE_QUERY, updates);

    let with_score = count_rows(&database, "match $p isa person, has score $s; select $p;");
    assert_eq!(with_score, 2, "expected 2 persons with scores, got {with_score}");

    let dave_score = count_rows(
        &database,
        r#"match $p isa person, has name "dave", has score 12.5; select $p;"#,
    );
    assert_eq!(dave_score, 1, "expected dave score=12.5 to exist");
    let erin_score = count_rows(
        &database,
        r#"match $p isa person, has name "erin", has score 99.0; select $p;"#,
    );
    assert_eq!(erin_score, 1, "expected erin score=99.0 to exist");
    eprintln!("verify_update: OK (scores applied to the correct persons)");
}

fn verify_relation() {
    let (_tmp, database) = setup();
    let seed = vec![
        vec![Some(quoted("frank")), Some("1".to_string())],
        vec![Some(quoted("gina")), Some("2".to_string())],
        vec![Some(quoted("hank")), Some("3".to_string())],
    ];
    run_write(&database, INSERT_QUERY, seed);

    let pairs = vec![
        vec![Some(quoted("frank")), Some(quoted("gina"))],
        vec![Some(quoted("gina")), Some(quoted("hank"))],
    ];
    run_write(&database, RELATION_QUERY, pairs);

    let friendships = count_rows(&database, "match $f isa friendship; select $f;");
    assert_eq!(friendships, 2, "expected 2 friendships, got {friendships}");

    let frank_gina = count_rows(
        &database,
        r#"match $a isa person, has name "frank"; $b isa person, has name "gina"; $f isa friendship (friend: $a, friend: $b); select $f;"#,
    );
    assert_eq!(frank_gina, 1, "expected a frank--gina friendship");
    eprintln!("verify_relation: OK (relations link the matched persons)");
}

fn main() {
    verify_insert();
    verify_update();
    verify_relation();
    eprintln!();
    eprintln!("All inputs-stage correctness checks passed.");
}
