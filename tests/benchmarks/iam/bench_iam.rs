/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs::File, io::Read, path::Path, sync::Arc, time::Instant};

use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::{batch::Batch, pipeline::stage::StageIterator, ExecutionInterrupt};
use options::TransactionOptions;
use storage::durability_client::WALClient;
use test_utils::create_tmp_dir;

const DB_NAME: &str = "benchmark-iam";
const RESOURCE_PATH: &str = "tests/benchmarks/iam";
const SCHEMA_FILENAME: &str = "schema.tql";
const FUNCTIONS_FILENAME: &str = "functions.tql";
const DATA_FILENAME: &str = "data.tql";

fn load_schema_tql(database: Arc<Database<WALClient>>, schema_tql: &Path) {
    let mut contents = Vec::new();
    File::open(schema_tql).unwrap().read_to_end(&mut contents);
    let schema_str = String::from_utf8(contents).unwrap();
    let schema_query = typeql::parse_query(schema_str.as_str()).unwrap().into_structure().into_schema();

    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionSchema {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = tx;
    let mut inner_snapshot = snapshot.into_inner();
    query_manager
        .execute_schema(
            &mut inner_snapshot,
            &type_manager,
            &thing_manager,
            &function_manager,
            schema_query,
            &schema_str,
        )
        .unwrap();
    let tx = TransactionSchema::from_parts(
        Arc::new(inner_snapshot),
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );
    tx.commit().1.unwrap();
}

fn load_data_tql(database: Arc<Database<WALClient>>, data_tql: &Path) {
    let mut contents = Vec::new();

    File::open(data_tql).unwrap().read_to_end(&mut contents);
    let data_str = String::from_utf8(contents).unwrap();
    let data_query = typeql::parse_query(data_str.as_str()).unwrap().into_structure().into_pipeline();
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionWrite {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = tx;
    let write_pipeline = query_manager
        .prepare_write_pipeline(
            snapshot.into_inner(),
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &data_query,
            &data_str,
        )
        .unwrap();
    let (_output, context) = write_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let tx = TransactionWrite::from_parts(
        context.snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );
    tx.commit().1.unwrap();
}

fn setup() -> Arc<Database<WALClient>> {
    let tmp_dir = create_tmp_dir();
    {
        let dbm = DatabaseManager::new(&tmp_dir).unwrap();
        dbm.put_database(DB_NAME).unwrap();
        let database = dbm.database(DB_NAME).unwrap();
        let schema_path = Path::new(RESOURCE_PATH).join(Path::new(SCHEMA_FILENAME));
        let functions_path = Path::new(RESOURCE_PATH).join(Path::new(FUNCTIONS_FILENAME));
        let data_path = Path::new(RESOURCE_PATH).join(Path::new(DATA_FILENAME));

        load_schema_tql(database.clone(), &schema_path);
        load_schema_tql(database.clone(), &functions_path);
        load_data_tql(database.clone(), &data_path);
    }
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();

    dbm.database(DB_NAME).unwrap()
}

fn run_query(database: Arc<Database<WALClient>>, query_str: &str) -> Batch {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone_inner(),
            type_manager,
            thing_manager.clone(),
            function_manager,
            &query,
            query_str,
        )
        .unwrap();
    let (rows, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    rows.collect_owned().unwrap()
}

#[test]
fn has_permission() {
    let email = "douglas.schmidt@vaticle.com";
    let path = "root/engineering/typedb-studio/src/README.md";
    let operation = "edit file";
    let query = format!(
        r#"
    match
        $p isa person, has email "{email}";
        $f isa file, has path "{path}";
        $o isa operation, has name "{operation}";
        let $permission = has_permission($p, $f, $o); # TODO: This used to have a validity true check
    "#
    );

    let database = setup();
    let start_time = Instant::now();
    let answers = run_query(database.clone(), query.as_str());
    let time_taken = start_time.elapsed();
    assert_eq!(1, answers.len());
    println!("Completed successfully in: {} ms", time_taken.as_millis())
}

#[test]
fn list_permissions() {
    let email = "douglas.schmidt@vaticle.com";
    let query = format!(
        r#"
    match
        $p isa person, has email "{email}";
        let $ac in list_permissions($p); #, has validity $v;
        $ac isa access (object: $o, action: $a);
        # $o isa object, has id $id;
        # $a isa action, has name $n;
    "#
    );

    let database = setup();

    let start_time = Instant::now();
    let answers = run_query(database.clone(), query.as_str());
    let time_taken = start_time.elapsed();
    assert_eq!(67, answers.len());
    println!("Completed successfully in: {} ms", time_taken.as_millis())
}
