/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use database::Database;
use database::transaction::{TransactionSchema, TransactionWrite};
use executor::ExecutionInterrupt;
use function::function_manager::FunctionManager;
use options::TransactionOptions;
use query::query_manager::QueryManager;
use storage::durability_client::WALClient;
use storage::snapshot::CommittableSnapshot;
use test_utils_encoding::create_core_storage;

fn load_schema_tql(database: Arc<Database<WALClient>>, schema_tql: &Path) {
    let mut contents = Vec::new();
    File::open(schema_tql).unwrap().read_to_end(&mut contents);
    let schema_str = String::from_utf8(contents).unwrap();
    let schema_query = typeql::parse_query(schema_str.as_str()).unwrap().into_schema();

    let mut tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionSchema { query_manager, mut snapshot, type_manager, thing_manager, function_manager, .. } = &mut tx;
    query_manager.execute_schema(&mut snapshot, type_manager, thing_manager, &function_manager, schema_query).unwrap()
}

fn load_data_tql(database: Arc<Database<WALClient>>, data_tql: &Path) {
    let mut contents = Vec::new();

    File::open(data_tql).unwrap().read_to_end(&mut contents);
    let data_str = String::from_utf8(contents).unwrap();
    let data_query = typeql::parse_query(data_str.as_str()).unwrap().into_pipeline();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionWrite { query_manager, mut snapshot, type_manager, thing_manager, function_manager, .. } = &mut tx;
    let write_pipeline = query_manager.prepare_write_pipeline(&mut snapshot, type_manager, thing_manager.clone(), &function_manager, &data_query).unwrap();
    let (output, context) = write_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    context.snapshot.commit().unwrap();
}


fn setup() {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);


    load_schema_tql(database, )
}