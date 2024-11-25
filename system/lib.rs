pub mod repositories;
pub mod concepts;
pub mod util;

use crate::util::transaction_util::TransactionUtil;
use database::database_manager::DatabaseManager;
use database::Database;
use std::sync::Arc;
use storage::durability_client::WALClient;
use typeql;
use crate::repositories::SCHEMA;

const SYSTEM_DB: &str = "system";

pub fn initialise_system_database(database_manager: &DatabaseManager) -> Arc<Database<WALClient>> {
    match database_manager.database(SYSTEM_DB) {
        Some(db) => db,
        None => {
            database_manager.create_database(SYSTEM_DB).unwrap();
            let db = database_manager.database(SYSTEM_DB).unwrap();
            let tx_mgr = TransactionUtil::new(db.clone());
            tx_mgr.schema_transaction(|query_mgr, snapshot, type_mgr, thing_mgr, fn_mgr| {
                let query = typeql::parse_query(SCHEMA).unwrap().into_schema();
                query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, query).unwrap();
            }).unwrap();
            db
        }
    }
}
