pub mod repositories;
pub mod concepts;
pub mod transaction_manager;

use database::database_manager::DatabaseManager;
use database::Database;
use std::sync::Arc;
use storage::durability_client::WALClient;
use crate::transaction_manager::TransactionManager;

const SYSTEM_DB: &str = "system";

pub fn create_if_not_exists(database_manager: &DatabaseManager) -> Arc<Database<WALClient>> {
    match database_manager.database(SYSTEM_DB) {
        Some(db) => db,
        None => {
            database_manager.create_database(SYSTEM_DB).unwrap();
            let db = database_manager.database(SYSTEM_DB).unwrap();
            let tx_mgr = TransactionManager::new(db.clone());
            tx_mgr.schema_transaction(|tx| {}).unwrap();
            db
        }
    }
}