pub mod repositories;
pub mod concepts;
pub mod util;

use std::fmt::format;
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
            database_manager.create_database(SYSTEM_DB)
                .expect(format!("Unable to create the {} database.", SYSTEM_DB).as_str());
            let db = database_manager.database(SYSTEM_DB)
                .expect(format!("The {} database could not be found.", SYSTEM_DB).as_str());
            let tx_mgr = TransactionUtil::new(db.clone());
            tx_mgr.schema_transaction(|query_mgr, snapshot, type_mgr, thing_mgr, fn_mgr| {
                let query = typeql::parse_query(SCHEMA)
                    .expect(format!(
                        "Unexpected error occurred when parsing the schema for the {} database.",
                        SYSTEM_DB).as_str()
                    ).into_schema();
                query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, query).expect(
                    format!(
                        "Unexpected error occurred when defining the schema for the {} database.",
                        SYSTEM_DB
                    ).as_str()
                );
            }).expect(format!(
                "Unexpected error occurred when committing the schema transaction for {} database.",
                SYSTEM_DB
            ).as_str());
            db
        }
    }
}
