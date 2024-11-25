pub mod repositories;
pub mod concepts;
pub mod util;

use crate::util::transaction_util::TransactionUtil;
use database::database_manager::DatabaseManager;
use database::Database;
use std::sync::Arc;
use storage::durability_client::WALClient;
use typeql;

const SYSTEM_DB: &str = "system";

// TODO: read from a file at compile time
const SCHEMA: &str = "
define
    attribute schema-version value long;
    attribute name value string;
    attribute uuid value string;
    attribute hash value string;
    attribute salt value string;

    entity user,
        owns uuid @card(1),
        owns name @card(1),
        plays user-password:user;

        entity password,
        owns hash @card(1),
        owns salt @card(1),
        plays user-password:password;

    relation user-password,
        relates user @card(1),
        relates password @card(1);
";

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
