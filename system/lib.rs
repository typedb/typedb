/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{database_manager::DatabaseManager, Database};
use resource::internal_database_prefix;
use storage::durability_client::WALClient;

use crate::{repositories::SCHEMA, util::transaction_util::TransactionUtil};

pub mod concepts;
pub mod repositories;
pub mod util;

const SYSTEM_DB: &str = concat!(internal_database_prefix!(), "system");

pub fn initialise_system_database(database_manager: &DatabaseManager) -> Arc<Database<WALClient>> {
    match database_manager.database_unrestricted(SYSTEM_DB) {
        Some(db) => db,
        None => {
            database_manager
                .put_database_unrestricted(SYSTEM_DB)
                .unwrap_or_else(|_| panic!("Unable to create the {} database.", SYSTEM_DB));
            let db = database_manager
                .database_unrestricted(SYSTEM_DB)
                .unwrap_or_else(|| panic!("The {} database could not be found.", SYSTEM_DB));
            let tx_util = TransactionUtil::new(db.clone());
            tx_util
                .schema_transaction_commit(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr| {
                    let query = typeql::parse_query(SCHEMA)
                        .unwrap_or_else(|_| {
                            panic!("Unexpected error occurred when parsing the schema for the {} database.", SYSTEM_DB)
                        })
                        .into_structure()
                        .into_schema();
                    query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, fn_mgr, query, SCHEMA).unwrap_or_else(
                        |_| {
                            panic!("Unexpected error occurred when defining the schema for the {} database.", SYSTEM_DB)
                        },
                    );
                })
                .1
                .unwrap_or_else(|_| {
                    panic!(
                        "Unexpected error occurred when committing the schema transaction for {} database.",
                        SYSTEM_DB
                    )
                });
            db
        }
    }
}
