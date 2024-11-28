/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod concepts;
pub mod repositories;
pub mod util;

use std::{fmt::format, sync::Arc};

use database::{database_manager::DatabaseManager, Database};
use storage::durability_client::WALClient;
use typeql;
use database::database_manager::INTERNAL_DATABASE_PREFIX;
use crate::{repositories::SCHEMA, util::transaction_util::TransactionUtil};

fn get_system_database_name() -> String {
    format!("{}system", INTERNAL_DATABASE_PREFIX)
}

pub fn initialise_system_database(database_manager: &DatabaseManager) -> Arc<Database<WALClient>> {
    let system_db_name = get_system_database_name();
    let system_db_name = system_db_name.as_str();
    match database_manager.database_unrestricted(system_db_name) {
        Some(db) => db,
        None => {
            database_manager
                .create_database_unrestricted(system_db_name)
                .expect(format!("Unable to create the {} database.", system_db_name).as_str());
            let db = database_manager
                .database_unrestricted(system_db_name)
                .expect(format!("The {} database could not be found.", system_db_name).as_str());
            let tx_util = TransactionUtil::new(db.clone());
            tx_util
                .schema_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr| {
                    let query = typeql::parse_query(SCHEMA)
                        .expect(
                            format!(
                                "Unexpected error occurred when parsing the schema for the {} database.",
                                system_db_name
                            )
                            .as_str(),
                        )
                        .into_schema();
                    query_mgr.execute_schema(snapshot, type_mgr, thing_mgr, query).expect(
                        format!("Unexpected error occurred when defining the schema for the {} database.", system_db_name)
                            .as_str(),
                    );
                })
                .expect(
                    format!(
                        "Unexpected error occurred when committing the schema transaction for {} database.",
                        system_db_name
                    )
                    .as_str(),
                );
            db
        }
    }
}
