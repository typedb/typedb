/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use macro_rules_attribute::apply;
use resource::constants::server::ASCII_LOGO;
use server::{parameters::config::Config, server::Server};
use test_utils::{create_tmp_dir, TempDir};
use tokio::sync::OnceCell;

use crate::{generic_step, Context};

mod database;
mod transaction;

const ADDRESS: &str = "0.0.0.0:1729";
const DISTRIBUTION: &str = "TypeDB CE TEST";
const VERSION: &str = "0.0.0";
static TYPEDB: OnceCell<(TempDir, Arc<Mutex<Server>>)> = OnceCell::const_new();

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    TYPEDB
        .get_or_init(|| async {
            let server_dir = create_tmp_dir();
            let config = Config::new(ADDRESS).development_mode(true).build();
            let server = Server::new(config, ASCII_LOGO, DISTRIBUTION, VERSION, None).await.unwrap();
            (server_dir, Arc::new(Mutex::new(server)))
        })
        .await;

    let (_, server) = TYPEDB.get().expect("Expected TypeDB to get or be initialised");
    context.server = Some(server.clone());
}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
#[step("connection closes")]
#[step("connection is open: true")]
#[step("connection is open: false")]
pub async fn connection_ignore(_: &mut Context) {}

#[apply(generic_step)]
#[step(expr = r"connection has {int} database(s)")]
pub async fn connection_has_count_databases(context: &mut Context, count: usize) {
    assert_eq!(context.server().unwrap().lock().unwrap().database_manager().database_names().len(), count)
}

#[derive(Debug, Clone)]
pub enum BehaviourConnectionTestExecutionError {
    CannotCommitReadTransaction,
    CannotRollbackReadTransaction,
}

impl fmt::Display for BehaviourConnectionTestExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Error for BehaviourConnectionTestExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CannotCommitReadTransaction => None,
            Self::CannotRollbackReadTransaction => None,
        }
    }
}
