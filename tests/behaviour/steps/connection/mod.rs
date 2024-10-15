/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex, OnceLock},
};

use macro_rules_attribute::apply;
use server::{parameters::config::Config, typedb};
use test_utils::{create_tmp_dir, TempDir};

use crate::{generic_step, Context};

mod database;
mod transaction;

static TYPEDB: OnceLock<(TempDir, Arc<Mutex<typedb::Server>>)> = OnceLock::new();

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    let (_, server) = TYPEDB.get_or_init(|| {
        let server_dir = create_tmp_dir();
        let config = Config::new_with_data_directory(server_dir.as_ref());
        let server = typedb::Server::open(config).unwrap();
        (server_dir, Arc::new(Mutex::new(server)))
    });

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
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
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
