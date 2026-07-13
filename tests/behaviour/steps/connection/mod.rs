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
use tokio::sync::OnceCell;

use crate::{Context, generic_step};

mod database;
mod transaction;

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    let server_instance = context.server_instance.as_ref().expect("Expected TypeDB to get or be initialised");
    context.server = Some(server_instance.clone());
}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
#[step("connection closes")]
#[step("connection is open: true")]
#[step("connection is open: false")]
pub async fn connection_ignore(_: &mut Context) {}

#[cucumber::given(expr = r"connection has {int} database(s)")]
#[cucumber::then(expr = r"connection has {int} database(s)")]
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
