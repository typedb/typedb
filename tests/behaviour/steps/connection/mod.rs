/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use itertools::Either;
use macro_rules_attribute::apply;
use server::authentication::Accessor;

use crate::{Context, generic_step};

mod database;
mod transaction;

pub(crate) fn admin_accessor() -> Accessor {
    Accessor::Default
}

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    let server_instance = context.server_instance.as_ref().expect("Expected TypeDB to get or be initialised");
    context.server = Some(server_instance.clone());
}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
async fn connection_opens_with_default_authentication(context: &mut Context) {
    context.active_connection = Some(Accessor::Default);
}

#[apply(generic_step)]
#[step("connection closes")]
async fn connection_closes(context: &mut Context) {
    context.active_connection = None;
}

#[apply(generic_step)]
#[step("connection is open: true")]
#[step("connection is open: false")]
pub async fn connection_ignore(_: &mut Context) {}

#[apply(generic_step)]
#[step(expr = "connection opens with username '{word}', password '{word}'{may_error}")]
async fn connection_opens_with_authentication(
    context: &mut Context,
    username: String,
    password: String,
    may_error: params::MayError,
) {
    context.active_connection = None; // Unset active session
    let server = context.server_state();
    let token_result = server.users().token_create(username, password).await;
    if let Either::Left(token) = may_error.do_not_expect_error_message().check(token_result) {
        let session = server.users().token_get_session(token.as_str()).await.unwrap();
        context.active_connection = Some(Accessor::Session(session));
    }
}

#[cucumber::given(expr = r"connection has {int} database(s)")]
#[cucumber::then(expr = r"connection has {int} database(s)")]
pub async fn connection_has_count_databases(context: &mut Context, count: usize) {
    assert_eq!(context.server_state().databases().all().await.unwrap().len(), count)
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
