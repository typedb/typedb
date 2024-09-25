/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Mutex;

use cucumber::gherkin::Step;
use futures::future::join_all;
use macro_rules_attribute::apply;
use server::typedb;

use crate::{generic_step, params, util, Context};

pub async fn server_create_database(server: &'_ Mutex<typedb::Server>, name: String) {
    server.lock().unwrap().database_manager().create_database(&name);
}

pub async fn server_delete_database(server: &'_ Mutex<typedb::Server>, name: String, may_error: params::MayError) {
    may_error.check(server.lock().unwrap().database_manager().delete_database(&name));
}

#[apply(generic_step)]
#[step(expr = "connection create database: {word}")]
pub async fn connection_create_database(context: &mut Context, name: String) {
    server_create_database(context.server().unwrap(), name).await
}

#[apply(generic_step)]
#[step(expr = "connection create database(s):")]
pub async fn connection_create_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step) {
        server_create_database(context.server().unwrap(), name.into()).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection create database(s) in parallel:")]
pub async fn connection_create_databases_in_parallel(context: &mut Context, step: &Step) {
    join_all(util::iter_table(step).map(|name| server_create_database(context.server().unwrap(), name.into()))).await;
}

#[apply(generic_step)]
#[step(expr = "connection reset database: {word}")]
pub async fn connection_reset_database(context: &mut Context, name: String) {
    if context.active_transaction.is_some() {
        context.close_transaction();
    }
    context.server().unwrap().lock().unwrap().database_manager().reset_else_recreate_database(&name).unwrap();
}

#[apply(generic_step)]
#[step(expr = "connection delete database: {word}{may_error}")]
pub async fn connection_delete_database(context: &mut Context, name: String, may_error: params::MayError) {
    server_delete_database(context.server().unwrap(), name, may_error).await
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s):")]
async fn connection_delete_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step) {
        server_delete_database(context.server().unwrap(), name.into(), params::MayError::False).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s) in parallel:")]
async fn connection_delete_databases_in_parallel(context: &mut Context, step: &Step) {
    join_all(
        util::iter_table(step)
            .map(|name| server_delete_database(context.server().unwrap(), name.into(), params::MayError::False)),
    )
    .await;
}

#[apply(generic_step)]
#[step(expr = "connection has database: {word}")]
async fn connection_has_database(context: &mut Context, name: String) {
    assert!(
        context.server().unwrap().lock().unwrap().database_manager().database(&name).is_some(),
        "Connection doesn't contain database {name}.",
    );
}

#[apply(generic_step)]
#[step(expr = "connection has database(s):")]
async fn connection_has_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step).map(str::to_owned) {
        connection_has_database(context, name).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection does not have database: {word}")]
async fn connection_does_not_have_database(context: &mut Context, name: String) {
    assert!(
        context.server().unwrap().lock().unwrap().database_manager().database(&name).is_none(),
        "Connection should not contain database {name}.",
    );
}

#[apply(generic_step)]
#[step(expr = "connection does not have database(s):")]
async fn connection_does_not_have_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step).map(str::to_owned) {
        connection_does_not_have_database(context, name).await;
    }
}
