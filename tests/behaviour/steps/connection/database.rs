/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use macro_rules_attribute::apply;

use crate::{generic_step, util, Context};

#[apply(generic_step)]
#[step(expr = "connection create database: {word}")]
pub async fn connection_create_database(context: &mut Context, name: String) {
    context.server_mut().unwrap().create_database(name);
}

#[apply(generic_step)]
#[step(expr = "connection create database(s):")]
async fn connection_create_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step) {
        connection_create_database(context, name.into()).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection create databases in parallel:")]
async fn connection_create_databases_in_parallel(context: &mut Context, step: &Step) {
    todo!()
    // join_all(util::iter_table(step).map(|name| create_database(&context.databases, name.to_string()))).await;
}

#[apply(generic_step)]
#[step(expr = "connection delete database: {word}")]
pub async fn connection_delete_database(context: &mut Context, name: String) {
    todo!()
    // context.databases.get(name).and_then(Database::delete).await.unwrap();
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s):")]
async fn connection_delete_databases(context: &mut Context, step: &Step) {
    todo!()
    // for name in util::iter_table(step) {
    // context.databases.get(name).and_then(Database::delete).await.unwrap();
    // }
}

#[apply(generic_step)]
#[step(expr = "connection delete databases in parallel:")]
async fn connection_delete_databases_in_parallel(context: &mut Context, step: &Step) {
    todo!()
    // try_join_all(util::iter_table(step).map(|name| context.databases.get(name).and_then(Database::delete)))
    // .await
    // .unwrap();
}

#[apply(generic_step)]
#[step(expr = "connection delete database; throws exception: {word}")]
async fn connection_delete_database_throws_exception(context: &mut Context, name: String) {
    todo!()
    // assert!(context.databases.get(name).and_then(Database::delete).await.is_err());
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s); throws exception")]
async fn connection_delete_databases_throws_exception(context: &mut Context, step: &Step) {
    todo!()
    // for name in util::iter_table(step) {
    // assert!(context.databases.get(name).and_then(Database::delete).await.is_err());
    // }
}

#[apply(generic_step)]
#[step(expr = "connection has database: {word}")]
async fn connection_has_database(context: &mut Context, name: String) {
    assert!(context.databases().contains_key(&name), "Connection doesn't contain database {name}.",);
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
    assert!(!context.databases().contains_key(&name), "Connection doesn't contain database {name}.",);
}

#[apply(generic_step)]
#[step(expr = "connection does not have database(s):")]
async fn connection_does_not_have_databases(context: &mut Context, step: &Step) {
    for name in util::iter_table(step).map(str::to_owned) {
        connection_does_not_have_database(context, name).await;
    }
}
