/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use futures::future::{join_all, try_join_all};
use hyper::{client::HttpConnector, Client};
use macro_rules_attribute::apply;

use crate::{
    generic_step, in_background,
    message::{
        databases, databases_create, databases_delete, databases_get, databases_schema, databases_type_schema, query,
        transactions_query,
    },
    params::TokenMode,
    util::{iter_table, random_uuid},
    Context, HttpContext,
};

async fn create_database(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    name: String,
    may_error: params::MayError,
) {
    may_error.check(databases_create(http_client, auth_token, &name).await);
}

async fn delete_database(
    http_client: &Client<HttpConnector>,
    auth_token: Option<impl AsRef<str>>,
    name: &str,
    may_error: params::MayError,
) {
    may_error.do_not_expect_error_message().check(databases_delete(http_client, auth_token, name).await);
}

async fn has_database(context: &HttpContext, name: &str) -> bool {
    databases(context.http_client(), context.auth_token())
        .await
        .unwrap()
        .databases
        .iter()
        .any(|database| database.name == name)
}

#[apply(generic_step)]
#[step(expr = "{token_mode}connection create database: {word}{may_error}")]
pub async fn connection_create_database(
    context: &mut Context,
    token_mode: TokenMode,
    name: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    create_database(context.http_client(), context.auth_token_by_mode(token_mode), name, may_error).await;
}

#[apply(generic_step)]
#[step(expr = "connection create database with empty name{may_error}")]
pub async fn connection_create_database_with_an_empty_name(context: &mut Context, may_error: params::MayError) {
    create_database(
        context.http_client(),
        context.auth_token(),
        "".to_string(),
        may_error.do_not_expect_error_message(),
    )
    .await;
}

#[apply(generic_step)]
#[step(expr = "connection create database(s):")]
async fn connection_create_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step) {
        create_database(context.http_client(), context.auth_token(), name.into(), params::MayError::False).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection create databases in parallel:")]
async fn connection_create_databases_in_parallel(context: &mut Context, step: &Step) {
    join_all(iter_table(step).map(|name| databases_create(context.http_client(), context.auth_token(), name))).await;
}

#[apply(generic_step)]
#[step(expr = "in background, connection create database: {word}{may_error}")]
pub async fn in_background_connection_create_database(
    _context: &mut Context,
    name: String,
    may_error: params::MayError,
) {
    in_background!(context, |background| {
        create_database(background.http_client(), background.auth_token(), name, may_error).await;
    });
}

#[apply(generic_step)]
#[step(expr = "{token_mode}connection delete database: {word}{may_error}")]
pub async fn connection_delete_database(
    context: &mut Context,
    token_mode: TokenMode,
    name: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    delete_database(context.http_client(), context.auth_token_by_mode(token_mode), &name, may_error).await;
}

#[apply(generic_step)]
#[step(expr = "connection delete database(s):")]
async fn connection_delete_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step) {
        delete_database(context.http_client(), context.auth_token(), name, params::MayError::False).await;
    }
}

#[apply(generic_step)]
#[step(expr = "connection delete databases in parallel:")]
async fn connection_delete_databases_in_parallel(context: &mut Context, step: &Step) {
    try_join_all(iter_table(step).map(|name| databases_delete(context.http_client(), context.auth_token(), name)))
        .await
        .unwrap();
}

#[apply(generic_step)]
#[step(expr = "in background, connection delete database: {word}{may_error}")]
pub async fn in_background_connection_delete_database(
    _context: &mut Context,
    name: String,
    may_error: params::MayError,
) {
    in_background!(context, |background| {
        delete_database(background.http_client(), background.auth_token(), &name, may_error).await;
    });
}

#[apply(generic_step)]
#[step(expr = "{token_mode}connection get all databases{may_error}")]
async fn connection_get_all_databases(context: &mut Context, token_mode: TokenMode, may_error: params::MayError) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error.check(databases(context.http_client(), context.auth_token_by_mode(token_mode)).await);
}

#[apply(generic_step)]
#[step(expr = "{token_mode}connection get database: {word}{may_error}")]
async fn connection_get_database(
    context: &mut Context,
    token_mode: TokenMode,
    name: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error.check(databases_get(context.http_client(), context.auth_token_by_mode(token_mode), &name).await);
}

#[apply(generic_step)]
#[step(expr = "connection has database: {word}")]
async fn connection_has_database(context: &mut Context, name: String) {
    assert!(has_database(&context.http_context, &name).await, "Connection doesn't contain database {name}.",);
}

#[apply(generic_step)]
#[step(expr = "connection has database(s):")]
async fn connection_has_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step).map(|name| name.to_owned()) {
        assert!(
            has_database(&context.http_context, &name).await,
            "Connection doesn't contain at least one of the databases.",
        );
    }
}

#[apply(generic_step)]
#[step(expr = "connection does not have database: {word}")]
async fn connection_does_not_have_database(context: &mut Context, name: String) {
    assert!(!has_database(&context.http_context, &name).await, "Connection contains database {name}.",);
}

#[apply(generic_step)]
#[step(expr = "connection does not have database(s):")]
async fn connection_does_not_have_databases(context: &mut Context, step: &Step) {
    for name in iter_table(step).map(|name| name.to_owned()) {
        assert!(
            !has_database(&context.http_context, &name).await,
            "Connection doesn't contain at least one of the databases.",
        );
    }
}

async fn create_temporary_database_with_schema(context: &mut Context, schema_query: String) -> String {
    let name = format!("temp-{}", random_uuid());

    create_database(context.http_client(), context.auth_token(), name.clone(), params::MayError::False).await;
    query(context.http_client(), context.auth_token(), &name, "schema", &schema_query, &None, &None, true)
        .await
        .expect("Expected successful query with commit");

    name
}

#[apply(generic_step)]
#[step(expr = r"connection get database\({word}\) has schema:")]
async fn connection_get_database_has_schema(context: &mut Context, name: String, step: &Step) {
    let expected_schema = step.docstring.as_ref().unwrap().trim().to_string();
    let expected_schema_retrieved = if expected_schema.is_empty() {
        String::new()
    } else {
        let temp_database_name = create_temporary_database_with_schema(context, expected_schema).await;
        databases_schema(context.http_client(), context.auth_token(), &temp_database_name)
            .await
            .expect("Expected successful schema retrieval")
            .trim()
            .to_string()
    };
    let schema = databases_schema(context.http_client(), context.auth_token(), &name)
        .await
        .expect("Expected successful schema retrieval")
        .trim()
        .to_string();
    assert_eq!(expected_schema_retrieved, schema);
}

#[apply(generic_step)]
#[step(expr = r"connection get database\({word}\) has type schema:")]
async fn connection_get_database_has_type_schema(context: &mut Context, name: String, step: &Step) {
    let expected_type_schema = step.docstring.as_ref().unwrap().trim().to_string();
    let expected_type_schema_retrieved = if expected_type_schema.is_empty() {
        String::new()
    } else {
        let temp_database_name = create_temporary_database_with_schema(context, expected_type_schema).await;
        databases_type_schema(context.http_client(), context.auth_token(), &temp_database_name)
            .await
            .expect("Expected successful type schema retrieval")
            .trim()
            .to_string()
    };
    let type_schema = databases_type_schema(context.http_client(), context.auth_token(), &name)
        .await
        .expect("Expected successful type schema retrieval")
        .trim()
        .to_string();
    assert_eq!(expected_type_schema_retrieved, type_schema);
}
