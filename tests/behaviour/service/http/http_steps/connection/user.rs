/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, slice};

use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;

use crate::{
    generic_step,
    message::{users, users_create, users_delete, users_get, users_update},
    params::TokenMode,
    util::{iter_table, random_uuid},
    Context, HttpContext,
};

async fn get_all_usernames(context: &HttpContext) -> impl IntoIterator<Item = String> {
    users(context.http_client(), context.auth_token())
        .await
        .expect("Expected users")
        .users
        .into_iter()
        .map(|user| user.username)
}

#[apply(generic_step)]
#[step(expr = "get all users:")]
async fn get_all_users(context: &mut Context, step: &Step) {
    let expected_users: HashSet<String> = iter_table(step).map(|name| name.to_owned()).collect();
    let users = get_all_usernames(&context.http_context).await.into_iter().collect::<HashSet<_>>();
    assert!(users == expected_users, "Expected users: {expected_users:?}, got: {users:?}");
}

#[apply(generic_step)]
#[step(expr = "{token_mode}get all users{may_error}")]
async fn with_a_wrong_token_get_all_users_error(
    context: &mut Context,
    token_mode: TokenMode,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error.check(users(context.http_client(), context.auth_token_by_mode(token_mode)).await);
}

#[apply(generic_step)]
#[step(expr = "get all users {contains_or_doesnt}: {word}")]
async fn get_all_users_contains(context: &mut Context, contains_or_doesnt: params::ContainsOrDoesnt, username: String) {
    let usernames = get_all_usernames(&context.http_context).await.into_iter().collect_vec();
    contains_or_doesnt.check(slice::from_ref(&username), &usernames);
}

#[apply(generic_step)]
#[step(expr = "{token_mode}get user: {word}{may_error}")]
async fn with_a_wrong_token_get_user(
    context: &mut Context,
    token_mode: TokenMode,
    username: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error.check(users_get(context.http_client(), context.auth_token_by_mode(token_mode), &username).await);
}

#[apply(generic_step)]
#[step(expr = r"get user\({word}\) get name: {word}")]
async fn get_user_get_name(context: &mut Context, user: String, name: String) {
    let user = users_get(context.http_client(), context.auth_token(), &user).await.unwrap();
    assert_eq!(user.username, name);
}

#[apply(generic_step)]
#[step(expr = "{token_mode}create user with username '{word}', password '{word}'{may_error}")]
async fn with_a_wrong_token_create_user(
    context: &mut Context,
    token_mode: TokenMode,
    username: String,
    password: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error
        .check(users_create(context.http_client(), context.auth_token_by_mode(token_mode), &username, &password).await);
}

#[apply(generic_step)]
#[step(expr = r"{token_mode}get user\({word}\) update password to '{word}'{may_error}")]
async fn with_a_wrong_token_get_user_update_password(
    context: &mut Context,
    token_mode: TokenMode,
    username: String,
    password: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error
        .check(users_update(context.http_client(), context.auth_token_by_mode(token_mode), &username, &password).await);
}

#[apply(generic_step)]
#[step(expr = "{token_mode}delete user: {word}{may_error}")]
async fn with_a_wrong_token_delete_user(
    context: &mut Context,
    token_mode: TokenMode,
    username: String,
    may_error: params::MayError,
) {
    context.randomize_auth_token_if_needed(token_mode);
    may_error.check(users_delete(context.http_client(), context.auth_token_by_mode(token_mode), &username).await);
}

#[apply(generic_step)]
#[step(expr = "get current username: {word}")]
async fn get_current_username(_context: &mut Context, _username: String) {
    // no op: do not test it here, you just have a token!
}
