/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, slice};

use cucumber::{gherkin::Step, given, then, when};
use futures::TryFutureExt;
use itertools::Itertools;
use macro_rules_attribute::apply;
use params;
use tokio::time::sleep;

use crate::{
    assert_err, generic_step,
    message::{users, users_create, users_delete, users_get, users_update},
    util::iter_table,
    Context, HttpContext,
};

async fn get_all_usernames(context: &HttpContext) -> impl IntoIterator<Item = String> {
    users(context).await.expect("Expected users").users.into_iter().map(|user| user.username)
}

#[apply(generic_step)]
#[step(expr = "get all users:")]
async fn get_all_users(context: &mut Context, step: &Step) {
    let expected_users: HashSet<String> = iter_table(step).map(|name| name.to_owned()).collect();
    let users = get_all_usernames(&context.http_context).await.into_iter().collect::<HashSet<_>>();
    assert!(users == expected_users, "Expected users: {expected_users:?}, got: {users:?}");
}

#[apply(generic_step)]
#[step(expr = "get all users{may_error}")]
async fn get_all_users_error(context: &mut Context, may_error: params::MayError) {
    may_error.check(users(&context.http_context).await);
}

#[apply(generic_step)]
#[step(expr = "get all users {contains_or_doesnt}: {word}")]
async fn get_all_users_contains(context: &mut Context, contains_or_doesnt: params::ContainsOrDoesnt, username: String) {
    let usernames = get_all_usernames(&context.http_context).await.into_iter().collect_vec();
    contains_or_doesnt.check(slice::from_ref(&username), &usernames);
}

#[apply(generic_step)]
#[step(expr = "get user: {word}{may_error}")]
async fn get_user(context: &mut Context, username: String, may_error: params::MayError) {
    may_error.check(users_get(&context.http_context, &username).await);
}

#[apply(generic_step)]
#[step(expr = r"get user\({word}\) get name: {word}")]
async fn get_user_get_name(context: &mut Context, user: String, name: String) {
    let user = users_get(&context.http_context, &user).await.unwrap();
    assert_eq!(user.username, name);
}

#[apply(generic_step)]
#[step(expr = "create user with username '{word}', password '{word}'{may_error}")]
async fn create_user(context: &mut Context, username: String, password: String, may_error: params::MayError) {
    may_error.check(users_create(&context.http_context, &username, &password).await);
}

#[apply(generic_step)]
#[step(expr = r"get user\({word}\) update password to '{word}'{may_error}")]
async fn get_user_update_password(
    context: &mut Context,
    username: String,
    password: String,
    may_error: params::MayError,
) {
    may_error.check(users_update(&context.http_context, &username, &password).await);
}

#[apply(generic_step)]
#[step(expr = "delete user: {word}{may_error}")]
async fn delete_user(context: &mut Context, username: String, may_error: params::MayError) {
    may_error.check(users_delete(&context.http_context, &username).await);
}

#[apply(generic_step)]
#[step(expr = "get current username: {word}")]
async fn get_current_username(_context: &mut Context, _username: String) {
    // no op: do not test it here, you just have a token!
}
