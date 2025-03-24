/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use system::concepts::User;

use crate::service::http::message::from_request_parts_impl;

#[derive(Debug)]
pub(crate) struct UserPath {
    pub(crate) username: String,
}

from_request_parts_impl!(UserPath { username: String });

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateUserPayload {
    pub password: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateUserPayload {
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsersResponse {
    pub users: Vec<UserResponse>,
}

pub(crate) fn encode_users(users: Vec<User>) -> UsersResponse {
    UsersResponse { users: users.into_iter().map(|user| encode_user(&user)).collect_vec() }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserResponse {
    pub username: String,
}

pub(crate) fn encode_user(user: &User) -> UserResponse {
    UserResponse { username: user.name.clone() }
}
