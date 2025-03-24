/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use system::concepts::{Credential, User};
use tonic::Request;
use user::errors::{UserCreateError, UserUpdateError};

pub fn users_create_req(
    request: Request<typedb_protocol::user_manager::create::Req>,
) -> Result<(User, Credential), UserCreateError> {
    let message = request.into_inner();
    match message.user {
        Some(typedb_protocol::User { name: username, password: Some(password) }) => {
            let user = User::new(username);
            let credential = Credential::new_password(password.as_str());
            Ok((user, credential))
        }
        _ => Err(UserCreateError::IncompleteUserDetail {}),
    }
}

pub fn users_update_req(
    request: Request<typedb_protocol::user::update::Req>,
) -> Result<(String, Option<User>, Option<Credential>), UserUpdateError> {
    let message = request.into_inner();
    match message.user {
        Some(typedb_protocol::User { name: username, password }) => {
            Ok((message.name, Some(User::new(username)), password.map(|p| Credential::new_password(p.as_str()))))
        }
        None => Err(UserUpdateError::UserDetailNotProvided {}),
    }
}
