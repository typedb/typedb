/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use resource::constants::server::{AUTHENTICATOR_PASSWORD_FIELD, AUTHENTICATOR_USERNAME_FIELD};
use system::concepts::Credential;
use tonic::{Request, Status};
use user::user_manager::UserManager;

const ERROR_INVALID_CREDENTIAL: &str = "Invalid credential supplied";

#[derive(Debug)]
pub struct Authenticator {
    user_manager: Arc<UserManager>,
}

impl Authenticator {
    pub(crate) fn new(user_manager: Arc<UserManager>) -> Self {
        Self { user_manager }
    }
}

impl Authenticator {
    pub fn authenticate(&self, req: Request<()>) -> Result<Request<()>, Status> {
        let metadata = req.metadata();
        let username_metadata = metadata.get(AUTHENTICATOR_USERNAME_FIELD).map(|u| u.to_str());
        let password_metadata = metadata.get(AUTHENTICATOR_PASSWORD_FIELD).map(|u| u.to_str());
        match (username_metadata, password_metadata) {
            (Some(Ok(username)), Some(Ok(password))) => match self.user_manager.get(username) {
                Ok(get_result) => match get_result {
                    Some((_, Credential::PasswordType { password_hash })) => {
                        if password_hash.matches(password) {
                            Ok(req)
                        } else {
                            Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))
                        }
                    }
                    None => Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL)),
                },
                Err(_) => Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL)),
            },
            _ => Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL)),
        }
    }
}
