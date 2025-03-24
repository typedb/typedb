/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use system::concepts::Credential;
use user::user_manager::UserManager;

use crate::authentication::AuthenticationError;

#[derive(Clone, Debug)]
pub(crate) struct CredentialVerifier {
    user_manager: Arc<UserManager>,
}

impl CredentialVerifier {
    pub(crate) fn new(user_manager: Arc<UserManager>) -> Self {
        Self { user_manager }
    }

    // NOTE: Password verification is an expensive CPU-bound operation!
    pub(crate) fn verify_password(&self, username: &str, password: &str) -> Result<(), AuthenticationError> {
        let Ok(Some((_, Credential::PasswordType { password_hash }))) = self.user_manager.get(username) else {
            return Err(AuthenticationError::InvalidCredential {});
        };

        password_hash.matches(password).then(|| ()).ok_or(AuthenticationError::InvalidCredential {})
    }
}
