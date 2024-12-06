/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use resource::constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD};
use system::concepts::{Credential, PasswordHash, User};

use crate::user_manager::UserManager;
pub mod errors;
pub mod permission_manager;
pub mod user_manager;

pub fn initialise_default_user(user_manager: &UserManager) {
    if !user_manager
        .contains(DEFAULT_USER_NAME)
        .expect("An unexpected error occurred when checking for the existence of default user")
    {
        user_manager
            .create(
                &User::new(DEFAULT_USER_NAME.to_string()),
                &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
            )
            .unwrap();
    }
}
