/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use system::concepts::{Credential, PasswordHash, User};

use crate::user_manager::UserManager;
use resource::constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD};
pub mod errors;
pub mod user_manager;

pub fn initialise_default_user(user_manager: &UserManager) {
    if !user_manager.contains(DEFAULT_USER_NAME) {
        user_manager
            .create(
                &User::new(DEFAULT_USER_NAME.to_string()),
                &Credential::PasswordType { password_hash: PasswordHash::from_password(DEFAULT_USER_PASSWORD) },
            )
            .unwrap();
    }
}
