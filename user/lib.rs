/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD};
use system::concepts::{Credential, PasswordHash, User};

use crate::user_manager::UserManager;

pub mod errors;
pub mod permission_manager;
pub mod user_manager;
