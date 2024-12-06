/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use resource::constants::server::DEFAULT_USER_NAME;

pub struct PermissionManager {}

impl PermissionManager {
    pub fn exec_user_get_permitted(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }

    pub fn exec_user_all_permitted(accessor: &str) -> bool {
        accessor == DEFAULT_USER_NAME
    }

    pub fn exec_user_create_permitted(accessor: &str) -> bool {
        accessor == DEFAULT_USER_NAME
    }

    pub fn exec_user_update_permitted(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }

    pub fn exec_user_delete_allowed(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }
}
