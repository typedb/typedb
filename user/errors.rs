/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;

typedb_error!(
    pub UserGetError(component = "User create", prefix = "USC") {
        QueryExecutionError(1, "User get query could not be executed"),
    }
);

typedb_error!(
    pub UserCreateError(component = "User create", prefix = "USC") {
        QueryExecutionError(1, "User create query could not be executed"),
        Unexpected(2, "An unexpected error has occurred in the process of creating a new user"),
    }
);

typedb_error!(
    pub UserUpdateError(component = "User update", prefix = "USU") {
        Unexpected(1, "An unexpected error has occurred in the process of updating a user"),
    }
);

typedb_error!(
    pub UserDeleteError(component = "User delete", prefix = "USD") {
        DefaultUserCannotBeDeleted(1, "Default user cannot be deleted"),
        QueryExecutionError(2, "User delete query could not be executed"),
        Unexpected(3, "An unexpected error has occurred in the process of deleting a user"),
    }
);
