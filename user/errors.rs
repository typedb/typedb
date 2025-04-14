/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;

typedb_error! {
    pub UserGetError(component = "User get", prefix = "USG") {
        IllegalUsername(1, "Invalid credential supplied."),
        Unexpected(2, "An unexpected error has occurred in the process of getting a user."),
    }
}

typedb_error! {
    pub UserCreateError(component = "User create", prefix = "USC") {
        IllegalUsername(1, "Invalid credential supplied."),
        UserAlreadyExist(2, "User already exists."),
        IncompleteUserDetail(3, "Incomplete user detail."),
        Unexpected(4, "An unexpected error has occurred in the process of creating a new user."),
    }
}

typedb_error! {
    pub UserUpdateError(component = "User update", prefix = "USU") {
        UserDetailNotProvided(1, "User detail not provided."),
        IllegalUsername(2, "Invalid credential supplied,"),
        Unexpected(3, "An unexpected error has occurred in the process of updating a user."),
    }
}

typedb_error! {
    pub UserDeleteError(component = "User delete", prefix = "USD") {
        DefaultUserCannotBeDeleted(1, "Default user cannot be deleted."),
        IllegalUsername(2, "Invalid credential supplied."),
        UserDoesNotExist(3, "User does not exist."),
        Unexpected(4, "An unexpected error has occurred in the process of deleting a user."),
    }
}
