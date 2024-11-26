use error::typedb_error;

typedb_error!(
    pub UserCreateError(component = "User create", prefix = "USC") {
        Unexpected(1, "An unexpected error has occurred in the process of creating a new user"),
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
        Unexpected(2, "An unexpected error has occurred in the process of deleting a user"),
    }
);
