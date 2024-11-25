use error::typedb_error;

typedb_error!(
    pub UserCreateError(component = "User create", prefix = "USC") {
        Unexpected(1, "TODO: unexpected error"),
    }
);

typedb_error!(
    pub UserUpdateError(component = "User update", prefix = "USC") {
        Unexpected(1, "TODO: unexpected error"),
    }
);

typedb_error!(
    pub UserDeleteError(component = "User delete", prefix = "USC") {
        Unexpected(1, "TODO: unexpected error"),
    }
);