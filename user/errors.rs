use error::typedb_error;

typedb_error!(
    pub UserCreateError(component = "User create", prefix = "USC") {
        Unexpected(1, "TODO: unexpected error"),
    }
);