use tonic::Request;
use typedb_protocol::server_manager::all::Req;
use system::concepts::{Credential, PasswordHash, User};
use user::errors::{UserCreateError, UserUpdateError};

pub fn users_create_req(request: Request<typedb_protocol::user_manager::create::Req>) -> Result<(User, Credential), UserCreateError> {
    let message = request.into_inner();
    match message.user {
        Some(
            typedb_protocol::User {
                name: username,
                credential: Some(
                    typedb_protocol::Credential {
                        credential: Some(
                            typedb_protocol::credential::Credential::Password(
                                typedb_protocol::credential::Password { value: Some(password) }
                            )
                        )
                    }
                )
            }
        ) => {
            let user = User::new(username);
            let credential = Credential::new_password_type(
                PasswordHash::from_password(password.as_str())
            );
            Ok((user, credential))
        }
        _ => {
            Err(UserCreateError::Unexpected { }) // user object must be supplied
        }
    }
}

pub fn users_update_req(request: Request<typedb_protocol::user::update::Req>) -> Result<(String, User, Credential), UserUpdateError> {
    todo!()
}
