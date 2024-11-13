use tonic::Request;
use system::concepts::{Credential, Password, User};
use user::errors::UserCreateError;

pub fn users_create_req(request: Request<typedb_protocol::user_manager::create::Req>) -> Result<(User, Credential), UserCreateError> {
    let message = request.into_inner();
    match message.user {
        Some(user) => {
            match user.credential {
                Some(cred) => {
                    match cred.credential {
                        Some(cred2) => {
                            todo!()
                        }
                        None => {
                            todo!()
                        }
                    }
                }
                None => {
                    // todo!("credential object must be supplied")
                    Ok((User::new("test user".to_string()), Credential::new_password_type(Password::new(Vec::new(), Vec::new()))))
                }
            }
        }
        None => {
            todo!("user object must be supplied")
        }
    }
}