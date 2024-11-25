use std::sync::Arc;
use tonic::{Request, Status};
use system::concepts::Credential;
use user::user_manager::UserManager;

#[derive(Debug)]
pub struct Authenticator {
    user_manager: Arc<UserManager>
}

impl Authenticator {
    pub(crate) fn new(user_manager: Arc<UserManager>) -> Self {
        Self { user_manager }
    }
}

impl Authenticator {
    pub fn authenticate(&self, req: Request<()>) -> Result<Request<()>, Status> {
        let metadata = req.metadata();
        let username_metadata = metadata.get("username")
            .map(|u| u.to_str());
        let password_metadata = metadata.get("password")
            .map(|u| u.to_str());
        match (username_metadata, password_metadata) {
            (Some(Ok(username)), Some(Ok(password))) => {
                match self.user_manager.get(username) {
                    Some((_, Credential::PasswordType { password_hash })) => {
                        if password_hash.is_hash_equal(password) {
                            Ok(req)
                        } else {
                            Err(Status::unauthenticated("invalid credential supplied"))
                        }
                    }
                    None => Err(Status::unauthenticated("invalid credential supplied"))
                }
            }
            _ => {
                Ok(req)
                // Err(Status::unauthenticated("credential must be supplied"))
            }
        }
    }
}