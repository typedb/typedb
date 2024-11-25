use std::sync::Arc;

use system::concepts::{Credential, PasswordHash, User};

use crate::user_manager::UserManager;

pub mod errors;
pub mod user_manager;

pub fn initialise_default_user(user_manager: &Arc<UserManager>) {
    if !user_manager.contains("admin") {
        user_manager
            .create(
                &User::new("admin".to_string()),
                &Credential::PasswordType { password_hash: PasswordHash::from_password("password") },
            )
            .unwrap();
    }
}
