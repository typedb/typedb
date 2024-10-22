use system::concepts::User;
use system::repositories::UserRepository;

#[derive(Debug)]
pub struct UserManager {
    user_repository: UserRepository
}

impl UserManager {
    pub fn new() -> Self {
        todo!()
    }

    pub fn all(&self) -> Vec<User> {
        todo!()
    }

    pub fn contains(&self, name: &str) -> bool {
        todo!()
    }

    pub fn get(&self, name: &str) -> Option<User> {
        todo!()
    }

    pub fn create(&self, name: &str) -> Result<User, i8> {
        todo!()
    }
}