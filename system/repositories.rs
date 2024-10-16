use crate::concepts::User;

pub struct UserRepository {}

impl UserRepository {
    fn list() -> User {
        todo!()
    }

    fn get(username: &str) -> Option<User> {
        todo!()
    }

    fn create(user: User) -> Result<User, i8> {
        todo!()
    }

    fn update(username: &str, update: Option<User>) {
        todo!()
    }

    fn delete(username: &str) -> Result<(), i8> {
        todo!()
    }
}
