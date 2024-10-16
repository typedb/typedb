use system::concepts::User;
use system::repositories::UserRepository;

struct UserManager {
    user_repository: UserRepository
}

impl UserManager {

    fn all() -> Vec<User> {
        todo!()
    }

    fn contains(name: &str) -> bool {
        todo!()
    }

    fn get(name: &str) -> Option<User> {
        todo!()
    }

    fn create(name: &str) -> Result<User, i8> {
        todo!()
    }

    fn create_admin_if_not_exists() {
        todo!()
    }
}