use resource::constants::server::DEFAULT_USER_NAME;

pub struct PermissionManager {}

impl PermissionManager {
    pub fn exec_user_get_permitted(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }

    pub fn exec_user_all_permitted(accessor: &str) -> bool {
        accessor == DEFAULT_USER_NAME
    }

    pub fn exec_user_create_permitted(accessor: &str) -> bool {
        accessor == DEFAULT_USER_NAME
    }

    pub fn exec_user_update_permitted(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }

    pub fn exec_user_delete_allowed(accessor: &str, subject: &str) -> bool {
        accessor == DEFAULT_USER_NAME || accessor == subject
    }
}
