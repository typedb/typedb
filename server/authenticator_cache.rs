use std::sync::Arc;
use system::concepts::Credential;
use moka::sync::Cache;

#[derive(Clone, Debug)]
pub struct AuthenticatorCache {
    cache: Cache<String, String>
}

impl AuthenticatorCache {
    pub fn new() -> Self {
        Self { cache: Cache::new(100) }
    }

    pub fn get_user(&self, username: &str) -> Option<String> {
        self.cache.get(username)
    }

    pub fn cache_user(&self, username: &str, password: &str) {
        self.cache.insert(username.to_string(), password.to_string());
    }

    pub fn invalidate_user(&self, username: &str) {
        self.cache.remove(username);
    }
}

