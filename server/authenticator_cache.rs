/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use moka::sync::Cache;
use resource::constants::server::{AUTHENTICATOR_CACHE_TTI, AUTHENTICATOR_CACHE_TTL};

#[derive(Clone, Debug)]
pub struct AuthenticatorCache {
    cache: Cache<String, String>,
}

impl AuthenticatorCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder().time_to_live(AUTHENTICATOR_CACHE_TTL).time_to_idle(AUTHENTICATOR_CACHE_TTI).build(),
        }
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
