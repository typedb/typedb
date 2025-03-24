/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use pwhash::bcrypt;

#[derive(Debug)]
pub struct User {
    pub name: String,
}

impl User {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[derive(Debug)]
pub enum Credential {
    PasswordType { password_hash: PasswordHash },
}

impl Credential {
    pub fn new_password(password: &str) -> Self {
        Self::PasswordType { password_hash: PasswordHash::from_password(password) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PasswordHash {
    pub value: String,
}

impl PasswordHash {
    pub fn new(hash: String) -> Self {
        Self { value: hash }
    }

    pub fn from_password(password: &str) -> Self {
        Self { value: bcrypt::hash(password).unwrap() }
    }

    pub fn matches(&self, password: &str) -> bool {
        bcrypt::verify(password, self.value.as_str())
    }
}
