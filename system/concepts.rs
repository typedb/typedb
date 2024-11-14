use pwhash::bcrypt;

#[derive(Debug)]
pub struct User {
    pub name: String
}

impl User {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[derive(Debug)]
pub enum Credential {
    PasswordType { password_hash: PasswordHash }
}

impl Credential {
    pub fn new_password_type(password_hash: PasswordHash) -> Credential {
        Credential::PasswordType { password_hash }
    }
}

#[derive(Debug)]
pub struct PasswordHash {
    pub hash: String,
    pub salt: String
}

impl PasswordHash {
    pub fn new(hash: String, salt: String) -> Self {
        Self { hash, salt }
    }

    pub fn from_password(password: &str) -> Self {
        Self { hash: bcrypt::hash(password).unwrap(), salt: "".to_string() }
    }

    pub fn is_hash_equal(&self, password: &str) -> bool {
        bcrypt::verify(password, self.hash.as_str())
    }
}