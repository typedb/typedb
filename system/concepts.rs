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
    pub fn new_password_type(password_hash: PasswordHash) -> Credential {
        Credential::PasswordType { password_hash }
    }
}

#[derive(Debug)]
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
