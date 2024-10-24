pub struct User {
    pub name: String
}

impl User {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub enum Credential {
    PasswordType { password: Password }
}

impl Credential {
    pub fn new_password_type(password: Password) -> Credential {
        Credential::PasswordType { password }
    }
}

pub struct Password {
    hash: Vec<u8>,
    salt: Vec<u8>
}

impl Password {
    pub fn new(hash: Vec<u8>, salt: Vec<u8>) -> Self {
        todo!()
    }
}