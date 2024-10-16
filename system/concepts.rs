pub struct User {
    name: String,
    credential: Credential
}

pub enum Credential {
    Password { hash: Vec<u8>, salt: Vec<u8> }
}
