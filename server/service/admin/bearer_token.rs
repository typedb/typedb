/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::Arc,
};

use rand::RngCore;
use resource::constants::server::{ADMIN_TOKEN_BYTES, ADMIN_TOKEN_FILE_MODE};
use tonic::{Request, Status, metadata::MetadataValue, service::Interceptor};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

/// Generate a fresh random token, write it to `path` with mode 0600, and return its value.
///
/// The trust anchor for the admin endpoint is filesystem access to this file: the typedb
/// admin tool reads it, the server's interceptor compares against it. Re-rolling on every
/// startup means tokens leaked at any previous run become invalid the next time the
/// server boots.
///
/// Implementation notes:
/// - The token is `ADMIN_TOKEN_BYTES` random bytes hex-encoded — 64 ASCII chars for the
///   default 32-byte size, plenty for a localhost shared secret.
/// - The file is opened with `O_CREAT | O_TRUNC` and the unix `mode` bit set so that, on
///   *creation*, the file inherits the requested mode. If the file already exists from a
///   previous boot, the OS uses the existing permissions, so we follow up with an
///   explicit `set_permissions` to guarantee 0600 regardless.
/// - The parent directory is the server's data directory, which is created with restrictive
///   permissions earlier in startup, so there is no race window where a peer process could
///   read the token between the write and the chmod.
pub fn generate_and_write_token(path: &Path) -> Result<String, std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut bytes = vec![0u8; ADMIN_TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = encode_hex(&bytes);

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(ADMIN_TOKEN_FILE_MODE)
        .open(path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    fs::set_permissions(path, fs::Permissions::from_mode(ADMIN_TOKEN_FILE_MODE))?;

    Ok(token)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(hex_nibble(b >> 4));
        s.push(hex_nibble(b & 0x0f));
    }
    s
}

fn hex_nibble(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!(),
    }
}

/// Constant-time equality on byte slices of equal length.
///
/// Used to compare the request's bearer token against the server's secret. A plain `==`
/// would short-circuit on the first mismatch and could leak the matching prefix length
/// through timing — for a localhost admin channel this is a very weak attack vector, but
/// the implementation is trivial and removes the concern entirely.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// tonic interceptor that enforces a bearer-token `authorization` header on every admin
/// RPC.
///
/// This is the load-bearing fence for admin authority: it requires the caller to present
/// the same token written by [`generate_and_write_token`], which only processes with
/// read access to the token file can possess.
#[derive(Clone)]
pub struct BearerTokenInterceptor {
    expected: Arc<String>,
}

impl BearerTokenInterceptor {
    pub fn new(expected: Arc<String>) -> Self {
        Self { expected }
    }
}

impl Interceptor for BearerTokenInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let header: &MetadataValue<_> = req
            .metadata()
            .get(AUTHORIZATION_HEADER)
            .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?;
        let header_str =
            header.to_str().map_err(|_| Status::unauthenticated("Authorization header is not valid ASCII"))?;
        let presented = header_str
            .strip_prefix(BEARER_PREFIX)
            .ok_or_else(|| Status::unauthenticated("Authorization header must use the Bearer scheme"))?;
        if constant_time_eq(presented.as_bytes(), self.expected.as_bytes()) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("Invalid admin token"))
        }
    }
}

