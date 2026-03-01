/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Authentication bridge for the Postgres wire protocol.
//!
//! Defines [`PgAuthenticator`], a trait that verifies username/password
//! credentials. Concrete implementations bridge to the TypeDB credential
//! store (e.g. via `ServerState::user_verify_password()`).
//!
//! Also provides [`TrustAuthenticator`] (always succeeds — for development)
//! and helpers for extracting usernames from startup parameters and
//! decoding password messages.

use std::sync::Arc;

// ── Authenticator trait ────────────────────────────────────────────

/// Verifies credentials during the Postgres wire protocol handshake.
///
/// The projection crate cannot depend on the `server` crate directly,
/// so this trait is the bridge. The server wiring layer implements it
/// by delegating to `ServerState::user_verify_password()`.
pub trait PgAuthenticator: Send + Sync + std::fmt::Debug {
    /// Verify a username/password pair.
    ///
    /// Returns `Ok(())` on success, `Err(message)` on authentication failure.
    fn verify_password(&self, username: &str, password: &str) -> Result<(), String>;
}

// ── Auth mode ──────────────────────────────────────────────────────

/// Determines how a pgwire connection authenticates clients.
#[derive(Debug, Clone)]
pub enum AuthMode {
    /// No authentication — immediately send AuthenticationOk.
    Trust,
    /// Cleartext password authentication — request a password from the client,
    /// then verify it via the supplied authenticator.
    CleartextPassword(Arc<dyn PgAuthenticator>),
}

// ── Built-in authenticators ────────────────────────────────────────

/// An authenticator that accepts every credential. For development/testing only.
#[derive(Debug, Clone)]
pub struct TrustAuthenticator;

impl PgAuthenticator for TrustAuthenticator {
    fn verify_password(&self, _username: &str, _password: &str) -> Result<(), String> {
        Ok(())
    }
}

/// An authenticator that always rejects. Useful as a test double.
#[derive(Debug, Clone)]
pub struct RejectAuthenticator;

impl PgAuthenticator for RejectAuthenticator {
    fn verify_password(&self, _username: &str, _password: &str) -> Result<(), String> {
        Err("authentication failed".to_string())
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Extract the `"user"` parameter from startup message key-value pairs.
///
/// Returns `None` if no `"user"` key is present.
pub fn extract_username(params: &[(String, String)]) -> Option<&str> {
    params.iter().find(|(k, _)| k == "user").map(|(_, v)| v.as_str())
}

/// Extract the `"database"` parameter from startup message key-value pairs.
///
/// Returns `None` if no `"database"` key is present.
pub fn extract_database(params: &[(String, String)]) -> Option<&str> {
    params.iter().find(|(k, _)| k == "database").map(|(_, v)| v.as_str())
}

/// Decode a PasswordMessage payload into a password string.
///
/// The payload is a null-terminated C string (the password).
pub fn decode_password_message(payload: &[u8]) -> Result<String, String> {
    let end = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| "missing null terminator in password message".to_string())?;
    String::from_utf8(payload[..end].to_vec()).map_err(|e| format!("invalid UTF-8 in password message: {e}"))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── TrustAuthenticator ───────────────────────────────────────

    #[test]
    fn trust_authenticator_accepts_any_credentials() {
        let auth = TrustAuthenticator;
        assert!(auth.verify_password("admin", "secret").is_ok());
    }

    #[test]
    fn trust_authenticator_accepts_empty_credentials() {
        let auth = TrustAuthenticator;
        assert!(auth.verify_password("", "").is_ok());
    }

    // ── RejectAuthenticator ──────────────────────────────────────

    #[test]
    fn reject_authenticator_rejects_all() {
        let auth = RejectAuthenticator;
        assert!(auth.verify_password("admin", "secret").is_err());
    }

    #[test]
    fn reject_authenticator_error_message() {
        let auth = RejectAuthenticator;
        let err = auth.verify_password("admin", "secret").unwrap_err();
        assert_eq!(err, "authentication failed");
    }

    // ── AuthMode ─────────────────────────────────────────────────

    #[test]
    fn auth_mode_trust_variant() {
        let mode = AuthMode::Trust;
        assert!(matches!(mode, AuthMode::Trust));
    }

    #[test]
    fn auth_mode_cleartext_variant() {
        let auth: Arc<dyn PgAuthenticator> = Arc::new(TrustAuthenticator);
        let mode = AuthMode::CleartextPassword(auth);
        assert!(matches!(mode, AuthMode::CleartextPassword(_)));
    }

    // ── extract_username ─────────────────────────────────────────

    #[test]
    fn extract_username_found() {
        let params = vec![("user".to_string(), "admin".to_string()), ("database".to_string(), "mydb".to_string())];
        assert_eq!(extract_username(&params), Some("admin"));
    }

    #[test]
    fn extract_username_missing() {
        let params = vec![("database".to_string(), "mydb".to_string())];
        assert_eq!(extract_username(&params), None);
    }

    #[test]
    fn extract_username_empty_params() {
        let params: Vec<(String, String)> = vec![];
        assert_eq!(extract_username(&params), None);
    }

    #[test]
    fn extract_username_first_match() {
        let params = vec![("user".to_string(), "first".to_string()), ("user".to_string(), "second".to_string())];
        assert_eq!(extract_username(&params), Some("first"));
    }

    // ── extract_database ─────────────────────────────────────────

    #[test]
    fn extract_database_found() {
        let params = vec![("user".to_string(), "admin".to_string()), ("database".to_string(), "mydb".to_string())];
        assert_eq!(extract_database(&params), Some("mydb"));
    }

    #[test]
    fn extract_database_missing() {
        let params = vec![("user".to_string(), "admin".to_string())];
        assert_eq!(extract_database(&params), None);
    }

    // ── decode_password_message ──────────────────────────────────

    #[test]
    fn decode_password_simple() {
        let payload = b"mypassword\0";
        assert_eq!(decode_password_message(payload).unwrap(), "mypassword");
    }

    #[test]
    fn decode_password_empty() {
        let payload = b"\0";
        assert_eq!(decode_password_message(payload).unwrap(), "");
    }

    #[test]
    fn decode_password_with_trailing_data() {
        // Extra bytes after null are ignored (per Postgres spec).
        let payload = b"pass\0extra";
        assert_eq!(decode_password_message(payload).unwrap(), "pass");
    }

    #[test]
    fn decode_password_missing_null_terminator() {
        let payload = b"noterm";
        assert!(decode_password_message(payload).is_err());
    }

    #[test]
    fn decode_password_unicode() {
        let mut payload = "pässwörd".as_bytes().to_vec();
        payload.push(0);
        assert_eq!(decode_password_message(&payload).unwrap(), "pässwörd");
    }

    // ── Mock authenticator ───────────────────────────────────────

    #[derive(Debug)]
    struct FixedCredentialAuth {
        expected_user: String,
        expected_pass: String,
    }

    impl PgAuthenticator for FixedCredentialAuth {
        fn verify_password(&self, username: &str, password: &str) -> Result<(), String> {
            if username == self.expected_user && password == self.expected_pass {
                Ok(())
            } else {
                Err(format!("invalid credentials for user '{username}'"))
            }
        }
    }

    #[test]
    fn custom_authenticator_correct_credentials() {
        let auth = FixedCredentialAuth { expected_user: "admin".to_string(), expected_pass: "secret123".to_string() };
        assert!(auth.verify_password("admin", "secret123").is_ok());
    }

    #[test]
    fn custom_authenticator_wrong_password() {
        let auth = FixedCredentialAuth { expected_user: "admin".to_string(), expected_pass: "secret123".to_string() };
        let err = auth.verify_password("admin", "wrong").unwrap_err();
        assert!(err.contains("invalid credentials"));
    }

    #[test]
    fn custom_authenticator_wrong_username() {
        let auth = FixedCredentialAuth { expected_user: "admin".to_string(), expected_pass: "secret123".to_string() };
        assert!(auth.verify_password("other", "secret123").is_err());
    }

    #[test]
    fn authenticator_is_object_safe() {
        // Verify PgAuthenticator can be used as a trait object.
        let auth: Arc<dyn PgAuthenticator> = Arc::new(TrustAuthenticator);
        assert!(auth.verify_password("any", "any").is_ok());
    }

    // ── Integration: full cleartext flow simulation ──────────────

    #[test]
    fn cleartext_password_flow_success() {
        // Simulate the server-side flow:
        // 1. Extract username from startup params
        // 2. Decode password from client message
        // 3. Verify credentials
        let startup_params =
            vec![("user".to_string(), "admin".to_string()), ("database".to_string(), "typedb".to_string())];
        let password_payload = b"secret123\0";

        let auth = FixedCredentialAuth { expected_user: "admin".to_string(), expected_pass: "secret123".to_string() };

        let username = extract_username(&startup_params).unwrap();
        let password = decode_password_message(password_payload).unwrap();
        assert!(auth.verify_password(username, &password).is_ok());
    }

    #[test]
    fn cleartext_password_flow_failure() {
        let startup_params = vec![("user".to_string(), "admin".to_string())];
        let password_payload = b"wrong\0";

        let auth = FixedCredentialAuth { expected_user: "admin".to_string(), expected_pass: "secret123".to_string() };

        let username = extract_username(&startup_params).unwrap();
        let password = decode_password_message(password_payload).unwrap();
        assert!(auth.verify_password(username, &password).is_err());
    }

    #[test]
    fn cleartext_flow_missing_username() {
        let startup_params = vec![("database".to_string(), "mydb".to_string())];
        assert!(extract_username(&startup_params).is_none());
    }
}
