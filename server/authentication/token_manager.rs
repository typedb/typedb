/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use concurrency::TokioIntervalRunner;
use error::typedb_error;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::{self, Rng};
use resource::constants::server::{MAX_AUTHENTICATION_TOKEN_TTL, MIN_AUTHENTICATION_TOKEN_TTL};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::RwLock;

use crate::authentication::AuthenticationError;

#[derive(Clone, Debug)]
pub struct TokenManager {
    token_owners: Arc<RwLock<HashMap<String, String>>>,
    tokens_expiration_time: Duration,
    secret_key: String,
    _tokens_cleanup_job: Arc<TokioIntervalRunner>,
}

impl TokenManager {
    const TOKENS_CLEANUP_INTERVAL_MULTIPLIER: u32 = 2;

    pub fn new(tokens_expiration_time: Duration) -> Result<Self, TokenManagerError> {
        Self::validate_tokens_expiration_time(tokens_expiration_time)?;

        let token_owners = Arc::new(RwLock::new(HashMap::new()));
        let token_owners_clone = token_owners.clone();

        // We do not specifically aim to use JWT, as we perform additional manual validation
        // and use local caches (meaning that every server restart invalidates previously generated tokens).
        // Therefore, it is acceptable to generate random secret keys without exposing or configuring them.
        // This approach can be changed in the future if needed.
        let secret_key = Self::random_key();
        let secret_key_clone = secret_key.clone();

        let tokens_cleanup_interval = tokens_expiration_time * Self::TOKENS_CLEANUP_INTERVAL_MULTIPLIER;
        let tokens_cleanup_job = Arc::new(TokioIntervalRunner::new(
            move || {
                let token_owners = token_owners_clone.clone();
                let secret_key = secret_key_clone.clone();
                async move {
                    Self::cleanup_expired_tokens(secret_key.as_ref(), token_owners).await;
                }
            },
            tokens_cleanup_interval,
            false,
        ));
        Ok(Self { token_owners, tokens_expiration_time, secret_key, _tokens_cleanup_job: tokens_cleanup_job })
    }

    pub async fn new_token(&self, username: String) -> String {
        // Lock earlier to make sure that `issued_at` and the token are unique
        let mut write_guard = self.token_owners.write().await;

        let issued_at = SystemTime::now();
        let expires_at = issued_at + self.tokens_expiration_time;
        let claims = Claims {
            sub: username.clone(),
            exp: Self::system_time_to_seconds(expires_at),
            iat: Self::system_time_to_seconds(issued_at),
        };

        let token = Self::encode_token(self.secret_key.as_ref(), claims);
        write_guard.insert(token.clone(), username);
        token
    }

    pub async fn get_valid_token_owner(&self, token: &str) -> Option<String> {
        if let Some(claims) = Self::decode_token(self.secret_key.as_ref(), token) {
            if !Self::is_expired(claims.exp) {
                return self.token_owners.read().await.get(token).cloned();
            }
        }
        None
    }

    pub async fn invalidate_user(&self, username: &str) {
        let mut write_guard = self.token_owners.write().await;
        write_guard.retain(|_, token_username| token_username != username);
    }

    async fn cleanup_expired_tokens(secret_key: &[u8], token_owners: Arc<RwLock<HashMap<String, String>>>) {
        let mut write_guard = token_owners.write().await;
        write_guard.retain(|token, _| {
            let Some(claims) = Self::decode_token(secret_key, token) else { return false };
            !Self::is_expired(claims.exp)
        });
    }

    fn encode_token(secret_key: &[u8], claims: Claims) -> String {
        // Default algorithm is HS512
        encode(&Header::default(), &claims, &EncodingKey::from_secret(secret_key))
            .expect("Expected authentication token encoding")
    }

    fn decode_token(secret_key: &[u8], token: &str) -> Option<Claims> {
        // We pass all invalid and expired tokens here. If it's somehow incorrect and returns an
        // error, we don't care - just say that decoding leads to no valid claims.
        decode(token, &DecodingKey::from_secret(secret_key), &Validation::default()).map(|res| res.claims).ok()
    }

    fn system_time_to_seconds(time: SystemTime) -> u64 {
        time.duration_since(UNIX_EPOCH).expect("Expected duration since Unix epoch").as_secs()
    }

    fn is_expired(token_exp: u64) -> bool {
        token_exp <= Self::system_time_to_seconds(SystemTime::now())
    }

    fn random_key() -> String {
        rand::thread_rng().sample_iter(&rand::distributions::Alphanumeric).take(128).map(char::from).collect()
    }

    fn validate_tokens_expiration_time(tokens_expiration_time: Duration) -> Result<(), TokenManagerError> {
        if tokens_expiration_time < MIN_AUTHENTICATION_TOKEN_TTL
            || tokens_expiration_time > MAX_AUTHENTICATION_TOKEN_TTL
        {
            Err(TokenManagerError::InvlaidTokensExpirationTime {
                value: tokens_expiration_time.as_secs(),
                min: MIN_AUTHENTICATION_TOKEN_TTL.as_secs(),
                max: MAX_AUTHENTICATION_TOKEN_TTL.as_secs(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct Claims {
    sub: String,
    exp: u64,
    iat: u64,
}

typedb_error! {
    pub TokenManagerError(component = "Token manager", prefix = "TKM") {
        InvlaidTokensExpirationTime(1, "Invalid tokens expiration time '{value}'. It must be between '{min}' and '{max}' seconds.", value: u64, min: u64, max: u64),
    }
}
