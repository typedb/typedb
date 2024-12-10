/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::BoxFuture;
use resource::constants::server::{AUTHENTICATOR_PASSWORD_FIELD, AUTHENTICATOR_USERNAME_FIELD};
use system::concepts::Credential;
use tonic::{body::BoxBody, metadata::MetadataMap, Status};
use tower::{Layer, Service};
use user::user_manager::UserManager;

const ERROR_INVALID_CREDENTIAL: &str = "Invalid credential supplied";

#[derive(Clone, Debug)]
pub struct Authenticator {
    user_manager: Arc<UserManager>,
    cache: HashMap<String, String>
}

impl Authenticator {
    pub(crate) fn new(user_manager: Arc<UserManager>) -> Self {
        Self { user_manager, cache: HashMap::new() }
    }
}

impl Authenticator {
    pub fn authenticate(&self, http: http::Request<BoxBody>) -> Result<http::Request<BoxBody>, Status> {
        let (parts, body) = http.into_parts();

        let metadata = MetadataMap::from_headers(parts.headers.clone());
        let username_metadata = metadata.get(AUTHENTICATOR_USERNAME_FIELD).and_then(|u| u.to_str().ok());
        let password_metadata = metadata.get(AUTHENTICATOR_PASSWORD_FIELD).and_then(|u| u.to_str().ok());

        let username = username_metadata.ok_or(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))?;
        let password = password_metadata.ok_or(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))?;

        match self.get_from_cache(username) {
            Some(p) => {
                if p == password {
                    return Ok(http::Request::from_parts(parts, body));
                } else {
                    self.remove_from_cache(username);
                }
            }
            None => {}
        }

        let Ok(Some((_, Credential::PasswordType { password_hash }))) = self.user_manager.get(username) else {
            return Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL));
        };

        if password_hash.matches(password) {
            self.insert_to_cache(username, password);
            Ok(http::Request::from_parts(parts, body))
        } else {
            Err(Status::unauthenticated(ERROR_INVALID_CREDENTIAL))
        }
    }

    fn insert_to_cache(&mut self, username: &str, password: &str) {
        self.cache.insert(username.to_string(), password.to_string());
    }

    fn get_from_cache(&self, username: &str) -> Option<&String> {
        self.cache.get(username)
    }

    fn remove_from_cache(&mut self, username: &str) {
        self.cache.remove(username);
    }
}

impl<S: Clone> Layer<S> for Authenticator {
    type Service = AuthenticatedService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthenticatedService::new(service, self.clone())
    }
}

#[derive(Clone)]
pub struct AuthenticatedService<S> {
    inner: S,
    authenticator: Authenticator,
}

impl<S> AuthenticatedService<S> {
    pub fn new(inner: S, authenticator: Authenticator) -> Self {
        Self { inner, authenticator }
    }
}

impl<S> Service<http::Request<BoxBody>> for AuthenticatedService<S>
where
    S: Service<http::Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<Status>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let req = tokio::task::spawn_blocking(move || authenticator.authenticate(req)).await.unwrap()?;
            inner.call(req).await
        })
    }
}
