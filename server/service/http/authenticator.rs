/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{convert, sync::Arc};

use axum::{body::Body, response::IntoResponse};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use futures::future::BoxFuture;
use http::{Request, Response};
use tower::{Layer, Service};

use crate::{
    authentication::{authenticate, credential_verifier::CredentialVerifier, token_manager::TokenManager},
    service::http::error::HttpServiceError,
};

#[derive(Clone, Debug)]
pub struct Authenticator {
    credential_verifier: Arc<CredentialVerifier>,
    token_manager: Arc<TokenManager>,
    diagnostics_manager: Arc<DiagnosticsManager>,
}

impl Authenticator {
    pub(crate) fn new(
        credential_verifier: Arc<CredentialVerifier>,
        token_manager: Arc<TokenManager>,
        diagnostics_manager: Arc<DiagnosticsManager>,
    ) -> Self {
        Self { credential_verifier, token_manager, diagnostics_manager }
    }
}

impl Authenticator {
    pub async fn authenticate(&self, request: Request<Body>) -> Result<Request<Body>, impl IntoResponse> {
        authenticate(self.token_manager.clone(), request)
            .await
            .map_err(|typedb_source| HttpServiceError::Authentication { typedb_source })
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

impl<S> Service<Request<Body>> for AuthenticatedService<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = convert::Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            match authenticator.authenticate(request).await {
                Ok(req) => inner.call(req).await,
                Err(err) => Ok(err.into_response()),
            }
        })
    }
}
