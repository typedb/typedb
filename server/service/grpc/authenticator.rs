/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::Arc;

use diagnostics::{diagnostics_manager::DiagnosticsManager, metrics::ActionKind};
use futures::future::BoxFuture;
use http::Request;
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};

use crate::{
    authentication::{authenticate, credential_verifier::CredentialVerifier, token_manager::TokenManager},
    service::grpc::{
        diagnostics::run_with_diagnostics_async,
        error::{IntoGrpcStatus, IntoProtocolErrorMessage},
    },
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
    pub async fn authenticate(&self, request: Request<BoxBody>) -> Result<Request<BoxBody>, Status> {
        run_with_diagnostics_async(self.diagnostics_manager.clone(), None::<&str>, ActionKind::Authenticate, || async {
            authenticate(self.token_manager.clone(), request)
                .await
                .map_err(|typedb_source| typedb_source.into_error_message().into_status())
        })
        .await
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
    const AUTHENTICATION_FREE_METHODS: &'static [&'static str] = &["connection_open", "authentication_token_create"];

    pub fn new(inner: S, authenticator: Authenticator) -> Self {
        Self { inner, authenticator }
    }

    fn is_authentication_required(request: &Request<BoxBody>) -> bool {
        if let Some(method) = request.uri().path().split('/').last() {
            !Self::AUTHENTICATION_FREE_METHODS.contains(&method)
        } else {
            true
        }
    }
}

impl<S> Service<Request<BoxBody>> for AuthenticatedService<S>
where
    S: Service<Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<Status>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let request = match Self::is_authentication_required(&request) {
                true => authenticator.authenticate(request).await?,
                false => request,
            };
            inner.call(request).await
        })
    }
}
